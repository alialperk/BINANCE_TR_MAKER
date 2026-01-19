import logging
import numpy as np
import pandas as pd
import asyncio
import aiohttp
import ujson
import json
import time
import arbit_config_maker_BTR as arbit_config
import ccxt
from redis import asyncio as aioredis
import redis
import requests
import pickle
import sys
from datetime import datetime
from enum import Enum
import APIkeys
import base64
import hashlib
import hmac
from typing import Dict, List, Optional
from collections import deque
import math
import uvloop
from dataclasses import dataclass
import random
from decimal import Decimal
import CS_host_health_distribution as CS_health
import uuid
import os

# Optional import for CPU affinity (handled gracefully if not available)
try:
    import psutil  # type: ignore[import-untyped]
except ImportError:
    psutil = None  # Will be checked in set_cpu_affinity()

# Configuration flags (set before imports)
USE_CYTHON_OPTIMIZATION = os.getenv("ENABLE_CYTHON_OPT", "1") == "1"
MEASURE_PERFORMANCE = False
USE_CPP_OPTIMIZATION = True  # Always True - Use C++ WebSocket with shared memory (requires C++ client running)

# Read collocation from Redis early to determine USE_CPP_OPTIMIZATION_BINANCE
try:
    initial_redis_check = redis.Redis(host='localhost', port=6379, db=0)
    collocation_check = initial_redis_check.get('collocation')
    if collocation_check is None:
        collocation_check = 0
    else:
        collocation_check = int(collocation_check)
    initial_redis_check.close()
except Exception as e:
    logging.warning(f"Could not read collocation from Redis for USE_CPP_OPTIMIZATION_BINANCE check: {e}, defaulting to 0")
    collocation_check = 0  # Default to 0 if Redis read fails

# If collocation == 1, disable Binance C++ optimization; otherwise allow it based on import success
if collocation_check == 1:
    USE_CPP_OPTIMIZATION_BINANCE = True  # Disabled when collocation == 1
else:
    USE_CPP_OPTIMIZATION_BINANCE = False  # Will be set to True if import succeeds


# Try to import shared memory reader for C++ WebSocket optimization
if USE_CPP_OPTIMIZATION:
    try:
        from python_binance_tr_shared_memory_reader import BinTROrderbookReader, run_bintr_shared_memory_reader as run_shared_memory_reader
        logging.info("C++ WebSocket optimization enabled - using shared memory for BinTR orderbook updates")
    except ImportError as e:
        USE_CPP_OPTIMIZATION = False
        logging.warning(f"C++ WebSocket optimization requested but shared memory reader not available: {e}")
        logging.warning("Falling back to regular WebSocket connection")
    
    # Try to import Binance shared memory reader
    try:
        from python_binance_cs_shared_memory_reader import BinanceSharedMemoryReader, run_binance_shared_memory_reader, get_binance_usdttry_rate
        USE_CPP_OPTIMIZATION_BINANCE = True
        logging.info("C++ WebSocket optimization enabled - using shared memory for Binance/CS orderbook updates")
    except ImportError as e:
        USE_CPP_OPTIMIZATION_BINANCE = False
        logging.warning(f"C++ WebSocket optimization for Binance requested but shared memory reader not available: {e}")
        logging.warning("Falling back to regular Binance WebSocket connection")
        get_binance_usdttry_rate = None  # Will be None if import failed

# Import CS Trading Adapter configuration from arbit_config
from arbit_config_maker_BTR import (
    CS_TRADING_ADAPTER_HOST, CS_TRADING_ADAPTER_PORT, CS_TRADING_ADAPTER_WS_ENDPOINT,
    CLIENT_PROCESS_ID, CLIENT_APP_VER, CLIENT_APP_NAME, CLIENT_ORG, ACCOUNT_ID,
    CS_ORDER_PLACE_RESPONSE_TIMEOUT, CS_ORDER_FILL_TIMEOUT, 
    CS_HEARTBEAT_SEC, CS_PING_TIMEOUT_SEC
)

# Enable uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Script configuration for distributed processing
SCRIPT_ID = 1  # Script 1 of 10

# CPU affinity configuration (16-Core System)
# IMPORTANT: Cores 0, 1, and 2 are reserved for C++ WebSocket clients:
#   - Core 0: BinTR C++ WebSocket client
#   - Core 1: Binance C++ WebSocket client 1
#   - Core 2: Binance C++ WebSocket client 2
# Python arbitrage scripts use cores 3-12 (10 scripts, 1 per core, isolated)
# Cores 11-15 are available for GUI, Redis, monitoring, and other system processes
CPU_CORES = {
    1: 14,   # Script 1 -> CPU Core 3 (Cores 0-2 reserved for C++ clients)
    2: 15,   # Script 2 -> CPU Core 4
    3: 16,   # Script 3 -> CPU Core 5
    4: 17,   # Script 4 -> CPU Core 6
    5: 18,   # Script 5 -> CPU Core 7
    6: 19,   # Script 6 -> CPU Core 8
    7: 20,   # Script 7 -> CPU Core 9
    8: 21,  # Script 8 -> CPU Core 10
    9: 22,  # Script 9 -> CPU Core 11
    10: 23, # Script 10 -> CPU Core 12
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

np.seterr(invalid='ignore')

# Display options for pandas
PANDAS_DISPLAY_OPTIONS = {
    'display.max_columns': None,
    'display.max_rows': None,
    'display.width': 1000,
    'display.max_colwidth': 50
}
# Set pandas display options
for option, value in PANDAS_DISPLAY_OPTIONS.items():
    pd.set_option(option, value)

try:
    initial_redis = redis.Redis(host='localhost', port=6379, db=0)
except Exception as e:
    logging.error(f"Error connecting to Redis: {e}")
    initial_redis = None
    exit()

try:
    TEST_MODE = initial_redis.get('TEST_MODE')
    if TEST_MODE is None:
        TEST_MODE = 1
        initial_redis.set('TEST_MODE', TEST_MODE)
        logging.info(f"TEST_MODE set to 1 by default")
    else:
        TEST_MODE = int(TEST_MODE)

    BINANCE_ORDERBOOK_MODE = initial_redis.get('BINANCE_orderbook_mode')

    if BINANCE_ORDERBOOK_MODE is None:
        BINANCE_ORDERBOOK_MODE = "depth"
        initial_redis.set('BINANCE_orderbook_mode', BINANCE_ORDERBOOK_MODE)
        logging.info(f"BINANCE_orderbook_mode set to depth by default")
    else:
        BINANCE_ORDERBOOK_MODE = BINANCE_ORDERBOOK_MODE.decode('utf-8')

    # Get collocation parameter from Redis (default: 0)
    collocation = initial_redis.get('collocation')
    if collocation is None:
        collocation = 0
        initial_redis.set('collocation', collocation)
        logging.info(f"collocation set to 0 by default")
    else:
        collocation = int(collocation)

    # Get AUTO_CS_SWITCH parameter from Redis (default: 1)
    # 0 = Connect to all hosts and subscribe all symbols to each host (no switching)
    # 1 = Use host switching mechanism based on health/latency
    AUTO_CS_SWITCH = initial_redis.get('AUTO_CS_SWITCH')
    if AUTO_CS_SWITCH is None:
        AUTO_CS_SWITCH = 1
        initial_redis.set('AUTO_CS_SWITCH', AUTO_CS_SWITCH)
        logging.info(f"AUTO_CS_SWITCH set to 1 by default")
    else:
        AUTO_CS_SWITCH = int(AUTO_CS_SWITCH)

except Exception as e:
    logging.error(f"Error getting parameters from Redis: {e}")
    exit()

logging.info(f"TEST_MODE: {TEST_MODE}")
logging.info(f"BINANCE_ORDERBOOK_MODE: {BINANCE_ORDERBOOK_MODE}")
logging.info(f"collocation: {collocation}")
logging.info(f"AUTO_CS_SWITCH: {AUTO_CS_SWITCH}")

ACCOUNT_TYPE = "MAKER_TEST" if TEST_MODE else "MAKER"
MANUAL_ACCOUNT_TYPE = "MANUAL_TEST" if TEST_MODE else "MANUAL"
logging.info(f"ACCOUNT_TYPE: {ACCOUNT_TYPE}")
logging.info(f"MANUAL_ACCOUNT_TYPE: {MANUAL_ACCOUNT_TYPE}")

if TEST_MODE:
    BINANCE_API_KEY = APIkeys.BINANCE_api_key_ALPER
    BINANCE_SECRET_KEY = APIkeys.BINANCE_secret_key_ALPER
else:
    BINANCE_API_KEY = APIkeys.BINANCE_api_key_KERIM
    BINANCE_SECRET_KEY = APIkeys.BINANCE_secret_key_KERIM

# Create async Exchange objects & connections
BINANCE = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'keepAlive': True,
    'options': {'defaultType': 'future', 'adjustForTimeDifference': True, },
})

BinTR = ccxt.BinTR({
    'keepAlive': True,
    'options': {'defaultType': 'spot', 'adjustForTimeDifference': True, },
})

binance_symbol_list = arbit_config.binance_symbol_list
BinTR_symbol_list = arbit_config.BinTR_symbol_list
combined_symbol_list = arbit_config.combined_symbol_list

# Configure websocket data streaming parameters
websocket_reconnection_time = 3  # in seconds

monitor_interval = 5
currency_update_interval = 120 # in seconds
update_redis_arbitrage_table_interval = 1

USDTTRY_bid = 1
USDTTRY_ask = 1

# Validate SCRIPT_ID
if SCRIPT_ID not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
    logging.error(f"Invalid SCRIPT_ID: {SCRIPT_ID}. Must be 1-10")
    exit()



# Calculate symbol ranges for this script using the symbol groups from config
total_symbols = len(combined_symbol_list)
script_symbols = arbit_config.binance_symbol_groups[SCRIPT_ID - 1]  # Use the actual symbol group for this script

# Convert Binance symbols back to base symbols
script_symbols = [symbol.replace('USDT', '') for symbol in script_symbols]
# Handle exceptional symbols
script_symbols = [symbol.replace('LUNA2', 'LUNA').replace('BEAMX', 'BEAM') for symbol in script_symbols]
script_symbols = [symbol.replace('1000PEPE', 'PEPE').replace('1000BONK', 'BONK').replace('1000SHIB', 'SHIB').replace('1000FLOKI', 'FLOKI') for symbol in script_symbols]

# Calculate start and end indices for Redis updates
start_idx = 0
for i in range(SCRIPT_ID - 1):
    start_idx += len(arbit_config.binance_symbol_groups[i])
end_idx = start_idx + len(script_symbols)

# Log script configuration
logging.info(f"=== Script {SCRIPT_ID} Configuration ===")
logging.info(f"Total symbols in system: {total_symbols}")
logging.info(f"Symbols handled by Script {SCRIPT_ID}: {len(script_symbols)}")
logging.info(f"Symbol range: {start_idx} to {end_idx-1}")
logging.info(f"Script symbols: {script_symbols}")
logging.info(f"CPU Core assigned: {CPU_CORES.get(SCRIPT_ID, 'Unknown')}")
logging.info("======================================")

# Create a NumPy array with NaNs for only the symbols this script handles
arbitrage_table_np = np.full((len(script_symbols), len(arbit_config.columns)), np.nan, dtype=object)

# Create dictionaries for each exchange to map base symbols to formatted symbols
BinTR_dict = {symbol[:-3]: symbol for symbol in BinTR_symbol_list}  # Remove 'TRY'
binance_dict = {symbol[:-4]: symbol for symbol in binance_symbol_list}  # Remove 'USDT'

# Create global symbol-to-index mapping for shared memory (consistent across all scripts)
# This maps base symbols to their global index in combined_symbol_list
global_symbol_to_index = {symbol: idx for idx, symbol in enumerate(combined_symbol_list)}

# Set the critical parameters for trading!
#arbitrage_table_np[:, arbit_config_maker.col_Maker_Type] = 0

# Initialize order_lock to 0 by default there should be no open orders
arbitrage_table_np[:, arbit_config.col_Buy_Order_Lock] = 0
arbitrage_table_np[:, arbit_config.col_Sell_Order_Lock] = 0
arbitrage_table_np[:, arbit_config.col_Buy_ActionType] = 0
arbitrage_table_np[:, arbit_config.col_Sell_ActionType] = 0

# Initialize order_id to 0 by default there should be no open orders
arbitrage_table_np[:, arbit_config.col_Buy_Order_ID] = 0
arbitrage_table_np[:, arbit_config.col_Sell_Order_ID] = 0

arbitrage_table_np[:, arbit_config.col_BinTR_orderable_bid_price] = 0
arbitrage_table_np[:, arbit_config.col_BinTR_orderable_ask_price] = 0

arbitrage_table_np[:, arbit_config.col_BinTR_price_step] = 0
arbitrage_table_np[:, arbit_config.col_BinTR_price_precision] = 0
arbitrage_table_np[:, arbit_config.col_BinTR_AmountPrecision] = 0


# Global Variables
open_positions_dict = {}  # Dictionary to track open positions

BINANCE_ws_connected = False
BinTR_ws_connected = False

# Buy orders disabled flag - set to True when BalanceNotEnough error is received for Buy orders
buy_orders_disabled = False

# Sell orders disabled per symbol - set of symbols that have sell orders disabled due to BalanceNotEnough
sell_orders_disabled_symbols = set()

# Initialize task variables
binance_task = None
BinTR_task = None
arbitrage_task = None
update_currency_task = None
#benchmark_task = None
redis_task = None
command_listener_task = None
update_balances_task = None
BinTR_private_ws_listen_task = None
cs_ws_tasks = []  # List of CS websocket tasks (one per host)

# Multiple BinTR websocket tasks for parallelism
BinTR_task = None
BinTR_ws_connected = False

BinTR_order_ws = None

# Create custom websocket URIs for this script's symbols
script_binance_symbols = [f"{symbol}USDT" for symbol in script_symbols]
# Handle exceptional symbols for Binance
script_binance_symbols_formatted = []
for symbol in script_binance_symbols:
    if symbol == "LUNAUSDT":
        script_binance_symbols_formatted.append("LUNA2USDT")
    elif symbol == "BEAMUSDT":
        script_binance_symbols_formatted.append("BEAMXUSDT")
    elif symbol in ["PEPEUSDT", "BONKUSDT", "SHIBUSDT", "FLOKIUSDT"]:
        script_binance_symbols_formatted.append(f"1000{symbol}")
    else:
        script_binance_symbols_formatted.append(symbol)

max_BinanceTimeDiff = 150

# Align symbols based on the script's symbol list
for i, base_symbol in enumerate(script_symbols):
    # Insert the corresponding formatted symbols into the appropriate columns
    if base_symbol in binance_dict:
        arbitrage_table_np[i, arbit_config.col_Binance_Symbol] = binance_dict[base_symbol]
    elif base_symbol == "BEAM":
        arbitrage_table_np[i, arbit_config.col_Binance_Symbol] = "BEAMXUSDT"
    elif base_symbol == "LUNA":
        arbitrage_table_np[i, arbit_config.col_Binance_Symbol] = "LUNA2USDT"
    elif base_symbol in ["PEPE", "BONK", "SHIB", "FLOKI"]:
        arbitrage_table_np[i, arbit_config.col_Binance_Symbol] = f"1000{base_symbol}USDT"

    if base_symbol in BinTR_dict:
        arbitrage_table_np[i, arbit_config.col_BinTR_Symbol] = BinTR_dict[base_symbol]

arbitrage_table_np[:, arbit_config.col_Base_Symbol] = script_symbols

# Convert to a pandas DataFrame for easier inspection
arbitrage_table_pd = pd.DataFrame(arbitrage_table_np)
logging.info(f"Length of arbitrage_table: {len(arbitrage_table_pd)}")

symbol_index_map = {symbol: idx for idx, symbol in enumerate(script_symbols)}
logging.info(f"Length of symbol_index_map: {len(symbol_index_map)}")
logging.info(f"Symbol index map keys: {list(symbol_index_map.keys())[:10]}...")


# Cache column indices for faster access
_col_binance_time = arbit_config.col_Binance_Time
_col_binance_time_diff = arbit_config.col_BinanceTimeDiff
_col_binance_bid_p1 = arbit_config.col_Binance_BidP1
_col_binance_bid_a1 = arbit_config.col_Binance_BidA1
_col_binance_ask_p1 = arbit_config.col_Binance_AskP1
_col_binance_ask_a1 = arbit_config.col_Binance_AskA1

# Pre-compute timestamp thresholds for faster comparison
_TS_NANOSECONDS = 1000000000000000000
_TS_MICROSECONDS = 1000000000000

arbitrage_benchmark_cnt = 0

class ArbitrageState(Enum):
    STOPPED = "stopped"
    RUNNING = "running"

current_state = ArbitrageState.STOPPED

# Global Redis connection pool
redis_pool = None

cumulative_duration = 0

tradeID = 0

new_incoming_data = False

binance_packet_cnt = 0
BinTR_packet_cnt = 0


BinTR_private_ws_bot = None


USDTTRY = 1.0

async def get_redis_connection():
    global redis_pool
    if redis_pool is None:
        redis_pool = await aioredis.from_url( 
            f"redis://localhost:6379"
        )
    return redis_pool



BINANCE_average_ws_delay = 0


_calculate_times = []

# Cache for column indices to avoid repeated lookups
_cached_cols = None

def _get_cached_cols():
    """Cache column indices for performance"""
    global _cached_cols
    if _cached_cols is None:
        _cached_cols = {
            'binance_ask': arbit_config.col_Binance_AskP1,
            'binance_bid': arbit_config.col_Binance_BidP1,
            'BinTR_ask': arbit_config.col_BinTR_AskP1,
            'BinTR_bid': arbit_config.col_BinTR_BidP1,
            'buy_order_id': arbit_config.col_Buy_Order_ID,
            'sell_order_id': arbit_config.col_Sell_Order_ID,
            'buy_order_lock': arbit_config.col_Buy_Order_Lock,
            'sell_order_lock': arbit_config.col_Sell_Order_Lock,
            'buy_order_price': arbit_config.col_Buy_Order_Price,
            'sell_order_price': arbit_config.col_Sell_Order_Price,
            'open_margin': arbit_config.col_OpenMargin,
            'close_margin': arbit_config.col_CloseMargin,
            'open_stop_margin': arbit_config.col_OpenStopMargin,
            'open_trigger_margin': arbit_config.col_OpenTriggerMargin,
            'close_trigger_margin': arbit_config.col_CloseTriggerMargin,
            'close_stop_margin': arbit_config.col_CloseStopMargin,
            'binance_time_diff': arbit_config.col_BinanceTimeDiff,
            'maker_type': arbit_config.col_Maker_Type,
            'BinTR_position_try': arbit_config.col_BinTR_PositionAmount_TRY,
            'BinTR_free_try': arbit_config.col_BinTR_free_TRY,
            'min_buy_order_amount_try': arbit_config.col_MinBuyOrderAmount_TRY,
            'min_sell_order_amount_try': arbit_config.col_MinSellOrderAmount_TRY,
            'BinTR_max_position_amount_TRY': arbit_config.col_MaxPositionAmount_TRY,
            'buy_action_type': arbit_config.col_Buy_ActionType,
            'sell_action_type': arbit_config.col_Sell_ActionType,
            'BinTR_price_precision': arbit_config.col_BinTR_price_precision,
            'BinTR_price_precision_step': arbit_config.col_BinTR_price_step,
            'BinTR_open_aggression': arbit_config.col_OpenAggression,
            'BinTR_close_aggression': arbit_config.col_CloseAggression,
            'BinTR_next_bid_price': arbit_config.col_BinTR_next_bid_price,
            'BinTR_next_ask_price': arbit_config.col_BinTR_next_ask_price,
            'BinTR_aggression_bid_price': arbit_config.col_BinTR_aggression_bid_price,
            'BinTR_aggression_ask_price': arbit_config.col_BinTR_aggression_ask_price,
            'BinTR_orderable_bid_price': arbit_config.col_BinTR_orderable_bid_price,
            'BinTR_orderable_ask_price': arbit_config.col_BinTR_orderable_ask_price,
        }
    return _cached_cols 

def _safe_float_array(arr):
    """Optimized safe conversion to float array - uses np.asarray for best performance"""
    # Fast path: use np.asarray directly (faster than Cython wrapper for this case)
    try:
        # Convert to float64 array directly
        result = np.asarray(arr, dtype=np.float64)
        # Handle NaN values
        return np.where(np.isnan(result), 0.0, result)
    except (ValueError, TypeError):
        # Fallback for object arrays: handle element by element
        result = np.zeros(len(arr), dtype=np.float64)
        for i, val in enumerate(arr):
            try:
                if val is None or (isinstance(val, str) and val.lower() in ['nan', 'none', '']):
                    result[i] = 0.0
                else:
                    result[i] = float(val)
            except (ValueError, TypeError):
                result[i] = 0.0
        return result
    except Exception as e:
        # Final fallback: return zeros array
        logging.warning(f"Error in _safe_float_array: {e}, returning zeros array")
        return np.zeros(len(arr), dtype=np.float64)

_calculate_arbitrage_cycle_count = 0
_calculate_arbitrage_perf_count = 0
_calculate_arbitrage_perf_total_time = 0.0

async def calculate_arbitrage():
    """
    Enhanced calculate_arbitrage function with TIMING DELAY PROTECTION:
    
    CRITICAL SAFETY FEATURES:
    1. CANCEL EXISTING ORDERS when Binance time delay > MAX_BINANCE_TIME_DIFF
    2. PREVENT NEW ORDERS when Binance time delay > MAX_BINANCE_TIME_DIFF
    3. PRIORITY cancellation for high-delay scenarios
    4. Enhanced logging for delay monitoring
    
    This ensures trading safety during network latency issues.
    """
    global _calculate_times, _calculate_arbitrage_perf_count, _calculate_arbitrage_perf_total_time
    
    # Performance measurement
    perf_start = time.perf_counter()
    
    try:
        
        # Original Python implementation (fallback)
        cols = _get_cached_cols()       

        # 1. Pre-fetch commonly used columns with optimized slicing and safe conversion
        binance_ask_p = _safe_float_array(arbitrage_table_np[:, cols['binance_ask']])
        binance_bid_p = _safe_float_array(arbitrage_table_np[:, cols['binance_bid']])
        BinTR_ask_p = _safe_float_array(arbitrage_table_np[:, cols['BinTR_ask']])
        BinTR_bid_p = _safe_float_array(arbitrage_table_np[:, cols['BinTR_bid']])

        BinTR_max_position_amount_TRY = _safe_float_array(arbitrage_table_np[:, cols['BinTR_max_position_amount_TRY']])
        min_buy_order_amount_try = _safe_float_array(arbitrage_table_np[:, cols['min_buy_order_amount_try']])
        min_sell_order_amount_try = _safe_float_array(arbitrage_table_np[:, cols['min_sell_order_amount_try']])

         # Price diff shall be calculated with the next bid and ask prices
        BinTR_price_precision_step = _safe_float_array(arbitrage_table_np[:, cols['BinTR_price_precision_step']])
        BinTR_next_bid_price = BinTR_bid_p + BinTR_price_precision_step
        BinTR_next_ask_price = BinTR_ask_p - BinTR_price_precision_step
        arbitrage_table_np[:, cols['BinTR_next_bid_price']] = BinTR_next_bid_price
        arbitrage_table_np[:, cols['BinTR_next_ask_price']] = BinTR_next_ask_price


        BinTR_open_aggression = _safe_float_array(arbitrage_table_np[:, cols['BinTR_open_aggression']])
        BinTR_close_aggression = _safe_float_array(arbitrage_table_np[:, cols['BinTR_close_aggression']])

        BinTR_aggression_open_price = BinTR_bid_p * BinTR_open_aggression
        BinTR_aggression_close_price = BinTR_ask_p * BinTR_close_aggression
        arbitrage_table_np[:, cols['BinTR_aggression_bid_price']] = BinTR_aggression_open_price
        arbitrage_table_np[:, cols['BinTR_aggression_ask_price']] = BinTR_aggression_close_price

        BinTR_aggression_open_price = np.round(BinTR_aggression_open_price * precision_multipliers) / precision_multipliers
        BinTR_aggression_close_price = np.round(BinTR_aggression_close_price * precision_multipliers) / precision_multipliers
        
        # Now take max/min of rounded prices
        orderable_open_price_rounded = np.maximum(BinTR_aggression_open_price, BinTR_next_bid_price)
        orderable_close_price_rounded = np.minimum(BinTR_aggression_close_price, BinTR_next_ask_price)

        # Apply vectorized rounding
        arbitrage_table_np[:, cols['BinTR_orderable_bid_price']] = orderable_open_price_rounded
        arbitrage_table_np[:, cols['BinTR_orderable_ask_price']] = orderable_close_price_rounded

        price_diff_open = binance_bid_p - orderable_open_price_rounded
        price_diff_close = orderable_close_price_rounded - binance_ask_p

        # Need to check if the orderable prices are within the ask and bid prices
        open_spread_available = orderable_open_price_rounded < BinTR_ask_p    
        close_spread_available = orderable_close_price_rounded > BinTR_bid_p

        # 2. Calculate margins using optimized vectorized operations
        # Pre-calculate price differences to avoid redundant operations
        price_diff_open = binance_bid_p - BinTR_bid_p
        price_diff_close = BinTR_ask_p - binance_ask_p
        
        # Replace NaN values in price differences with 0 before division
        price_diff_open = np.where(np.isnan(price_diff_open), 0.0, price_diff_open)
        price_diff_close = np.where(np.isnan(price_diff_close), 0.0, price_diff_close)
        
        # Use efficient division with zero handling
        # Check for both zero and NaN in denominators
        open_margin_valid = (BinTR_bid_p != 0) & ~np.isnan(BinTR_bid_p) & ~np.isnan(price_diff_open)
        close_margin_valid = (binance_ask_p != 0) & ~np.isnan(binance_ask_p) & ~np.isnan(price_diff_close)
        
        open_margin = np.divide(price_diff_open, BinTR_bid_p, out=np.zeros_like(price_diff_open), where=open_margin_valid)
        close_margin = np.divide(price_diff_close, binance_ask_p, out=np.zeros_like(price_diff_close), where=close_margin_valid)
        
        # Ensure no NaN values in final margins (replace any remaining NaN with 0)
        open_margin = np.where(np.isnan(open_margin), 0.0, open_margin)
        close_margin = np.where(np.isnan(close_margin), 0.0, close_margin)
        
        # Store margins back to table
        arbitrage_table_np[:, cols['open_margin']] = open_margin
        arbitrage_table_np[:, cols['close_margin']] = close_margin

        # Debug logging every 3 seconds for margin calculation verification
        if not hasattr(calculate_arbitrage, '_last_debug_time'):
            calculate_arbitrage._last_debug_time = 0
        current_time = time.time()
        if current_time - calculate_arbitrage._last_debug_time >= 3.0:
            calculate_arbitrage._last_debug_time = current_time
            
            # Get a sample of symbols to log (first 5 symbols from script_symbols)
            sample_symbols = list(symbol_index_map.keys())[:5]
            debug_lines = [f"=== MARGIN DEBUG (Script {SCRIPT_ID}) ==="]
            
            for symbol in sample_symbols:
                idx = symbol_index_map.get(symbol)
                if idx is not None and idx < len(BinTR_ask_p):
                    BinTR_ask = BinTR_ask_p[idx]
                    BinTR_bid = BinTR_bid_p[idx]
                    binance_ask = binance_ask_p[idx]
                    binance_bid = binance_bid_p[idx]
                    open_m = open_margin[idx]
                    close_m = close_margin[idx]
                    
                    # Check if prices are valid (not NaN, not zero)
                    BinTR_valid = not (np.isnan(BinTR_ask) or np.isnan(BinTR_bid) or BinTR_ask == 0 or BinTR_bid == 0)
                    binance_valid = not (np.isnan(binance_ask) or np.isnan(binance_bid) or binance_ask == 0 or binance_bid == 0)
                    
                    debug_lines.append(
                        f"  {symbol} (idx={idx}): "
                        f"BinTR ask={BinTR_ask:.4f} bid={BinTR_bid:.4f} {'✓' if BinTR_valid else '✗'}, "
                        f"Binance ask={binance_ask:.4f} bid={binance_bid:.4f} {'✓' if binance_valid else '✗'}, "
                        f"OpenMargin={open_m*10000:.2f}bps, CloseMargin={close_m*10000:.2f}bps"
                    )
            
            logging.info("\n".join(debug_lines))

        # 3. Create boolean masks using direct comparisons (avoiding intermediate variables)
        # Safe conversion for order IDs and locks
        buy_order_ids = arbitrage_table_np[:, cols['buy_order_id']]
        sell_order_ids = arbitrage_table_np[:, cols['sell_order_id']]
        buy_order_locks = arbitrage_table_np[:, cols['buy_order_lock']]
        sell_order_locks = arbitrage_table_np[:, cols['sell_order_lock']]
        
        # Convert to numeric safely for comparisons
        buy_order_ids = _safe_float_array(buy_order_ids)
        sell_order_ids = _safe_float_array(sell_order_ids)
        
        # CRITICAL: Check for actual orders (> 0), not temporary markers (-1)
        # -1 is used as temporary marker between order send and 601 confirmation
        buy_open_orders_exist = buy_order_ids > 0
        sell_open_orders_exist = sell_order_ids > 0
        no_buy_order_lock = buy_order_locks == 0
        no_sell_order_lock = sell_order_locks == 0
        
        # 4. Optimized price comparisons with single-pass conversion
        buy_order_prices = _safe_float_array(arbitrage_table_np[:, cols['buy_order_price']])
        sell_order_prices = _safe_float_array(arbitrage_table_np[:, cols['sell_order_price']])
        BinTR_best_bid_prices = _safe_float_array(BinTR_bid_p)
        BinTR_best_ask_prices = _safe_float_array(BinTR_ask_p)

        # 5. Optimized margin calculations with single-pass conversion
        open_stop_margin = _safe_float_array(arbitrage_table_np[:, cols['open_stop_margin']])
        open_trigger_margin = _safe_float_array(arbitrage_table_np[:, cols['open_trigger_margin']])
        close_trigger_margin = _safe_float_array(arbitrage_table_np[:, cols['close_trigger_margin']])
        close_stop_margin = _safe_float_array(arbitrage_table_np[:, cols['close_stop_margin']])

        # 6. Optimized conditions using vectorized operations
        open_stop_cond = open_margin < open_stop_margin
        close_stop_cond = close_margin < close_stop_margin

        # 7. Optimized BinanceTimeDiff check with enhanced monitoring
        binance_time_diff = _safe_float_array(arbitrage_table_np[:, cols['binance_time_diff']])
        binance_time_diff_high = binance_time_diff > MAX_BINANCE_TIME_DIFF

        # 8. Optimized maker type comparisons using direct comparison
        maker_type = arbitrage_table_np[:, cols['maker_type']]
        # Convert to numeric safely for comparisons
        maker_type = _safe_float_array(maker_type)
        is_buy_maker = maker_type == 1
        is_sell_maker = maker_type == 3
        is_combo_maker = maker_type == 13

        # 9. Optimized balance checks using vectorized operations
        BinTR_position_try = _safe_float_array(arbitrage_table_np[:, cols['BinTR_position_try']])
        BinTR_free_try = _safe_float_array(arbitrage_table_np[:, cols['BinTR_free_try']])
        min_buy_order_amount_try = _safe_float_array(arbitrage_table_np[:, cols['min_buy_order_amount_try']])
        min_sell_order_amount_try = _safe_float_array(arbitrage_table_np[:, cols['min_sell_order_amount_try']])
        
        enough_BinTR_balance_close = BinTR_position_try > min_sell_order_amount_try
        enough_BinTR_balance_open = BinTR_free_try > min_buy_order_amount_try

        max_position_check = BinTR_position_try < BinTR_max_position_amount_TRY
        
        # 10. Optimized price comparisons using vectorized operations
        not_best_bid = (buy_order_prices > 0) & (buy_order_prices < BinTR_best_bid_prices)
        not_best_ask = (sell_order_prices > 0) & (sell_order_prices > BinTR_best_ask_prices)

        # 11. Optimized condition calculations using vectorized operations
        # CRITICAL: Check for existing orders (> 0) and temporary markers (-1) to prevent duplicates
        # -1 is set when order is sent but 601 hasn't arrived yet - treat as "order exists" to prevent duplicates
        buy_order_ids_safe = _safe_float_array(buy_order_ids)
        sell_order_ids_safe = _safe_float_array(sell_order_ids)
        # Order exists if ID > 0 (real order) OR == -1 (temporary marker - order in progress)
        buy_open_orders_exist = (buy_order_ids_safe > 0) | (buy_order_ids_safe == -1)
        sell_open_orders_exist = (sell_order_ids_safe > 0) | (sell_order_ids_safe == -1)
        no_buy_open_orders = ~buy_open_orders_exist
        no_sell_open_orders = ~sell_open_orders_exist
        
        # Combine maker conditions once
        buy_maker_cond = is_buy_maker | is_combo_maker
        sell_maker_cond = is_sell_maker | is_combo_maker

        # 12. Calculate all conditions in one pass with ENHANCED TIMING DELAY PROTECTION
        
        # CRITICAL: Prevent new orders when Binance time delay is high
        # This ensures we don't place new orders during high latency periods
        open_trigger_cond = (open_margin > open_trigger_margin) & no_buy_open_orders & buy_maker_cond & no_buy_order_lock & enough_BinTR_balance_open & ~binance_time_diff_high
        close_trigger_cond = (close_margin > close_trigger_margin) & no_sell_open_orders & sell_maker_cond & no_sell_order_lock & enough_BinTR_balance_close & ~binance_time_diff_high

        open_trigger_cond = open_trigger_cond & open_spread_available & max_position_check
        close_trigger_cond = close_trigger_cond & close_spread_available
        
        # ENHANCED: Cancel orders when Binance time delay is high (existing logic + enhanced)
        # This ensures we cancel existing orders during high latency periods
        cancel_bid_cond = buy_open_orders_exist & (not_best_bid | open_stop_cond | binance_time_diff_high) & buy_maker_cond
        cancel_ask_cond = sell_open_orders_exist & (not_best_ask | close_stop_cond | binance_time_diff_high) & sell_maker_cond

        # PRIORITY: Cancel orders immediately when timing delay is high (stop-only conditions)
 
        # Apply order lock protection for cancel operations
        cancel_bid_trigger_cond = cancel_bid_cond & no_buy_order_lock
        cancel_ask_trigger_cond = cancel_ask_cond & no_sell_order_lock

        # 13. PRIORITY-BASED action type setting - only ONE action per symbol
        # Priority order: Cancel orders (highest) > Close orders > Open orders
        buy_action_type_col = cols['buy_action_type']
        sell_action_type_col = cols['sell_action_type']
        arbitrage_table_np[:, buy_action_type_col] = 0
        arbitrage_table_np[:, sell_action_type_col] = 0
        

        arbitrage_table_np[open_trigger_cond, buy_action_type_col] = 1
        arbitrage_table_np[cancel_bid_trigger_cond, buy_action_type_col] = 2

        arbitrage_table_np[close_trigger_cond, sell_action_type_col] = 3
        arbitrage_table_np[cancel_ask_trigger_cond, sell_action_type_col] = 4
        

    except Exception as e:
        logging.error(f"Error in calculate_arbitrage: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return False
    return True

async def monitor_performance():
   while True:
        await asyncio.sleep(5)
        if len(_calculate_times) > 0:
            duration = sum(_calculate_times)/len(_calculate_times)
            logging.info(f"Calculate_arbitrage loop: {duration*1000*1000:.0f} microseconds")
            _calculate_times.clear()
        else:
            logging.info("No calculate_arbitrage times")

maker_enable_orders = False
maker_enable_orders_previous_state = False

async def update_BinTR_balances_loop():
    while True:
        await asyncio.sleep(1)
        await update_BinTR_balances()
        
        
        # We need to request all open orders!!!
        # await BinTR_order_ws.request_all_open_orders()

        # Log lock status for monitoring
        await cleanup_stale_locks()


async def update_BinTR_balances():
    try:
        
        BinTR_balance = arbit_config.fetch_BinTR_balance(type=ACCOUNT_TYPE)
         # Process BinTR TRY balance once
        BinTR_free_try = BinTR_balance['TRY']['available']
        arbitrage_table_np[:, arbit_config.col_BinTR_free_TRY] = BinTR_free_try

         # Create a numpy array of balances for this script's symbols only
        BinTR_balances = np.array([BinTR_balance.get(symbol, {}).get('available', 0.0) for symbol in script_symbols])
        BinTR_balances_total = np.array([BinTR_balance.get(symbol, {}).get('total', 0.0) for symbol in script_symbols])

        # Update coin positions in one operation
        arbitrage_table_np[:, arbit_config.col_BinTR_PositionAmount_coin] = BinTR_balances
        arbitrage_table_np[:, arbit_config.col_BinTR_PositionAmount_coin_total] = BinTR_balances_total

        # Update TRY positions using vectorized multiplication
        arbitrage_table_np[:, arbit_config.col_BinTR_PositionAmount_TRY] = (
            BinTR_balances * arbitrage_table_np[:, arbit_config.col_BinTR_BidP1]
        )
        arbitrage_table_np[:, arbit_config.col_BinTR_PositionAmount_TRY_total] = (
            BinTR_balances_total * arbitrage_table_np[:, arbit_config.col_BinTR_BidP1]
        )

        # Update capacity gaps
        arbitrage_table_np[:, arbit_config.col_CapacityGap_TRY] = (
            arbitrage_table_np[:, arbit_config.col_MaxPositionAmount_TRY] - 
            arbitrage_table_np[:, arbit_config.col_BinTR_PositionAmount_TRY_total]
        )

    except Exception as e:
        logging.error(f"Error updating BinTR balances: {e}")
        return False
    return True

async def update_binance_balances(command: str = "Manual command"):

    try:
      
        try:
            # Fetch balances concurrently
            binance_balance = BINANCE.fetch_balance()
                
        except ccxt.RateLimitExceeded as e:
            logging.warning(f"Rate limit exceeded when fetching balances: {e}")
            # Wait before retrying
            await asyncio.sleep(2)
            return None
        except Exception as e:
            logging.error(f"Error fetching balances: {e}")
            return None

        # Process Binance USDT balance once
        binance_free_usdt = float(binance_balance['info']['availableBalance'])
        arbitrage_table_np[:, arbit_config.col_Binance_free_usdt] = binance_free_usdt
        arbitrage_table_np[:, arbit_config.col_Binance_free_TRY] = binance_free_usdt * USDTTRY_bid

        # Pre-process Binance positions into a dictionary for O(1) lookup
        binance_positions = {
            position['symbol']: float(position['positionAmt'])
            for position in binance_balance['info']['positions']
        }

        # Update Binance positions in one operation (only for this script's symbols)
        binance_positions_array = np.array([float(binance_positions.get(f"{symbol}USDT", 0.0)) 
                                          for symbol in script_symbols])
        arbitrage_table_np[:, arbit_config.col_BinancePositionAmount_coin] = binance_positions_array

        # Update Binance TRY positions
        arbitrage_table_np[:, arbit_config.col_BinancePositionAmount_TRY] = (
            binance_positions_array * arbitrage_table_np[:, arbit_config.col_Binance_BidP1]
        )

    except Exception as e:
        logging.error(f"Error checking balances and positions: {str(e)}")
        order_lock = False
        return False
    
# Global Variables
open_positions_dict = {}  # Dictionary to track open positions
open_orders = {}  # Dictionary to track open orders
placed_orders = deque(maxlen=100)  # Fixed-size deque to track placed order IDs (FIFO, max 100 entries)
prev_buy_order_prices = {}  # Dictionary to track previous order prices for each symbol
prev_sell_order_prices = {}  # Dictionary to track previous order prices for each symbol
prev_matched_amounts = {}  # Dictionary to track previous matched amounts for each order ID
external_order_id_to_symbol = {}  # Map externalOrderId to (symbol, idx, order_type) for 601 rejection matching
order_lock_reasons = {}  # Dictionary to track lock reasons: (idx, order_type) -> {"reason": str, "timestamp": float, "symbol": str}

min_BINANCE_order_amount_TRY = 1000

# CS Binance Order WebSocket class (for collocation == 1)
class CSBinanceOrderWS:
    """CS WebSocket client for sending Binance orders via Trading Adapter"""
    def __init__(self):
        self.host = CS_TRADING_ADAPTER_HOST
        self.port = CS_TRADING_ADAPTER_PORT
        self.url = f"ws://{self.host}:{self.port}{CS_TRADING_ADAPTER_WS_ENDPOINT}"
        self.connected = False
        self.authenticated = False
        self.ws = None
        self.session = None
        self.account_id = ACCOUNT_ID
        self.org = CLIENT_ORG
        self.app_name = CLIENT_APP_NAME
        self.app_ver = CLIENT_APP_VER
        self.process_id = CLIENT_PROCESS_ID
        self.pending_orders = {}  # {external_order_id: {order_info, future, timestamp}}
        self.order_responses = {}  # {external_order_id: response_data}
        self.running = True
        
    async def connect(self):
        """Connect to CS Trading Adapter WebSocket"""
        global collocation
        if not collocation:
            return False
            
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        connector = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        try:
            self.ws = await self.session.ws_connect(
                self.url,
                heartbeat=CS_HEARTBEAT_SEC,
                autoping=True,
                max_msg_size=8*1024*1024,
                timeout=CS_PING_TIMEOUT_SEC,
            )
            self.connected = True
            
            # Login/Authenticate
            await self.ws.send_json([13, self.org, self.app_name, self.app_ver, self.process_id])
            await asyncio.sleep(0.2)
            
            self.authenticated = True
            logging.info(f"Script {SCRIPT_ID}: CS Binance Order WS connected and authenticated")
            return True
        except Exception as e:
            logging.error(f"Script {SCRIPT_ID}: Failed to connect CS Binance Order WS: {e}")
            self.connected = False
            self.authenticated = False
            return False
    
    async def listen_for_responses(self):
        """Listen for order responses (placeResponse, fill messages)"""
        while self.running and self.connected:
            try:
                if self.ws.closed:
                    break
                    
                msg = await self.ws.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = ujson.loads(msg.data)
                        
                        # Handle placeResponse (msgType 14)
                        if isinstance(data, list) and len(data) >= 2 and data[0] == 14:
                            await self._handle_place_response(data)
                        
                        # Handle fill message (msgType 15)
                        elif isinstance(data, list) and len(data) >= 2 and data[0] == 15:
                            await self._handle_fill_message(data)
                            
                    except Exception as e:
                        logging.error(f"Script {SCRIPT_ID}: Error processing CS order response: {e}")
                        
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logging.error(f"Script {SCRIPT_ID}: CS Binance Order WS closed/error")
                    self.connected = False
                    break
                    
            except Exception as e:
                logging.error(f"Script {SCRIPT_ID}: Error in CS order response listener: {e}")
                break
    
    async def _handle_place_response(self, data):
        """Handle placeResponse message [14, externalOrderId, status, ...]"""
        try:
            if len(data) < 3:
                return
                
            external_order_id = data[1]
            status = data[2]
            
            # Store response
            self.order_responses[external_order_id] = {
                'status': status,
                'data': data,
                'timestamp': time.time()
            }
            
            # Resolve pending order future if exists
            if external_order_id in self.pending_orders:
                future = self.pending_orders[external_order_id].get('future')
                if future and not future.done():
                    future.set_result({'status': status, 'data': data})
            
            logging.info(f"Script {SCRIPT_ID}: CS placeResponse for order {external_order_id}: status={status}")
            
        except Exception as e:
            logging.error(f"Script {SCRIPT_ID}: Error handling placeResponse: {e}")
    
    async def _handle_fill_message(self, data):
        """Handle fill message [15, externalOrderId, fillData, ...] - supports partial fills"""
        try:
            if len(data) < 3:
                return
                
            external_order_id = data[1]
            fill_data = data[2] if len(data) > 2 else {}
            
            # Handle partial fills - accumulate fill data
            if external_order_id in self.order_responses:
                # Check if we already have fill data (for partial fills)
                if 'fills' not in self.order_responses[external_order_id]:
                    # Initialize fills list if this is the first fill
                    self.order_responses[external_order_id]['fills'] = []
                    # Keep single fill for backward compatibility
                    if 'fill' in self.order_responses[external_order_id]:
                        self.order_responses[external_order_id]['fills'].append(self.order_responses[external_order_id]['fill'])
                
                # Add new fill to the list
                self.order_responses[external_order_id]['fills'].append(fill_data)
                # Also update single fill with latest (for backward compatibility)
                self.order_responses[external_order_id]['fill'] = fill_data
            else:
                # First fill for this order
                self.order_responses[external_order_id] = {
                    'fills': [fill_data],
                    'fill': fill_data,  # For backward compatibility
                    'timestamp': time.time()
                }
            
            logging.info(f"Script {SCRIPT_ID}: CS fill message for order {external_order_id}: {fill_data}")
            
        except Exception as e:
            logging.error(f"Script {SCRIPT_ID}: Error handling fill message: {e}")
    
    async def send_market_order(self, instrument_id, side, quantity, external_order_id=None):
        """
        Send market order via CS
        
        Args:
            instrument_id: CS instrument ID
            side: "BUY" or "SELL"
            quantity: Order quantity
            external_order_id: Optional external order ID (generated if not provided)
        
        Returns:
            Future that resolves with order response
        """
        if not self.connected or not self.authenticated:
            logging.error(f"Script {SCRIPT_ID}: CS Binance Order WS not connected/authenticated")
            return None
        
        if external_order_id is None:
            external_order_id = f"BINANCE_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        
        # Create order message: [msgType, accountId, externalOrderId, instrumentId, side, quantity, orderType]
        # msgType 12 = placeOrder
        order_message = [
            12,  # msgType: placeOrder
            self.account_id,
            external_order_id,
            instrument_id,
            side.upper(),  # "BUY" or "SELL"
            str(quantity),  # Quantity as string
            "MARKET"  # Order type
        ]
        
        try:
            await self.ws.send_json(order_message)
            
            # Create future for response
            future = asyncio.Future()
            self.pending_orders[external_order_id] = {
                'future': future,
                'order_info': {
                    'instrument_id': instrument_id,
                    'side': side,
                    'quantity': quantity,
                    'external_order_id': external_order_id
                },
                'timestamp': time.time()
            }
            
            logging.info(f"Script {SCRIPT_ID}: CS Binance market {side} order sent: instrument_id={instrument_id}, quantity={quantity}, external_order_id={external_order_id}")
            
            # Wait for response with timeout
            try:
                response = await asyncio.wait_for(future, timeout=CS_ORDER_PLACE_RESPONSE_TIMEOUT)
                return response
            except asyncio.TimeoutError:
                logging.warning(f"Script {SCRIPT_ID}: CS order {external_order_id} placeResponse timeout")
                # Check if we have a cached response
                if external_order_id in self.order_responses:
                    return self.order_responses[external_order_id]
                return None
            finally:
                # Clean up pending order after timeout
                if external_order_id in self.pending_orders:
                    del self.pending_orders[external_order_id]
                    
        except Exception as e:
            logging.error(f"Script {SCRIPT_ID}: Error sending CS Binance order: {e}")
            if external_order_id in self.pending_orders:
                del self.pending_orders[external_order_id]
            return None
    
    async def close(self):
        """Close CS order WebSocket connection"""
        self.running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
        self.connected = False
        self.authenticated = False
        logging.info(f"Script {SCRIPT_ID}: CS Binance Order WS closed")

# Global CS Binance Order WS instance (legacy - kept for compatibility)
CS_BINANCE_ORDER_WS = None

# CS Trading Adapter WebSocket and tracking (matching taker code architecture)
cs_fill_ws = None
cs_fill_ws_task = None
pending_orders = {}  # Dictionary to track pending orders: {order_uuid: {"place_response": None}}
fill_messages = {}  # Dictionary to store fill messages: {order_uuid: [fill_data, ...]}

# Symbol to instrument ID cache - maps Binance symbol (e.g., "ETHUSDT") to CS instrument ID
_symbol_to_instrument_id_cache = {}

async def enable_orders_function():
    global maker_enable_orders, order_lock, orders_enabled_previous_state
    try:
        
        if not BinTR_order_ws.authenticated or not BinTR_order_ws.connected or not BINANCE_ws_connected or not BinTR_ws_connected:
            logging.error("Cannot enable orders: Not authenticated or missing websocket connection")
            return False
        
        await BinTR_order_ws.clear_buffer()
        time.sleep(0.1)

        maker_enable_orders = True
        
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Lock] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Lock] = 0

        await redis_pool.set('maker_enable_orders', 1)
        order_lock = False
        orders_enabled_previous_state = True
        logging.info("Orders enabled!")
    except Exception as e:
        logging.error(f"Error enabling orders: {str(e)}")
        await disable_orders("Error enabling orders")

async def disable_orders(reason: str = "Unknown"):
    global maker_enable_orders, order_lock, BINANCE_ws_connected, BinTR_ws_connected

    try:

        maker_enable_orders = False
        await redis_pool.set('maker_enable_orders', 0)
        logging.info(f"Disabling orders due to {reason}")
        
        logging.info("Cancelling all BinTR open orders...")
        await cancel_all_BinTR_open_orders()
        await BinTR_order_ws.clear_buffer()
        
        if not BINANCE_ws_connected:
            logging.info("Disabling orders due to error in one or more BINANCE websocket connections!")
            if not BINANCE_ws_connected:
                logging.info("Binance websocket disconnected")
        elif not BinTR_ws_connected:
            logging.info("Disabling orders due to error in one or more BinTR websocket connections!")
            if not BinTR_ws_connected:
                logging.info("BinTR websocket disconnected")
        else:
            logging.info(f"Disabling orders due to {reason}")
    except Exception as e:
        logging.error(f"Error disabling orders: {str(e)}")
    

async def process_buy_action(idx, buy_action_type):
    """Process buy action (open position or cancel buy order)"""
    global prev_buy_order_prices, buy_orders_disabled
    
    # Check if buy orders are disabled (due to BalanceNotEnough)
    if buy_orders_disabled and buy_action_type == 1:  # Only skip for place orders, not cancel
        symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        logging.debug(f"Script {SCRIPT_ID}: Skipping buy order for {symbol} - buy orders disabled (BalanceNotEnough recovery in progress)")
        return
    
    try:
        if buy_action_type in [1, 2]:
            symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
             
            # CRITICAL: Check both lock AND order ID to prevent duplicates (HFT-optimized: minimal operations)
            current_lock = arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock]
            if current_lock == 1:
                return  # Already locked - skip (no logging in hot path)
            
            # CRITICAL: For place orders (action_type == 1), also check if order already exists
            if buy_action_type == 1:
                current_order_id = arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID]
                if current_order_id > 0:
                    return  # Order already exists - skip (no logging in hot path)
            
            # Set lock atomically and track reason (no logging for HFT performance)
            lock_timestamp = time.time()
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock] = 1
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock_Time] = lock_timestamp
            lock_reason = f"process_buy_action - action_type={buy_action_type}"
            order_lock_reasons[(idx, 1)] = {"reason": lock_reason, "timestamp": lock_timestamp, "symbol": symbol}
            
            if buy_action_type == 2:
                buy_order_id = arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID]           
                if buy_order_id and buy_order_id > 0:
                    # HFT-OPTIMIZED: Fire-and-forget cancel - create task, don't await
                    # Response (401/602) will be handled by message handler asynchronously
                    # Lock will be released by 401 handler or cleanup_stale_locks if delayed
                    asyncio.create_task(BinTR_order_ws.cancel_order(buy_order_id))
                    # Don't wait for result - message handler processes response
                else:
                    # No order ID to cancel (order_id is 0 or negative), release lock immediately
                    release_order_lock(idx, buy_action_type, "cancel action - no order ID to cancel")
            elif buy_action_type == 1:
                await give_BinTR_limit_order(idx, buy_action_type)
    except Exception as e:
        symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        logging.error(f"Error processing buy action {buy_action_type} for index {idx}: {e}")
        release_order_lock(idx, buy_action_type, "error in process_buy_action")
        return

async def process_sell_action(idx, sell_action_type):
    """Process sell action (close position or cancel sell order)"""
    global prev_sell_order_prices, sell_orders_disabled_symbols
    
    # Check if sell orders are disabled for this symbol (due to BalanceNotEnough)
    if sell_action_type == 3:  # Only skip for place orders, not cancel
        symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        if symbol in sell_orders_disabled_symbols:
            logging.debug(f"Script {SCRIPT_ID}: Skipping sell order for {symbol} - sell orders disabled for this symbol (BalanceNotEnough recovery in progress)")
            return
    
    try:
        if sell_action_type in [3, 4]:
            symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]

            # CRITICAL: Check both lock AND order ID to prevent duplicates (HFT-optimized: minimal operations)
            current_lock = arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock]
            if current_lock == 1:
                return  # Already locked - skip (no logging in hot path)
            
            # CRITICAL: For place orders (action_type == 3), also check if order already exists
            if sell_action_type == 3:
                current_order_id = arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID]
                if current_order_id > 0:
                    return  # Order already exists - skip (no logging in hot path)
            
            # Set lock atomically and track reason (no logging for HFT performance)
            lock_timestamp = time.time()
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock] = 1
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock_Time] = lock_timestamp
            lock_reason = f"process_sell_action - action_type={sell_action_type}"
            order_lock_reasons[(idx, 3)] = {"reason": lock_reason, "timestamp": lock_timestamp, "symbol": symbol}

            if sell_action_type == 4:
                sell_order_id = arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID]
                if sell_order_id and sell_order_id > 0:
                    # HFT-OPTIMIZED: Fire-and-forget cancel - create task, don't await
                    # Response (401/602) will be handled by message handler asynchronously
                    # Lock will be released by 401 handler or cleanup_stale_locks if delayed
                    asyncio.create_task(BinTR_order_ws.cancel_order(sell_order_id))
                    # Don't wait for result - message handler processes response
                else:
                    # No order ID to cancel (order_id is 0 or negative), release lock immediately
                    release_order_lock(idx, sell_action_type, "cancel action - no order ID to cancel")
            elif sell_action_type == 3:
                await give_BinTR_limit_order(idx, sell_action_type)
    except Exception as e:
        symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        logging.error(f"Error processing sell action {sell_action_type} for index {idx}: {e}")
        release_order_lock(idx, sell_action_type, "error in process_sell_action")
        return

async def check_and_process_actions():
    """
    Processes both buy and sell actions for a single symbol.
    Prioritizes cancel actions for immediate execution.
    """
    global prev_buy_order_prices, prev_sell_order_prices
    # Separate cancel and place actions
    cancel_tasks = []

    # Use NumPy to find action indices - only calculate cancel indices here
    # Place indices will be calculated in process_order_place_actions() when needed
    buy_cancel_action_indices = np.where(arbitrage_table_np[:, arbit_config.col_Buy_ActionType] == 2)[0]
    sell_cancel_action_indices = np.where(arbitrage_table_np[:, arbit_config.col_Sell_ActionType] == 4)[0]
      
    # Process ALL cancel actions first (no limit on cancels)
    # HFT-OPTIMIZED: Fire-and-forget cancel tasks - don't wait for completion
    # This allows cancels to execute in parallel and immediately proceed to next cycle
    has_cancel_actions = False
    if buy_cancel_action_indices.size > 0:
        has_cancel_actions = True
        for idx in buy_cancel_action_indices:
            # Create task but don't await - let it run in background
            asyncio.create_task(process_buy_action(idx, 2))
    
    if sell_cancel_action_indices.size > 0:
        has_cancel_actions = True
        for idx in sell_cancel_action_indices:
            # Create task but don't await - let it run in background
            asyncio.create_task(process_sell_action(idx, 4))
    
    # HFT-OPTIMIZED: Process place actions immediately after firing cancel tasks
    # Cancel tasks run in parallel, we don't wait for them
    # Response handlers (401/602) will process confirmations asynchronously
    # This maximizes throughput - cancels and places can happen in same cycle
    await process_order_place_actions()

    return True

async def process_order_place_actions():

    place_tasks = []
    # Then process place orders (limited to max_place_actions total)
    max_buy_place_actions = 4
    max_sell_place_actions = 4
    processed_count = 0
    
    # CRITICAL: Track which symbols are being processed to prevent duplicates
    symbols_in_progress = set()
    
    buy_place_action_indices = np.where(arbitrage_table_np[:, arbit_config.col_Buy_ActionType] == 1)[0]
    sell_place_action_indices = np.where(arbitrage_table_np[:, arbit_config.col_Sell_ActionType] == 3)[0]
    
    if buy_place_action_indices.size > 0:
        for idx in buy_place_action_indices:
            if processed_count < max_buy_place_actions:
                symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
                # CRITICAL: Prevent same symbol from being processed multiple times (HFT-optimized: fast set lookup)
                if symbol in symbols_in_progress:
                    continue  # Skip duplicate (no logging in hot path)
                
                # Check if symbol has sufficient balance for buy orders
                BinTR_free_TRY = arbitrage_table_np[idx, arbit_config.col_BinTR_free_TRY]
                min_order_amount = arbitrage_table_np[idx, arbit_config.col_MinBuyOrderAmount_TRY]
                if not np.isnan(BinTR_free_TRY) and not np.isnan(min_order_amount) and BinTR_free_TRY >= min_order_amount:
                    symbols_in_progress.add(symbol)
                    place_tasks.append(asyncio.create_task(process_buy_action(idx, 1)))
                    processed_count += 1

    if sell_place_action_indices.size > 0:
        for idx in sell_place_action_indices:
            if processed_count < max_sell_place_actions:
                symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
                # CRITICAL: Prevent same symbol from being processed multiple times (HFT-optimized: fast set lookup)
                if symbol in symbols_in_progress:
                    continue  # Skip duplicate (no logging in hot path)
                
                BinTR_remaining_amount_TRY = arbitrage_table_np[idx, arbit_config.col_BinTR_PositionAmount_TRY]
                min_order_amount = arbitrage_table_np[idx, arbit_config.col_MinSellOrderAmount_TRY]
                if not np.isnan(BinTR_remaining_amount_TRY) and not np.isnan(min_order_amount) and BinTR_remaining_amount_TRY >= min_order_amount:
                    symbols_in_progress.add(symbol)
                    place_tasks.append(asyncio.create_task(process_sell_action(idx, 3)))
                    processed_count += 1
        
    # Execute place orders
    if place_tasks:
        await asyncio.gather(*place_tasks)

    return True

async def check_and_process_actions_loop():
    while True:
        try:
            if current_state == ArbitrageState.RUNNING and (BINANCE_ws_connected or CS_WS_CONNECTED) and BinTR_ws_connected and maker_enable_orders:
                await check_and_process_actions()

        except Exception as e:
            logging.error(f"Error in check and process actions loop: {e}")
        
        await asyncio.sleep(0.001)
            
async def calculate_arbitrage_BinanceBinTR_loop():
   
    while True:
        try:
            
            if current_state == ArbitrageState.RUNNING and (BINANCE_ws_connected or CS_WS_CONNECTED) and BinTR_ws_connected:
                await calculate_arbitrage()

        except Exception as e:
            logging.error(f"Error in arbitrage loop: {e}")
        
        await asyncio.sleep(0.001)

    
def floor_precision(value, precision):
    multiplier = 10 ** precision
    return math.floor(value * multiplier) / multiplier

def release_order_lock(idx, order_type, release_reason):
    """Release order lock for a given symbol and order type"""
    symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
    release_timestamp = time.time()

    # HFT-OPTIMIZED: Fast path - minimal operations, no logging
    # CRITICAL: Only clear price cache if order was NOT successfully placed
    # If order was successfully placed (601 success), keep price in cache to prevent duplicates
    should_clear_price = "601 order placed" not in release_reason and "order placed message received" not in release_reason
    
    if order_type in [1, 2]:
        if arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock] == 1:
            lock_key = (idx, 1)
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock] = 0
            arbitrage_table_np[idx, arbit_config.col_Buy_ActionType] = 0
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock_Time] = release_timestamp
            # Only clear price cache if order was NOT successfully placed
            if should_clear_price:
                prev_buy_order_prices[symbol] = 0
            # Clean up lock reason tracking
            order_lock_reasons.pop(lock_key, None)
    elif order_type in [3, 4]:
        if arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock] == 1:
            lock_key = (idx, 3)
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock] = 0
            arbitrage_table_np[idx, arbit_config.col_Sell_ActionType] = 0
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock_Time] = release_timestamp
            # Only clear price cache if order was NOT successfully placed
            if should_clear_price:
                prev_sell_order_prices[symbol] = 0
            # Clean up lock reason tracking
            order_lock_reasons.pop(lock_key, None)
    return

def calculate_lock_stale_duration(idx, order_type):
    """Check if a lock has been held too long"""
    current_time = time.time()
    if order_type in [1, 2]:
        lock_time = arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock_Time]
    elif order_type in [3, 4]:
        lock_time = arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock_Time]
    
    return (current_time - lock_time)

lock_timeout_seconds = 0.5  # 500ms timeout for HFT - 601 responses typically arrive in <100ms, this catches truly stuck locks quickly

async def check_stale_locks():
    """
    Check for stale locks, log them, and automatically release them.
    
    Common causes of stale locks:
    1. Authentication failure - WebSocket disconnected or auth expired after lock was set
    2. Network issues - Order sent but 601/401/402 messages lost in transit
    3. WebSocket disconnection - Connection lost after order sent, before response received
    4. Message handler failure - Response received but handler couldn't find order (idx=None, order_id mismatch)
    5. Exception during order send - Error occurred but lock wasn't released
    """
    stale_locks = []
    global external_order_id_to_symbol
    
    # Check authentication status - if not authenticated, all locks are potentially stale
    auth_status = BinTR_order_ws.authenticated if BinTR_order_ws else False
    ws_connected = BinTR_order_ws.connected if BinTR_order_ws else False
    
    # Clean up old external_order_id mappings (older than 30 seconds)
    current_time = time.time()
    old_external_ids = [ext_id for ext_id, mapping in external_order_id_to_symbol.items() 
                        if (current_time - mapping.get("timestamp", 0)) > 30]
    for ext_id in old_external_ids:
        external_order_id_to_symbol.pop(ext_id, None)

    
    for idx in range(len(arbitrage_table_np)):
        # Check buy locks
        if arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock] == 1:
            lock_duration = calculate_lock_stale_duration(idx, 1)
            if lock_duration > lock_timeout_seconds:
                symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
                buy_order_id = arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID]
                lock_time = arbitrage_table_np[idx, arbit_config.col_Buy_Order_Lock_Time]
                lock_key = (idx, 1)
                lock_info = order_lock_reasons.get(lock_key, {})
                lock_reason = lock_info.get("reason", "unknown")
                lock_acquired_time_str = datetime.fromtimestamp(lock_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                stale_locks.append({
                    "symbol": symbol,
                    "side": "buy",
                    "duration": lock_duration,
                    "order_id": buy_order_id,
                    "idx": idx,
                    "lock_time": lock_time,
                    "lock_reason": lock_reason,
                    "lock_acquired_time_str": lock_acquired_time_str,
                    "order_type": 1
                })
        
        # Check sell locks  
        if arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock] == 1:
            lock_duration = calculate_lock_stale_duration(idx, 3)
            if lock_duration > lock_timeout_seconds:
                symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
                sell_order_id = arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID]
                lock_time = arbitrage_table_np[idx, arbit_config.col_Sell_Order_Lock_Time]
                lock_key = (idx, 3)
                lock_info = order_lock_reasons.get(lock_key, {})
                lock_reason = lock_info.get("reason", "unknown")
                lock_acquired_time_str = datetime.fromtimestamp(lock_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                stale_locks.append({
                    "symbol": symbol,
                    "side": "sell",
                    "duration": lock_duration,
                    "order_id": sell_order_id,
                    "idx": idx,
                    "lock_time": lock_time,
                    "lock_reason": lock_reason,
                    "lock_acquired_time_str": lock_acquired_time_str,
                    "order_type": 3
                })
    
    if stale_locks:
        # Check authentication status - if not authenticated, all locks are potentially stale
        auth_status = BinTR_order_ws.authenticated if BinTR_order_ws else False
        ws_connected = BinTR_order_ws.connected if BinTR_order_ws else False
        auth_warning = ""
        if not auth_status or not ws_connected:
            auth_warning = f" | ⚠️ AUTH/WS ISSUE: authenticated={auth_status}, connected={ws_connected}"
        
        logging.warning(f"Script {SCRIPT_ID}: Found {len(stale_locks)} stale lock(s), releasing them{auth_warning}:")
        for lock_info in stale_locks:
            # Determine likely cause based on context
            likely_cause = "unknown"
            if not auth_status or not ws_connected:
                likely_cause = "authentication/websocket disconnection"
            elif lock_info['order_id'] == 0 or lock_info['order_id'] == -1:
                likely_cause = "order send failed (no order ID set)"
            elif lock_info['duration'] > 1.0:
                likely_cause = "message lost/not received (long duration)"
            else:
                likely_cause = "response message delayed/missing"
            
            logging.warning(f"  - {lock_info['symbol']}: {lock_info['side']} lock | Duration: {lock_info['duration']:.2f}s ({lock_info['duration']*1000:.1f}ms) | Order ID: {lock_info['order_id']} | Lock acquired at: {lock_info['lock_acquired_time_str']} | Reason: {lock_info['lock_reason']} | Likely cause: {likely_cause}")
            # CRITICAL: Automatically release stale locks
            release_order_lock(lock_info['idx'], lock_info['order_type'], f"stale lock cleanup (duration: {lock_info['duration']:.2f}s, cause: {likely_cause})")

async def cleanup_stale_locks():
    """Legacy function - kept for compatibility, but now just calls check_stale_locks"""
    await check_stale_locks()


async def give_BinTR_limit_order(idx, order_type):

    global prev_buy_order_prices, prev_sell_order_prices, open_orders
    
    try:
        # HFT-OPTIMIZED: Skip symbol lookup until needed (only for error logging)
        # CRITICAL: Check if order already exists before proceeding (fast path - no logging)
        if order_type == 1:
            existing_order_id = arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID]
            if existing_order_id > 0:
                release_order_lock(idx, order_type, "buy order exists")
                return
        elif order_type == 3:
            existing_order_id = arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID]
            if existing_order_id > 0:
                release_order_lock(idx, order_type, "sell order exists")
                return
        
        BinTR_symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        
        amount_precision = int(arbitrage_table_np[idx, arbit_config.col_AmountPrecision])
        BinTR_order_price = None
        BinTR_order_amount = None

        BinTR_free_TRY = arbitrage_table_np[idx, arbit_config.col_BinTR_free_TRY]*0.9

        if order_type == 1:
            min_order_amount_TRY = int(arbitrage_table_np[idx, arbit_config.col_MinBuyOrderAmount_TRY])
            max_order_amount_TRY = int(min_order_amount_TRY * 1.5)
            BinTR_order_amount_TRY = random.randint(min_order_amount_TRY, max_order_amount_TRY)
            
            if BinTR_free_TRY < BinTR_order_amount_TRY:
                release_order_lock(idx, order_type, "buy free TRY less than order amount TRY")
                return
            
            BinTR_side = 'Buy'
            best_price = arbitrage_table_np[idx, arbit_config.col_BinTR_BidP1]
            
            BinTR_order_amount = floor_precision((BinTR_order_amount_TRY / best_price), amount_precision)
            BinTR_order_price_raw = arbitrage_table_np[idx, arbit_config.col_BinTR_orderable_bid_price]
            price_precision = int(arbitrage_table_np[idx, arbit_config.col_BinTR_price_precision])
            BinTR_order_price = round(BinTR_order_price_raw, price_precision)


            # HFT-OPTIMIZED: Fast dictionary lookup (O(1))
            prev_buy_order_price = prev_buy_order_prices.get(BinTR_symbol, 0)
            if prev_buy_order_price == BinTR_order_price:
                release_order_lock(idx, order_type, "buy same price")
                return
                    
            # HFT-OPTIMIZED: Direct comparison without intermediate variable
            if arbitrage_table_np[idx, arbit_config.col_OpenMargin] <= arbitrage_table_np[idx, arbit_config.col_OpenTriggerMargin]:
                release_order_lock(idx, order_type, "buy margin failed")
                return

        elif order_type == 3:
            BinTR_remaining_amount_TRY = arbitrage_table_np[idx, arbit_config.col_BinTR_PositionAmount_TRY]
            
            min_order_amount_TRY = int(arbitrage_table_np[idx, arbit_config.col_MinSellOrderAmount_TRY])
            max_order_amount_TRY = int(min_order_amount_TRY * 1.5)
            BinTR_order_amount_TRY = random.randint(min_order_amount_TRY, max_order_amount_TRY)
            
            BinTR_order_amount_TRY = min(BinTR_remaining_amount_TRY, BinTR_order_amount_TRY)

            if BinTR_order_amount_TRY < min_order_amount_TRY:
                release_order_lock(idx, order_type, "sell remaining amount is less than min order amount TRY")
                return

            BinTR_side = 'Sell'
            best_price = arbitrage_table_np[idx, arbit_config.col_BinTR_AskP1]
            
            BinTR_order_amount = floor_precision((BinTR_order_amount_TRY / best_price), amount_precision)
            BinTR_order_price_raw = arbitrage_table_np[idx, arbit_config.col_BinTR_orderable_ask_price]
            price_precision = int(arbitrage_table_np[idx, arbit_config.col_BinTR_price_precision])
            BinTR_order_price = round(BinTR_order_price_raw, price_precision)

            # HFT-OPTIMIZED: Fast dictionary lookup (O(1))
            prev_sell_order_price = prev_sell_order_prices.get(BinTR_symbol, 0)
            if prev_sell_order_price == BinTR_order_price:
                release_order_lock(idx, order_type, "sell same price")
                return

            # HFT-OPTIMIZED: Direct comparison without intermediate variable
            if arbitrage_table_np[idx, arbit_config.col_CloseMargin] <= arbitrage_table_np[idx, arbit_config.col_CloseTriggerMargin]:
                release_order_lock(idx, order_type, "sell margin failed")
                return

        # CRITICAL: Set temporary marker (-1) to prevent duplicate orders between send and 601
        # This closes the race condition window where order is sent but ID not yet set
        if order_type == 1:
            # Double-check order doesn't exist (race condition protection)
            if arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID] > 0:
                logging.warning(f"{BinTR_symbol}: Buy order ID already set before send, aborting duplicate")
                release_order_lock(idx, order_type, "buy order ID already exists before send")
                return
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID] = -1  # Temporary marker
        elif order_type == 3:
            # Double-check order doesn't exist (race condition protection)
            if arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID] > 0:
                logging.warning(f"{BinTR_symbol}: Sell order ID already set before send, aborting duplicate")
                release_order_lock(idx, order_type, "sell order ID already exists before send")
                return
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID] = -1  # Temporary marker
        
        try:
            # CRITICAL: Store price IMMEDIATELY before sending order to prevent duplicate price orders
            # This prevents race condition where same price order is sent before 601 response arrives
            if order_type == 1:  # Buy order
                prev_buy_order_prices[BinTR_symbol] = BinTR_order_price
            elif order_type == 3:  # Sell order
                prev_sell_order_prices[BinTR_symbol] = BinTR_order_price
            
            # Calculate order amount in TRY for tracking
            BinTR_order_amount_TRY_for_tracking = BinTR_order_amount * BinTR_order_price
            
            # Get external order ID before sending
            order_msg = BinTR_order_ws.create_order_message(BinTR_symbol, BinTR_order_price, BinTR_order_amount, BinTR_side)
            order_data = json.loads(order_msg.split("|")[1])
            external_order_id = order_data.get("externalOrderId")
            
            result = await BinTR_order_ws.send_order(BinTR_symbol, BinTR_order_price, BinTR_order_amount, BinTR_side, idx, order_type)
            
            if not result:
                logging.error(f"Failed to send order for {BinTR_symbol}")
                # Clear temporary marker on failure
                if order_type == 1:
                    arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID] = 0
                elif order_type == 3:
                    arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID] = 0
                release_order_lock(idx, order_type, "failed to send order")
                return
        except Exception as e:
            logging.error(f"Exception sending order for {BinTR_symbol}: {e}")
            # Clear temporary marker on exception
            if order_type == 1:
                arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID] = 0
            elif order_type == 3:
                arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID] = 0
            release_order_lock(idx, order_type, "exception sending order")
            return
            
    except Exception as e:
        logging.error(f"Error in give_BinTR_limit_order for {BinTR_symbol} (idx={idx}, order_type={order_type}): {e}")
        release_order_lock(idx, order_type, "error in give_BinTR_limit_order")
        return
    
    
binance_retry_limit = 3

async def give_BINANCE_market_order(idx, order_type, BinTR_filled_amount, BinTR_avg_filled_price, BinTR_filled_amount_TRY, BinTR_executed_margin):
    
    retry_count = 0
    binance_trade = None
    # Process Binance order if there were any fills
    while retry_count <= binance_retry_limit:
        try:
            amount_precision = int(arbitrage_table_np[idx, arbit_config.col_AmountPrecision])
            order_amount = floor_precision(BinTR_filled_amount, amount_precision)
            Binance_symbol = arbitrage_table_np[idx, arbit_config.col_Binance_Symbol]
            if Binance_symbol in ["1000PEPEUSDT", "1000BONKUSDT", "1000SHIBUSDT", "1000FLOKIUSDT"]:
                order_amount = floor_precision(order_amount * 0.001, amount_precision)
            
            # Use CS orders if collocation is enabled, otherwise use REST API
            if collocation:
                binance_trade = await process_binance_order_CS(idx, Binance_symbol, order_type, order_amount, BinTR_avg_filled_price, BinTR_filled_amount_TRY)
            else:
                binance_trade = process_binance_order(idx, Binance_symbol, order_type, order_amount, BinTR_avg_filled_price, BinTR_filled_amount_TRY)
            
            # Break out of loop if order was successful (binance_trade is not None)
            if binance_trade is not None:
                break
            # If process_binance_order returned None, retry
            retry_count += 1
            if retry_count > binance_retry_limit:
                logging.error(f"Failed to execute Binance order after {binance_retry_limit} retries!")
                await update_binance_balances("Failed to execute Binance order after {binance_retry_limit} retries!")
                break
            logging.error(f"Retrying Binance order execution... ({retry_count}/{binance_retry_limit})")
            await asyncio.sleep(1)
                
        except Exception as e:
            logging.error(f"Error in Binance order execution: {str(e)}")
            retry_count += 1
            if retry_count > binance_retry_limit:
                logging.error(f"Failed to execute Binance order after {binance_retry_limit} retries!")
                await update_binance_balances("Failed to execute Binance order after {binance_retry_limit} retries!")
                break
            logging.error(f"Retrying Binance order execution... ({retry_count}/{binance_retry_limit})")
            await asyncio.sleep(1)

    # Only save if we have a valid trade
    if binance_trade is None:
        logging.error(f"Failed to execute Binance order: binance_trade is None")
        return False
        
    try:
        await save_binance_trade(binance_trade, BinTR_executed_margin, idx, order_type)
        return True

    except Exception as e:
        logging.error(f"Error in saving trade histories: {str(e)}") 
        await disable_orders("Error in saving trade histories")
        return False

async def handle_move_detection(idx, binance_trade, BinTR_executed_margin, binance_executed_margin, stop_margin, move_threshold, move_source):
    """
    Handle move detection: Set maker_type to 0 for the symbol, update Redis thresholds, cancel open orders, and save move history.
    """
    try:
        
        # Get BaseSymbol and BinTR symbol from arbitrage table first
        base_symbol = arbitrage_table_np[idx, arbit_config.col_Base_Symbol]
        
        # Set maker_type to 0 in the arbitrage table
        arbitrage_table_np[idx, arbit_config.col_Maker_Type] = 0
        logging.info(f"Set maker_type to 0 for symbol {base_symbol} (idx: {idx})")
        BinTR_symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
        binance_symbol = binance_trade.get('Symbol', '')
        
        # Cancel all open orders for this symbol immediately
        # Only cancel if BinTR_order_ws is initialized
        if BinTR_order_ws is not None:
            try:
                # Get buy and sell order IDs
                buy_order_id = int(arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID])
                sell_order_id = int(arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID])
                
                # Cancel individual orders if they exist
                if buy_order_id > 0:
                    await BinTR_order_ws.cancel_order(buy_order_id)
                    logging.info(f"Cancelling buy order {buy_order_id} for {base_symbol}")
                
                if sell_order_id > 0:
                    await BinTR_order_ws.cancel_order(sell_order_id)
                    logging.info(f"Cancelling sell order {sell_order_id} for {base_symbol}")
                
                # Also cancel all orders for this pair symbol (in case there are any other orders)
                await BinTR_order_ws.cancel_all_orders(pair_symbol=BinTR_symbol)
                logging.info(f"Cancelled all open orders for {base_symbol} (BinTR symbol: {BinTR_symbol})")
            except Exception as e:
                logging.error(f"Error cancelling orders for {base_symbol}: {e}")
        else:
            logging.debug(f"BinTR_order_ws not initialized yet, skipping order cancellation for {base_symbol}")
        
        
        
        # Update maker_arbitrage_thresholds in Redis
        redis = await get_redis_connection()
        thresholds_json = await redis.get('maker_arbitrage_thresholds')
        
        if thresholds_json:
            thresholds_list = ujson.loads(thresholds_json)
            # Find and update the threshold for this symbol
            updated = False
            for threshold in thresholds_list:
                if threshold.get('Symbol') == base_symbol:
                    threshold['Maker_Type'] = 0
                    updated = True
                    logging.info(f"Updated Maker_Type to 0 in thresholds for {base_symbol}")
                    break
            
            if updated:
                # Save updated thresholds back to Redis
                await redis.set('maker_arbitrage_thresholds', ujson.dumps(thresholds_list))
            else:
                logging.warning(f"Symbol {base_symbol} not found in maker_arbitrage_thresholds")
        else:
            logging.warning("maker_arbitrage_thresholds not found in Redis")
        
        # Save move history to Redis
        move_history = {
            'MoveTime': binance_trade.get('OrderTime', datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]),
            'OrderID': binance_trade.get('OrderID', ''),
            'Symbol': base_symbol,
            'Core': SCRIPT_ID,
            'BinanceExecutedMargin': binance_executed_margin,
            'BinTRExecutedMargin': BinTR_executed_margin,
            'StopMargin': stop_margin,
            'MoveThreshold': move_threshold,
            'MarginDifference': binance_executed_margin - stop_margin,
            'PNL': binance_trade.get('PNL', 0),
            'Source': move_source
        }
        
        await redis_pool.rpush('maker_move_history', ujson.dumps(move_history))
        logging.warning(f"Move detected for {base_symbol}: Binance margin={binance_executed_margin}, BinTR margin={BinTR_executed_margin}, Stop margin={stop_margin}, Margin difference={stop_margin - binance_executed_margin}")
        
        return True
    except Exception as e:
        logging.error(f"Error handling move detection: {e}")
        return False

async def save_binance_trade(binance_trade, BinTR_executed_margin, idx=None, order_type=None):
   
    binance_executed_margin = round(binance_trade['ExecutedMargin']*10000,2)
    BinTR_executed_margin = round(BinTR_executed_margin*10000,2)

    stop_margin = 0
    move_threshold = 15
    
    # Get base_symbol and move_threshold early for use in logging
    if idx is not None:
        base_symbol = arbitrage_table_np[idx, arbit_config.col_Base_Symbol]
        move_threshold = arbitrage_table_np[idx, arbit_config.col_MoveThreshold]
    else:
        base_symbol = binance_trade.get('Symbol', '')

    if order_type == 1:
        stop_margin = arbitrage_table_np[idx, arbit_config.col_OpenStopMargin]
    else:
        stop_margin = arbitrage_table_np[idx, arbit_config.col_CloseStopMargin]

    # move_threshold is in basis points, so we compare with the difference
    stop_margin = stop_margin * 10000
    binance_margin_diff = BinTR_executed_margin - binance_executed_margin
    BinTR_margin_diff = stop_margin - BinTR_executed_margin
    cumulative_diff = stop_margin - binance_executed_margin

    BINANCE_MOVE = binance_margin_diff > move_threshold
    BinTR_MOVE = BinTR_margin_diff > move_threshold
    BOTH_MOVE = BINANCE_MOVE and BinTR_MOVE
    CUMULATIVE_MOVE = cumulative_diff > move_threshold

    if CUMULATIVE_MOVE:  # If difference exceeds threshold
        if idx is not None:
            SOURCE = "BINANCE" if BINANCE_MOVE else "BinTR" if BinTR_MOVE else "BOTH" if BOTH_MOVE else "NONE"
            await handle_move_detection(idx, binance_trade, BinTR_executed_margin, binance_executed_margin, stop_margin, move_threshold, SOURCE)
            logging.warning(f"{base_symbol}: CUMULATIVE MARKET MOVE ORDER MOVED MORE THAN {move_threshold} BASIS POINTS FROM STOP MARGIN!!!!")
        else:
            await disable_orders(f"{base_symbol}: CUMULATIVE MARKET MOVE ORDER MOVED MORE THAN {move_threshold} BASIS POINTS FROM STOP MARGIN!!!!")

    await redis_pool.rpush('maker_BINANCE_trade_history', ujson.dumps(binance_trade))
    logging.info(f"{base_symbol}: BINANCE trade saved to redis ---> Binance ExecutedMargin:{binance_executed_margin} PNL:{binance_trade['PNL']}TL")
    logging.info(f"{base_symbol}: BinTR ExecutedMargin: BinTR_executed_margin, Stop Margin: {stop_margin}, Binance Margin Difference: {binance_margin_diff}, BinTR Margin Difference: {BinTR_margin_diff}, Cumulative Margin Difference: {cumulative_diff}")
    logging.info(f"{base_symbol}: Binance Margin Difference:{binance_margin_diff}, BinTR Margin Difference:{BinTR_margin_diff}, Cumulative Margin Difference:{cumulative_diff}")
  

BINANCE_fee_rate = 0.00032
BinTR_fee_rate = 0.00020

binance_exceptional_symbols = ["1000PEPEUSDT", "1000BONKUSDT", "1000SHIBUSDT", "1000FLOKIUSDT"]

def aggregate_cs_fills(fill_list):
    """
    Aggregate multiple CS fill messages for a single order and calculate weighted average price.
    
    Args:
        fill_list: List of fill dictionaries from CS fill messages
        
    Returns:
        Aggregated fill dictionary with:
        - quantity: Total filled quantity
        - price: Weighted average execution price
        - side: Order side (from first fill)
        - ownOrderId: Order ID (from first fill)
        - exchangeTimestampNs: Latest exchange timestamp
        - All other fields from the first fill
    """
    if not fill_list or len(fill_list) == 0:
        return None
    
    if len(fill_list) == 1:
        # Single fill - return as is
        return fill_list[0]
    
    # Multiple fills - aggregate
    total_quantity = 0.0
    total_value = 0.0
    latest_timestamp_ns = 0
    
    # Use first fill as base for metadata
    aggregated = fill_list[0].copy()
    
    # Aggregate quantity and calculate weighted average price
    for fill in fill_list:
        fill_quantity = float(fill.get("quantity", fill.get("executedQty", 0)))
        fill_price = float(fill.get("price", fill.get("avgPrice", 0)))
        fill_timestamp_ns = fill.get("exchangeTimestampNs", 0)
        
        if fill_quantity > 0 and fill_price > 0:
            total_quantity += fill_quantity
            total_value += fill_quantity * fill_price
        
        # Track latest timestamp
        if fill_timestamp_ns > latest_timestamp_ns:
            latest_timestamp_ns = fill_timestamp_ns
    
    # Calculate weighted average price
    if total_quantity > 0:
        average_price = total_value / total_quantity
    else:
        # Fallback to first fill's price if no valid quantities
        average_price = float(fill_list[0].get("price", fill_list[0].get("avgPrice", 0)))
    
    # Update aggregated fill with totals
    aggregated["quantity"] = str(total_quantity)
    aggregated["executedQty"] = total_quantity  # Also set executedQty for compatibility
    aggregated["price"] = str(average_price)
    aggregated["avgPrice"] = average_price  # Also set avgPrice for compatibility
    if latest_timestamp_ns > 0:
        aggregated["exchangeTimestampNs"] = latest_timestamp_ns
    
    return aggregated

def get_instrument_id(symbol):
    """Get instrument ID from symbol using pre-cached mapping (O(1) lookup) - matching taker code"""
    return _symbol_to_instrument_id_cache.get(symbol.upper())

# HFT Optimization: Pre-built order templates (matching taker code)
_cs_order_owner_template = {
    "instanceName": CLIENT_APP_NAME,
    "processId": CLIENT_PROCESS_ID,
    "org": CLIENT_ORG,
    "version": CLIENT_APP_VER
}

_cs_order_entry_template = {
    "type": "MARKET",
    "timeInForce": "GOOD_TILL_CANCEL",
    "postOnly": True,
    "displayQuantity": None,
    "triggerPrice": None,
    "pegOffsetValue": None,
    "openClose": None,
    "walletType": "ACCOUNT",
    "closeOnly": False,
    "reduceOnly": False,
    "autoBorrow": False,
    "autoRepay": False,
    "useSpare": False
}

_cs_order_request_template = {
    "msgType": "place",
    "tracing": {}
}

async def cs_fill_message_listener():
    """Listen for fill messages from CryptoStruct Trading Adapter WebSocket - matching taker code"""
    global cs_fill_ws, fill_messages, pending_orders
    
    ws_url = f"ws://{CS_TRADING_ADAPTER_HOST}:{CS_TRADING_ADAPTER_PORT}{CS_TRADING_ADAPTER_WS_ENDPOINT}"
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    ws_url,
                    heartbeat=20,
                    timeout=aiohttp.ClientTimeout(total=30, connect=10)
                ) as ws:
                    cs_fill_ws = ws
                    logging.info(f"Script {SCRIPT_ID}: Connected to CS Trading Adapter for fill messages")
                    
                    # Send login message
                    login_message = {
                        "msgType": "login",
                        "accountId": ACCOUNT_ID,
                        "requestId": str(uuid.uuid4()),
                        "processId": CLIENT_PROCESS_ID,
                        "version": CLIENT_APP_VER,
                        "options": {
                            "includeContext": True,
                            "filterByProcessId": True
                        }
                    }
                    await ws.send_json(login_message)
                    
                    # Wait for login response
                    try:
                        login_response = await asyncio.wait_for(ws.receive(), timeout=10.0)
                        if login_response.type == aiohttp.WSMsgType.TEXT:
                            login_data = json.loads(login_response.data)
                            if login_data.get("msgType") == "loginResponse" and "error" not in login_data:
                                logging.info(f"Script {SCRIPT_ID}: Logged in to CS Trading Adapter")
                    except asyncio.TimeoutError:
                        logging.warning(f"Script {SCRIPT_ID}: No login response, continuing anyway...")
                    
                    # Listen for messages
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                msg_type = data.get("msgType")
                                
                                if msg_type == "fill":
                                    # Extract fill data
                                    if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                                        # Check if there are multiple fills in the message
                                        fill_count = len(data["data"])
                                        if fill_count > 1:
                                            logging.debug(f"Script {SCRIPT_ID}: Fill message contains {fill_count} entries")
                                        
                                        # Process all fills in the data array
                                        for fill in data["data"]:
                                            own_order_id = fill.get("ownOrderId")
                                            if own_order_id:
                                                # Accumulate fills for the same order (orders can execute in multiple parts)
                                                if own_order_id not in fill_messages:
                                                    fill_messages[own_order_id] = []
                                                    logging.info(f"Script {SCRIPT_ID}: Received first fill message for order {own_order_id}")
                                                
                                                # Add this fill to the list
                                                fill_messages[own_order_id].append(fill)
                                                logging.debug(f"Script {SCRIPT_ID}: Accumulated fill {len(fill_messages[own_order_id])} for order {own_order_id}")
                                                
                                                # Signal the waiting task if it exists
                                                if own_order_id in pending_orders and "event" in pending_orders[own_order_id]:
                                                    pending_orders[own_order_id]["event"].set()
                                
                                elif msg_type == "placeResponse":
                                    # Handle place response (success or failure)
                                    results = data.get("results", [])
                                    if results and len(results) > 0:
                                        result = results[0]
                                        own_order_id = result.get("ownOrderId")
                                        if own_order_id:
                                            if own_order_id in pending_orders:
                                                if "error" in result:
                                                    # Order failed - will be retried
                                                    error_msg = result.get("error", {}).get("message", "Unknown error")
                                                    logging.warning(f"Script {SCRIPT_ID}: Order {own_order_id} failed: {error_msg}")
                                                    pending_orders[own_order_id]["place_response"] = result
                                                else:
                                                    # Order succeeded
                                                    logging.info(f"Script {SCRIPT_ID}: Order {own_order_id} placed successfully")
                                                    pending_orders[own_order_id]["place_response"] = result
                                            else:
                                                logging.debug(f"Script {SCRIPT_ID}: Received placeResponse for unknown order {own_order_id}")
                                
                            except Exception as e:
                                logging.error(f"Script {SCRIPT_ID}: Error processing fill message: {e}")
                        
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logging.warning(f"Script {SCRIPT_ID}: CS fill WebSocket closed/error: {msg.type}")
                            break
                            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Script {SCRIPT_ID}: Error in CS fill message listener: {e}")
            await asyncio.sleep(5)  # Wait before reconnecting
    cs_fill_ws = None

async def send_cs_order_with_retry(symbol, side, quantity, price, account_id, max_retries=3):
    """Send CS order with retry logic (up to 3 times) - HFT OPTIMIZED - matching taker code"""
    global pending_orders, fill_messages, cs_fill_ws
    
    # Fast path: Check WebSocket connection (single check, no function call overhead)
    if cs_fill_ws is None or cs_fill_ws.closed:
        logging.error("CS fill message WebSocket not connected")
        return None
    
    # HFT: Pre-compute instrument_id (O(1) lookup from cache)
    instrument_id = get_instrument_id(symbol)
    if instrument_id is None:
        logging.error(f"Script {SCRIPT_ID}: Cannot find instrument ID for symbol: {symbol}")
        return None
    
    # HFT: Pre-convert quantity and price to strings (avoid repeated conversions)
    quantity_str = str(quantity)
    price_str = str(price) if price is not None else None
    
    # HFT: Use monotonic time for better performance (if available)
    try:
        time_func = time.monotonic
    except AttributeError:
        time_func = time.time
    
    order_uuid = str(uuid.uuid4())
    
    # Store order info for tracking (minimal dict)
    pending_orders[order_uuid] = {
        "place_response": None
    }
    
    # HFT: Pre-build static parts of order entry (copy template, modify only dynamic fields)
    for attempt in range(max_retries):
        try:
            # HFT: Build order entry using template copy (faster than building from scratch)
            order_entry = _cs_order_entry_template.copy()
            order_entry.update({
                "ownOrderId": order_uuid,
                "clientOrderId": order_uuid,
                "instrumentId": instrument_id,
                "side": side,
                "price": price_str,
                "totalQuantity": quantity_str,
                "owner": {
                    "instanceId": str(uuid.uuid4()),
                    **_cs_order_owner_template  # Merge template
                }
            })
            
            # HFT: Build order request using template copy
            order_request = _cs_order_request_template.copy()
            order_request.update({
                "accountId": account_id,
                "requestId": str(uuid.uuid4()),
                "entries": [order_entry]
            })
            
            # HFT: Send order via WebSocket (no logging in hot path for first attempt)
            await cs_fill_ws.send_json(order_request)
            if attempt > 0:
                logging.info(f"Script {SCRIPT_ID}: Sent CS order {order_uuid} (attempt {attempt + 1}/{max_retries})")
            
            # HFT: Wait for placeResponse with optimized polling (reduced sleep time)
            timeout = CS_ORDER_PLACE_RESPONSE_TIMEOUT
            start_time = time_func()
            place_response_received = False
            poll_interval = 0.01  # 10ms polling for HFT (was 0.1s = 100ms)
            
            while (time_func() - start_time) < timeout:
                # Fast path: Direct dict access (avoid .get() overhead)
                order_info = pending_orders.get(order_uuid)
                if order_info and order_info.get("place_response") is not None:
                    place_response = order_info["place_response"]
                    place_response_received = True
                    
                    # Fast path: Check error (direct key access)
                    if isinstance(place_response, dict) and "error" in place_response:
                        error_msg = place_response.get("error", {}).get("message", "Unknown error")
                        if attempt == 0:  # Only log on first attempt
                            logging.warning(f"Script {SCRIPT_ID}: Order failed: {error_msg}")
                        
                        if attempt < max_retries - 1:
                            # Clear and retry - also clean up any leftover fill_messages from previous attempt
                            order_info["place_response"] = None
                            if order_uuid in fill_messages:
                                del fill_messages[order_uuid]
                            await asyncio.sleep(0.1)  # Brief delay before retry
                            break
                        else:
                            del pending_orders[order_uuid]
                            return None
                    else:
                        # Order succeeded - proceed to wait for fill
                        break
                
                await asyncio.sleep(poll_interval)
            
            if not place_response_received:
                if attempt == 0:  # Only log on first attempt
                    logging.warning(f"Script {SCRIPT_ID}: Timeout waiting for placeResponse")
                if attempt < max_retries - 1:
                    # Clean up any leftover fill_messages from previous attempt before retry
                    if order_uuid in fill_messages:
                        del fill_messages[order_uuid]
                    await asyncio.sleep(0.1)
                    continue
                else:
                    # Clean up on final failure
                    if order_uuid in fill_messages:
                        del fill_messages[order_uuid]
                    del pending_orders[order_uuid]
                    return None
            
            # HFT: Wait for fill message with optimized polling
            timeout = CS_ORDER_FILL_TIMEOUT
            start_time = time_func()
            
            fill_received_time = None
            grace_period = 0.05  # 50ms grace period to collect additional fills that may arrive separately
            while (time_func() - start_time) < timeout:
                # Fast path: Direct dict lookup
                if order_uuid in fill_messages:
                    fill_list = fill_messages[order_uuid]
                    
                    # Track when we first received a fill
                    if fill_received_time is None:
                        fill_received_time = time_func()
                        # Wait a short grace period to collect additional fills that may arrive separately
                        await asyncio.sleep(grace_period)
                        continue
                    
                    # After grace period, process all accumulated fills
                    time_since_first_fill = time_func() - fill_received_time
                    if time_since_first_fill >= grace_period:
                        # Process fills after grace period
                        fill_list = fill_messages.pop(order_uuid)  # Now pop and process
                        del pending_orders[order_uuid]
                        
                        # Aggregate multiple fills and calculate weighted average price
                        if not isinstance(fill_list, list):
                            # Handle legacy single fill format (shouldn't happen, but safety check)
                            fill_list = [fill_list]
                        
                        fill_data = aggregate_cs_fills(fill_list)
                        if fill_data:
                            fill_count = len(fill_list)
                            if fill_count > 1:
                                logging.info(f"Script {SCRIPT_ID}: Aggregated {fill_count} fills for order {order_uuid}, total qty: {fill_data.get('quantity')}, avg price: {fill_data.get('price')}")
                            return fill_data
                
                await asyncio.sleep(poll_interval)
            
            # Timeout waiting for fill
            if attempt == 0:  # Only log on first attempt
                logging.warning(f"Script {SCRIPT_ID}: Timeout waiting for fill message")
            # Clean up on timeout
            if order_uuid in fill_messages:
                del fill_messages[order_uuid]
            del pending_orders[order_uuid]
            return None
                    
        except Exception as e:
            if attempt == 0:  # Only log on first attempt
                logging.error(f"Script {SCRIPT_ID}: Error sending CS order: {e}")
            if attempt < max_retries - 1:
                # Clean up any leftover fill_messages from previous attempt before retry
                if order_uuid in fill_messages:
                    del fill_messages[order_uuid]
                await asyncio.sleep(0.1)
                continue
            else:
                # Clean up on final failure
                if order_uuid in fill_messages:
                    del fill_messages[order_uuid]
                if order_uuid in pending_orders:
                    del pending_orders[order_uuid]
                return None
    
    return None

async def process_binance_order_CS(idx, Binance_symbol, order_type, BinTR_filled_amount, BinTR_filled_price, BinTR_filled_amount_TRY):
    """Process Binance order via CS when collocation == 1 - matching taker code"""
    global cs_fill_ws
    
    if cs_fill_ws is None or cs_fill_ws.closed:
        logging.error(f"{Binance_symbol}: CS fill message WebSocket not connected")
        return None
    
    start_time = time.time()
    
    try:
        # Determine side
        if order_type == 1:  # Open trade: SELL on Binance
            Binance_side = "SELL"
        elif order_type == 3:  # Close trade: BUY on Binance
            Binance_side = "BUY"
        else:
            logging.error(f"{Binance_symbol}: Invalid order_type: {order_type}")
            return None
        
        # Convert side format: 'BUY'/'SELL' -> 'BID'/'ASK' for CS (matching taker code)
        cs_side = "BID" if Binance_side.upper() == "BUY" else "ASK"
        
        # Send order via CS with retry logic (matching taker code - price=None for market orders)
        fill_data = await send_cs_order_with_retry(
            symbol=Binance_symbol,
            side=cs_side,  # Use converted side (BID/ASK)
            quantity=BinTR_filled_amount,
            price=None,  # Market order - price not needed
            account_id=ACCOUNT_ID,
            max_retries=3
        )
        
        if not fill_data:
            # Failed after retries
            time_elapsed_binance_response = round((time.time() - start_time) * 1000, 0)
            logging.error(f"{Binance_symbol}: CS Binance order failed after {time_elapsed_binance_response}ms")
            return None
        
        # Successfully received fill data
        binance_execution_time_ns = fill_data.get("exchangeTimestampNs", 0)
        binance_execution_time = int(binance_execution_time_ns / 1_000_000) if binance_execution_time_ns else int(time.time() * 1000)
        time_elapsed_binance_response = round(binance_execution_time - start_time * 1000, 0)
        logging.info(f"{Binance_symbol}: Binance order filled in {time_elapsed_binance_response}ms")
        
        # Extract fill information (matching taker code format)
        binance_filled_amount = float(fill_data.get('quantity', fill_data.get('executedQty', BinTR_filled_amount)))
        binance_filled_price_usdt = float(fill_data.get('price', fill_data.get('avgPrice', 0)))
        order_id = fill_data.get('ownOrderId', fill_data.get('orderId', f"MAKER_{SCRIPT_ID}_{int(time.time() * 1000)}"))
        # Use exchangeTimestampNs if available, otherwise use updateTime
        if 'exchangeTimestampNs' in fill_data:
            update_time = int(fill_data['exchangeTimestampNs'] / 1_000_000)
        else:
            update_time = fill_data.get('updateTime', binance_execution_time)
        
        if binance_filled_price_usdt == 0:
            logging.error(f"{Binance_symbol}: Invalid fill price from CS: {fill_data}")
            return None
        
        binance_filled_amount_usdt = binance_filled_amount * binance_filled_price_usdt
        logging.info(f"{Binance_symbol}, BINANCE market {Binance_side} order executed via CS! Amount(coin): {binance_filled_amount}, Price: {binance_filled_price_usdt}")
        logging.info(f"{Binance_symbol}, BINANCE market {Binance_side} execution time: {time_elapsed_binance_response}ms")
        
        # Calculate PNL and margins (same logic as REST API)
        PNL = 0
        Gross_PNL = 0
        executed_margin = 0
        
        if Binance_symbol in binance_exceptional_symbols:
            BinTR_filled_amount = BinTR_filled_amount * 0.001
            BinTR_filled_price = BinTR_filled_price * 1000
        
        # Use USDTTRY from shared memory if available (C++ client updates it), otherwise use global variables
        if USE_CPP_OPTIMIZATION_BINANCE and get_binance_usdttry_rate:
            try:
                usdttry_rate_from_shm = get_binance_usdttry_rate()
                # Use the same rate for both bid and ask (C++ client provides average rate)
                binance_filled_price_TRY = binance_filled_price_usdt * usdttry_rate_from_shm
            except Exception as e:
                logging.warning(f"Failed to get USDTTRY from shared memory, using global: {e}")
                binance_filled_price_TRY = binance_filled_price_usdt * USDTTRY_bid if order_type == 1 else binance_filled_price_usdt * USDTTRY_ask
        else:
            binance_filled_price_TRY = binance_filled_price_usdt * USDTTRY_bid if order_type == 1 else binance_filled_price_usdt * USDTTRY_ask
        
        if order_type == 1:  # Open trade :  BINANCE SELL - BinTR BUY
            Gross_PNL = (binance_filled_price_TRY - BinTR_filled_price) * binance_filled_amount
            executed_margin = (binance_filled_price_TRY - BinTR_filled_price) / BinTR_filled_price
        else:  # Close trade :  BINANCE BUY - BinTR SELL
            Gross_PNL = (BinTR_filled_price - binance_filled_price_TRY) * binance_filled_amount
            executed_margin = (BinTR_filled_price - binance_filled_price_TRY) / binance_filled_price_TRY
        
        binance_filled_amount_TRY = binance_filled_amount * binance_filled_price_TRY
        
        BinTR_fee_TRY = BinTR_filled_amount_TRY * BinTR_fee_rate
        BINANCE_fee_TRY = binance_filled_amount_TRY * BINANCE_fee_rate
        total_fee_TRY = BinTR_fee_TRY + BINANCE_fee_TRY
        PNL = round(Gross_PNL - total_fee_TRY, 0)
        
        logging.info(f"{Binance_symbol}, Gross PNL: {Gross_PNL}, PNL: {PNL}, Executed margin: {executed_margin}")
        
        BinanceTimeDiff = int(time.time() * 1000) - int(update_time)
        
        # Create binance_trade dict (same format as REST API)
        binance_trade = {
            'TradeID': tradeID,
            'OrderID': order_id,
            'OrderTime': datetime.fromtimestamp(int(update_time) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # Include milliseconds
            'Symbol': Binance_symbol,
            'Side': Binance_side,
            'Amount': binance_filled_amount,
            'Price': binance_filled_price_usdt,
            'USDTTRY_bid': get_binance_usdttry_rate() if (USE_CPP_OPTIMIZATION_BINANCE and get_binance_usdttry_rate) else USDTTRY_bid,
            'USDTTRY_ask': get_binance_usdttry_rate() if (USE_CPP_OPTIMIZATION_BINANCE and get_binance_usdttry_rate) else USDTTRY_ask,
            'Price_TRY': binance_filled_price_TRY,
            'Amount_TRY': binance_filled_amount_TRY,
            'Amount_USDT': binance_filled_amount_usdt,
            'Fee': total_fee_TRY,
            'ExecutedMargin': executed_margin,
            'PNL': PNL,
            'BinanceTimeDiff': BinanceTimeDiff
        }
        
        update_binance_position(idx, binance_filled_amount, binance_filled_amount_usdt, order_type)
        
        return binance_trade
        
    except Exception as e:
        logging.error(f"{Binance_symbol}, Error in CS BINANCE order execution: {e}")
        logging.exception(f"Full traceback for CS Binance order error:")
        return None

def process_binance_order(idx, Binance_symbol, order_type, BinTR_filled_amount, BinTR_filled_price, BinTR_filled_amount_TRY):
    """Process Binance order - uses CS if collocation == 1, otherwise uses REST API"""
    global BINANCE, collocation
    
    # If collocation is enabled, use CS orders (async)
    if collocation:
        # Note: This function is called from async context, so we can use asyncio.run
        # But better to make the caller async and await this
        logging.warning(f"{Binance_symbol}: process_binance_order called with collocation=1, but function is sync. Use process_binance_order_CS instead.")
        return None
    
    # Original REST API implementation
    start_time = time.time()
    try:
        if order_type == 1:  # Open trade
            BINANCE_order = BINANCE.create_market_sell_order(Binance_symbol, BinTR_filled_amount)
        elif order_type == 3:  # Close trade
            BINANCE_order = BINANCE.create_market_buy_order(Binance_symbol, BinTR_filled_amount)
        else:
            BINANCE_order = None
    except Exception as e:
        logging.error(f"{Binance_symbol}, Error in BINANCE order execution: {e}")
        return None
    
    end_time = time.time()
    duration = end_time - start_time
    
    Binance_side = "SELL" if order_type == 1 else "BUY"

    if BINANCE_order:
    
        logging.info(f"{Binance_symbol}, BINANCE market {Binance_side} order sent!, Amount(coin): {BinTR_filled_amount}")
        logging.info(f"{Binance_symbol}, BINANCE market {Binance_side} execution time: {duration:.2f} seconds, Amount(coin): {BinTR_filled_amount}")

        BINANCE_info = BINANCE_order['info']

        if BINANCE_info['status'] == 'FILLED':


            # Calculate average execution price and fees for Binance
            binance_filled_amount = float(BINANCE_info['executedQty'])
            binance_filled_price_usdt = float(BINANCE_info['avgPrice'])

            binance_filled_amount_usdt = binance_filled_amount * binance_filled_price_usdt
            logging.info(f"{Binance_symbol}, BINANCE Filled Coin: {binance_filled_amount}, Filled Price: {binance_filled_price_usdt}")

            PNL = 0
            Gross_PNL = 0
            executed_margin = 0

            if Binance_symbol in binance_exceptional_symbols:

                BinTR_filled_amount = BinTR_filled_amount * 0.001
                BinTR_filled_price = BinTR_filled_price * 1000

            # Use USDTTRY from shared memory if available (C++ client updates it), otherwise use global variables
            if USE_CPP_OPTIMIZATION_BINANCE and get_binance_usdttry_rate:
                try:
                    usdttry_rate_from_shm = get_binance_usdttry_rate()
                    # Use the same rate for both bid and ask (C++ client provides average rate)
                    binance_filled_price_TRY = binance_filled_price_usdt * usdttry_rate_from_shm
                except Exception as e:
                    logging.warning(f"Failed to get USDTTRY from shared memory, using global: {e}")
                    binance_filled_price_TRY = binance_filled_price_usdt * USDTTRY_bid if order_type == 1 else binance_filled_price_usdt * USDTTRY_ask
            else:
                binance_filled_price_TRY = binance_filled_price_usdt * USDTTRY_bid if order_type == 1 else binance_filled_price_usdt * USDTTRY_ask
        
            if order_type == 1:  # Open trade :  BINANCE SELL - BinTR BUY
                Gross_PNL = (binance_filled_price_TRY - BinTR_filled_price) * binance_filled_amount
                executed_margin = (binance_filled_price_TRY - BinTR_filled_price) / BinTR_filled_price
            else:  # Close trade :  BINANCE BUY - BinTR SELL
                Gross_PNL = (BinTR_filled_price - binance_filled_price_TRY) * binance_filled_amount
                executed_margin = (BinTR_filled_price - binance_filled_price_TRY) / binance_filled_price_TRY

            binance_filled_amount_TRY = binance_filled_amount * binance_filled_price_TRY

            BinTR_fee_TRY = BinTR_filled_amount_TRY * BinTR_fee_rate
            BINANCE_fee_TRY = binance_filled_amount_TRY * BINANCE_fee_rate
            total_fee_TRY = BinTR_fee_TRY + BINANCE_fee_TRY
            PNL = round(Gross_PNL - total_fee_TRY, 0)

            logging.info(f"{Binance_symbol}, Gross PNL: {Gross_PNL}, PNL: {PNL}, Executed margin: {executed_margin}")

            BinanceTimeDiff = int(time.time() * 1000) - int(BINANCE_info['updateTime'])
            # Log Binance execution with PNL        
            binance_trade = {
                'TradeID': tradeID,
                'OrderID': BINANCE_info['orderId'],
                'OrderTime': datetime.fromtimestamp(int(BINANCE_info['updateTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # Include milliseconds
                'Symbol': Binance_symbol,
                'Side': Binance_side,
                'Amount': binance_filled_amount,
                'Price': binance_filled_price_usdt,
                'USDTTRY_bid': USDTTRY_bid,
                'USDTTRY_ask': USDTTRY_ask,
                'Price_TRY': binance_filled_price_TRY,
                'Amount_TRY': binance_filled_amount_TRY,
                'Amount_USDT': binance_filled_amount_usdt,
                'Fee': total_fee_TRY,
                'ExecutedMargin': executed_margin,
                'PNL': PNL,
                'BinanceTimeDiff': BinanceTimeDiff
            }
            
            update_binance_position(idx, binance_filled_amount, binance_filled_amount_usdt, order_type)

            return binance_trade
    else:
        logging.error(f"{Binance_symbol}, BINANCE market {Binance_side} order failed after 3 retries!")
        time.sleep(1)
        return None

async def delete_open_order(idx, OrderId, order_type):
    global open_orders, prev_buy_order_prices, prev_sell_order_prices
    try:
       
        BinTR_symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
              # Safely remove order from open_orders - won't raise KeyError if not found
        removed_order = open_orders.pop(OrderId, None)
        if removed_order is None:
            logging.warning(f"Order {OrderId} not found in open_orders (may have been already removed)")

        try:
            redis = await get_redis_connection()
            # Get all orders from redis
            orders = await redis.lrange('maker_open_orders', 0, -1)

            # Find and remove the matching order
            for order in orders:
                order_data = ujson.loads(order)
                if order_data.get('OrderId') == OrderId:
                    await redis.lrem('maker_open_orders', 1, order)
#                    logging.info(f"Order {OrderId} deleted from redis open trades")
                    break

        except Exception as e:
            logging.error(f"Error deleting order {OrderId} from redis: {e}")    

        if order_type == 1:
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID] = 0
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Time] = 0
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Price] = 0
            arbitrage_table_np[idx, arbit_config.col_Buy_Order_Amount] = 0
            prev_buy_order_prices[BinTR_symbol] = 0
            
        elif order_type == 3:
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID] = 0
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Time] = 0
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Price] = 0
            arbitrage_table_np[idx, arbit_config.col_Sell_Order_Amount] = 0
            prev_sell_order_prices[BinTR_symbol] = 0
           
        release_order_lock(idx, order_type, "delete_open_order")
        
    except Exception as e:
        logging.error(f"Unexpected error in delete_open_order for order {OrderId}: {e}")

def update_binance_position(idx, filled_amount, filled_amount_usdt, order_type):
    # Ensure we're working with numeric values
    filled_amount = float(filled_amount)
    filled_amount_usdt = float(filled_amount_usdt)
    
    # Convert current position values to float to ensure numeric operations
    current_coin_position = float(arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_coin])
    current_usdt_position = float(arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_usdt])
    current_free_usdt = float(arbitrage_table_np[idx, arbit_config.col_Binance_free_usdt])
    
    if order_type == 1: #BINANCE SELL for open
        new_coin_position = current_coin_position - filled_amount
        new_usdt_position = current_usdt_position - filled_amount_usdt
        
        arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_coin] = new_coin_position
        arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_usdt] = new_usdt_position
        
    else:   #BINANCE BUY for close
        new_coin_position = current_coin_position + filled_amount
        new_usdt_position = current_usdt_position + filled_amount_usdt
        
        arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_coin] = new_coin_position
        arbitrage_table_np[idx, arbit_config.col_BinancePositionAmount_usdt] = new_usdt_position
    
    #For both cases, we need to subtract the filled amount from the free balance
    new_free_usdt = current_free_usdt - filled_amount_usdt
    arbitrage_table_np[idx, arbit_config.col_Binance_free_usdt] = new_free_usdt
    return

async def update_balances_loop():
    
    while True:

        balance_update_check_interval = 60
        try:
            redis_pool = await get_redis_connection()
            balance_update_check_interval = int(await redis_pool.get('maker_BalanceUpdateCheckInterval'))
            logging.info(f"Periodic balance check (interval: {balance_update_check_interval} seconds)...")
        except Exception as e:   
            await redis_pool.set('maker_BalanceUpdateCheckInterval', int(balance_update_check_interval))
            logging.info(f"Error reading balance update check interval, setting to 60 seconds by default")

        try:
            await update_binance_balances()
        except Exception as e:
            logging.error(f"Error updating balances: {e}")
                
        await asyncio.sleep(balance_update_check_interval)

open_orders = {}
fetched_open_orders = {}

async def cancel_all_BinTR_open_orders():
    """Cancel all BinTR open orders - tries WebSocket 504 method first, falls back to individual cancellations"""
    global open_orders
    try:
        logging.info(f"Cancelling all open orders...")
        
        # Try the new WebSocket 504 method first (more efficient)
        try:
            success = await BinTR_order_ws.cancel_all_orders()
            if success:
                logging.info(f"Cancel all orders request sent via WebSocket 504")
            else:
                logging.warning(f"WebSocket 504 method failed, falling back to individual cancellations")
                raise Exception("WebSocket 504 method failed")
        except Exception as e:
            logging.warning(f"WebSocket 504 method failed: {e}, falling back to individual cancellations")
            
            # Fallback to individual order cancellations
            try:
                for order_id in open_orders.keys():
                    try:
                        await BinTR_order_ws.cancel_order(order_id)
                        logging.info(f"Open order {order_id} cancelled")
                    except Exception as e:
                        logging.error(f"Error canceling open order {order_id}: {e}")
            except Exception as e:
                logging.error(f"Error canceling open orders: {e}")

        await asyncio.sleep(1)
        # Reset arbitrage table and clear Redis data
        await reset_arbitrage_table_and_redis()

    except Exception as e:
        logging.error(f"Error clearing open orders: {e}")

async def reset_arbitrage_table_and_redis():
    """Reset arbitrage table order IDs and clear Redis data"""
    global arbitrage_table_np, open_orders
    
    try:
        logging.info("Resetting arbitrage table order IDs and clearing Redis data...")
        
        # Reset arbitrage table order IDs to 0
        arbitrage_table_np[:, arbit_config.col_Buy_Order_ID] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_ID] = 0
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Lock] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Lock] = 0
        
        # Clear order times and prices
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Time] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Time] = 0
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Amount] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Amount] = 0
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Price] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Price] = 0
        arbitrage_table_np[:, arbit_config.col_Buy_Order_Best_Price] = 0
        arbitrage_table_np[:, arbit_config.col_Sell_Order_Best_Price] = 0
        
        # Clear Redis data
        try:
            redis = await get_redis_connection()
            
            # Clear maker_open_orders list
            await redis.delete('maker_open_orders')
            logging.info("Cleared maker_open_orders from Redis")
                
        except Exception as e:
            logging.error(f"Error clearing Redis data: {e}")
            
        logging.info("Arbitrage table and Redis data reset completed")

        # Clear local open_orders dictionary
        open_orders.clear()
        
    except Exception as e:
        logging.error(f"Error resetting arbitrage table and Redis: {e}")

MAX_BINANCE_TIME_DIFF = 200

precision_multipliers = None
BinTR_price_precision_step = None

async def initialize():
    global current_state, USDTTRY_bid, USDTTRY_ask, MAX_BINANCE_TIME_DIFF
    global BinTR_private_ws_bot
    global BinTR_order_ws, cs_fill_ws_task
    global precision_multipliers, BinTR_price_precision_step
    global USE_CPP_OPTIMIZATION_BINANCE
    global _symbol_to_instrument_id_cache

    logging.info("Initializing...")

    try:
        redis = await get_redis_connection()
        
        try:
            await update_thresholds()
            logging.info("Thresholds initialized & updated successfully!")
        except Exception as e:
            logging.error(f"Error initializing & updating thresholds: {e}")
            logging.error("Failed to initialize & update thresholds. System will not start.")
            return False

        logging.info("Orders disabled at startup!")
       

        try:
            logging.info("Arbitrage system initializing...")

            logging.info("Loading markets...")
            start_time = time.time()
            BINANCE.load_markets()
            BinTR.load_markets()
            duration = (time.time() - start_time) * 1000
            logging.info(f"Markets loaded successfully! Duration: {duration:.2f} milliseconds")

            # Initialize symbol to instrument ID cache from common_symbol_info.json
            logging.info("Loading symbol to instrument ID cache...")
            try:
                import os
                common_symbol_info_path = os.path.join(os.path.dirname(__file__), "common_symbol_info.json")
                if os.path.exists(common_symbol_info_path):
                    with open(common_symbol_info_path, 'r') as f:
                        common_symbol_info = json.load(f)
                        symbols_list = common_symbol_info.get('symbols', [])
                        for symbol_info in symbols_list:
                            binance_symbol = symbol_info.get('binance_futures_symbol', '')
                            instrument_id = symbol_info.get('CS_instrument_id')
                            if binance_symbol and instrument_id is not None:
                                _symbol_to_instrument_id_cache[binance_symbol.upper()] = instrument_id
                        logging.info(f"Loaded {len(_symbol_to_instrument_id_cache)} symbol-to-instrument mappings")
                else:
                    logging.warning(f"common_symbol_info.json not found at {common_symbol_info_path}, symbol-to-instrument cache will be empty")
            except Exception as e:
                logging.error(f"Error loading symbol to instrument ID cache: {e}")
                logging.warning("Symbol-to-instrument cache initialization failed, CS orders may not work correctly")

            logging.info("Calculating amount precision for script symbols...")
            precision_values, BinTR_precision = await arbit_config.calculate_amount_precision_for_script(BINANCE, BinTR, script_symbols)
            arbitrage_table_np[:, arbit_config.col_AmountPrecision] = precision_values
            arbitrage_table_np[:, arbit_config.col_BinTR_AmountPrecision] = BinTR_precision
            logging.info("Amount precision calculation completed!")

            logging.info("Fetching BinTR price precision for script symbols...")
            BinTR_price_precisions =  await arbit_config.fetch_BinTR_price_precisions_for_script(BinTR, script_symbols)
            arbitrage_table_np[:, arbit_config.col_BinTR_price_precision] = BinTR_price_precisions
            arbitrage_table_np[:, arbit_config.col_BinTR_price_step] = 1/np.power(10, BinTR_price_precisions)
            logging.info("BinTR price precision fetched successfully!")
            logging.info(f"BinTR price precisions: {BinTR_price_precisions}")
            logging.info(f"BinTR price steps: {1/np.power(10, BinTR_price_precisions)}")

             # Get fresh price precision values and convert to integers for rounding
            # Use _safe_float_array to handle NaN values before converting to int
            try:
                BinTR_price_precision_safe = _safe_float_array(arbitrage_table_np[:, arbit_config.col_BinTR_price_precision])
                BinTR_price_precision_int = BinTR_price_precision_safe.astype(int)
                # Apply vectorized per-element rounding using scale-based approach
                precision_multipliers = np.power(10.0, BinTR_price_precision_int)
                logging.info(f"precision_multipliers initialized successfully, shape: {precision_multipliers.shape}")
            except Exception as e:
                logging.error(f"Error initializing precision_multipliers: {e}")
                # Fallback: use default precision of 8 (0.00000001 step)
                precision_multipliers = np.full(len(script_symbols), 1e8, dtype=np.float64)
                logging.warning("Using default precision_multipliers (1e8) as fallback")
                    
            # Price diff shall be calculated with the next bid and ask prices
            BinTR_price_precision_step = _safe_float_array(arbitrage_table_np[:, arbit_config.col_BinTR_price_step])


            logging.info("Starting all required tasks...")

            # Log symbol distribution for this script
            logging.info(f"=== Script {SCRIPT_ID} Symbol Distribution ===")
            logging.info(f"  Total symbols in system: {len(combined_symbol_list)}")
            logging.info(f"  Symbols handled by Script {SCRIPT_ID}: {len(script_symbols)}")
            logging.info(f"  Symbol range: {start_idx} to {end_idx-1}")
            logging.info(f"  Script symbols: {script_symbols}")
            logging.info(f"  Symbol group size from config: {len(arbit_config.binance_symbol_groups[SCRIPT_ID - 1])}")
            
            # Show the complete symbol distribution across all scripts
            logging.info("=== Complete Symbol Distribution Across All Scripts ===")
            for i, group in enumerate(arbit_config.binance_symbol_groups, 1):
                base_symbols = [symbol.replace('USDT', '') for symbol in group]
                base_symbols = [symbol.replace('LUNA2', 'LUNA').replace('BEAMX', 'BEAM') for symbol in base_symbols]
                base_symbols = [symbol.replace('1000PEPE', 'PEPE').replace('1000BONK', 'BONK').replace('1000SHIB', 'SHIB').replace('1000FLOKI', 'FLOKI') for symbol in base_symbols]
                status = " (CURRENT SCRIPT)" if i == SCRIPT_ID else ""
                logging.info(f"  Script {i}{status}: {len(base_symbols)} symbols - {base_symbols}")
            logging.info("==================================================")

            # Only start USDTTRY update task if not using C++ optimization (C++ client handles it)
            if not USE_CPP_OPTIMIZATION_BINANCE:
                update_currency_task = asyncio.create_task(update_usdt_try_price())
                logging.info(f"Currency update task created, USDT/TRY will be updated every {currency_update_interval} seconds.")
            else:
                logging.info("USDTTRY rate updates handled by C++ client, skipping Python update task.")
            
            # Start all required tasks
            redis_task = asyncio.create_task(update_redis_arbitrage_table(script_id=SCRIPT_ID))
            logging.info(f"Redis task created for Script {SCRIPT_ID}")

            # C++ clients handle all CS host connections, health checks, etc.
            if USE_CPP_OPTIMIZATION_BINANCE and collocation:
                logging.info(f"Script {SCRIPT_ID}: C++ WebSocket optimization enabled - Python will only read from shared memory")
                logging.info(f"Script {SCRIPT_ID}: C++ clients handle all CS host connections and health checks")
                # Start the shared memory reader directly (C++ clients handle all CS connections)
                try:
                    from python_binance_cs_shared_memory_reader import run_binance_shared_memory_reader
                    
                    # Set connection flag callback
                    def set_connected(value):
                        global CS_WS_CONNECTED, BINANCE_ws_connected
                        CS_WS_CONNECTED = value
                        BINANCE_ws_connected = value
                    
                    # Create global-to-local index mapping for this script
                    global_to_local_index = {}
                    for base_symbol, local_idx in symbol_index_map.items():
                        global_idx = global_symbol_to_index.get(base_symbol)
                        if global_idx is not None:
                            global_to_local_index[global_idx] = local_idx
                    
                    # Get all available hosts from instruments file (C++ clients connect to all hosts)
                    hosts_to_read = None
                    try:
                        import json
                        import os
                        instruments_file = os.path.join(os.path.dirname(__file__), "binance_websocket_instruments.json")
                        if os.path.exists(instruments_file):
                            with open(instruments_file, 'r') as f:
                                instruments_data = json.load(f)
                                hosts_to_read = instruments_data.get("hosts", [])
                                if hosts_to_read:
                                    logging.info(f"Script {SCRIPT_ID}: Will read from {len(hosts_to_read)} Binance C++ client(s): {hosts_to_read}")
                    except Exception as e:
                        logging.warning(f"Script {SCRIPT_ID}: Could not read hosts from instruments file: {e}")
                    
                    # Start shared memory reader task
                    # Note: Prices are already in TRY (converted by C++ client), Python just reads them
                    binance_sm_task = asyncio.create_task(run_binance_shared_memory_reader(
                        arbitrage_table_np,
                        symbol_index_map,
                        arbit_config.col_Binance_Time,
                        arbit_config.col_Binance_AskP1,
                        arbit_config.col_Binance_BidP1,
                        update_interval=0.001,  # 1ms polling interval
                        set_connected_flag=set_connected,
                        global_to_local_index=global_to_local_index,
                        hosts=hosts_to_read,  # Read from multiple hosts (one C++ client per host)
                        col_time_diff=arbit_config.col_BinanceTimeDiff  # Read time diff from shared memory
                    ))
                    logging.info(f"Script {SCRIPT_ID}: Started Binance shared memory reader task (C++ clients handle CS connections)")
                except Exception as e:
                    logging.error(f"Script {SCRIPT_ID}: Failed to start Binance shared memory reader: {e}")
                    import traceback
                    logging.error(traceback.format_exc())
                    logging.warning("Falling back to regular Binance WebSocket connection")
                    USE_CPP_OPTIMIZATION_BINANCE = False
           
            if collocation:
                logging.info(f"Script {SCRIPT_ID}: Starting CS fill message listener (collocation enabled)")
                global cs_fill_ws_task
                cs_fill_ws_task = asyncio.create_task(cs_fill_message_listener())
                logging.info(f"Script {SCRIPT_ID}: CS fill message listener task started.")

            await asyncio.sleep(0.5)  # Small delay before canceling orders
            await cancel_all_BinTR_open_orders()

            arbitrage_task = asyncio.create_task(calculate_arbitrage_BinanceBinTR_loop())
            logging.info("Arbitrage task created")

            check_and_process_actions_task = asyncio.create_task(check_and_process_actions_loop())
            logging.info("Check and process actions task created")

            update_balances_task = asyncio.create_task(update_balances_loop())
            logging.info("Update balances task created")

            update_BinTR_balances_task = asyncio.create_task(update_BinTR_balances_loop())
            logging.info("Update BinTR balances task created")


            logging.info("Fetching MAX_BINANCE_TIME_DIFF from Redis...")
            try:
                # Use asyncio.wait_for to add timeout to Redis call
                MAX_BINANCE_TIME_DIFF = await asyncio.wait_for(redis.get('maker_MaxBinanceTimeDiff'), timeout=5.0)
                logging.info(f"MAX_BINANCE_TIME_DIFF retrieved: {MAX_BINANCE_TIME_DIFF}")
                if MAX_BINANCE_TIME_DIFF:
                    MAX_BINANCE_TIME_DIFF = int(MAX_BINANCE_TIME_DIFF)
                else:
                    MAX_BINANCE_TIME_DIFF = 500
                    await asyncio.wait_for(redis.set('maker_MaxBinanceTimeDiff', int(MAX_BINANCE_TIME_DIFF)), timeout=5.0)
                    logging.info(f"Max Binance Time Diff set to {MAX_BINANCE_TIME_DIFF} at redis")
            except asyncio.TimeoutError:
                logging.error("Timeout getting MAX_BINANCE_TIME_DIFF from Redis (5s timeout), using default value 500")
                MAX_BINANCE_TIME_DIFF = 500
            except Exception as e:
                logging.error(f"Error getting MAX_BINANCE_TIME_DIFF from Redis: {e}")
                MAX_BINANCE_TIME_DIFF = 500
                try:
                    await asyncio.wait_for(redis.set('maker_MaxBinanceTimeDiff', int(MAX_BINANCE_TIME_DIFF)), timeout=5.0)
                    logging.info(f"Max Binance Time Diff set to {MAX_BINANCE_TIME_DIFF} at redis")
                except Exception as set_error:
                    logging.error(f"Error setting MAX_BINANCE_TIME_DIFF in Redis: {set_error}")
            logging.info(f"Max Binance Time Diff: {MAX_BINANCE_TIME_DIFF}")
            
            logging.info("All tasks started successfully!")

            # Set running state only after successful initialization
            current_state = ArbitrageState.RUNNING
            await redis.set('maker_arbitrage_state', b'running')
            logging.info("Arbitrage system initialized and now running!")
            logging.info("Waiting for orders to be enabled...")


        except Exception as e:
            logging.error(f"Error during initialization: {e}")
            current_state = ArbitrageState.STOPPED
            await redis.set('maker_arbitrage_state', b'stopped')
            return False

    except Exception as e:
        logging.error(f"Error in Redis operations: {e}")
        return False

    return True

async def update_usdt_try_price():
    global USDTTRY_bid, USDTTRY_ask, USDTTRY
    url = "https://api.binance.com/api/v3/ticker/bookTicker"
    params = {
        'symbol': 'USDTTRY'
    }
    while True:
        try:
            response = requests.get(url, params=params)
            data = response.json()
            USDTTRY_bid = float(data['bidPrice'])
            USDTTRY_ask = float(data['askPrice'])
            logging.info(f"USDT/TRY Best Bid: {USDTTRY_bid}, Best Ask: {USDTTRY_ask}")
            arbitrage_table_np[:, arbit_config.col_USDTTRY_bid] = USDTTRY_bid
            arbitrage_table_np[:, arbit_config.col_USDTTRY_ask] = USDTTRY_ask
            USDTTRY = (USDTTRY_ask + USDTTRY_bid) / 2
        except Exception as e:
            logging.error(f"Error updating USDT/TRY price: {e}")
        await asyncio.sleep(currency_update_interval)

async def check_binance_time_diffs():
    
    try:
        binance_time_diff = _safe_float_array(arbitrage_table_np[:, arbit_config.col_BinanceTimeDiff])
        binance_time_diff_high = binance_time_diff > MAX_BINANCE_TIME_DIFF
            
        if np.any(binance_time_diff_high):
            high_delay_count = np.sum(binance_time_diff_high)
            max_delay = np.max(binance_time_diff)
            logging.warning(f"⚠️ HIGH BINANCE DELAY DETECTED: {high_delay_count} symbols with delay > {MAX_BINANCE_TIME_DIFF}ms (max: {max_delay:.1f}ms)")
            logging.warning(f"⚠️ CANCELLING EXISTING ORDERS and PREVENTING NEW ORDERS due to high delay")
            
    except Exception as e:
        logging.error(f"ERROR in check_binance_time_diffs: {e}")
        import traceback
        logging.error(f"TRACEBACK: {traceback.format_exc()}")


async def update_redis_arbitrage_table(key='maker_arbitrage_table', script_id=1):
    """
    Update Redis with arbitrage table data in a distributed manner.
    Each script updates only its allocated portion of the full table.
    
    Args:
        key: Redis key for the arbitrage table
        script_id: ID of the current script (1, 2, 3, or 4) to determine which symbols to handle
    """
    
    redis = await get_redis_connection()
    
    # Calculate symbol ranges for this script based on symbol groups
    total_symbols = len(combined_symbol_list)
    start_idx = 0
    for i in range(script_id - 1):
        start_idx += len(arbit_config.binance_symbol_groups[i])
    end_idx = start_idx + len(arbit_config.binance_symbol_groups[script_id - 1])
    
    logging.info(f"Script {script_id}: Handling symbols {start_idx} to {end_idx-1} (total symbols: {total_symbols})")
    
    while True:
        try:
            # await check_binance_time_diffs()

            # Get the current full table from Redis or create a new one
            existing_data = await redis.get(key)
            if existing_data:
                # Load existing full table
                full_df = pickle.loads(existing_data)
            else:
                # Create new full table if none exists
                full_df = pd.DataFrame(np.full((total_symbols, len(arbit_config.columns)), np.nan, dtype=object))
                # Set the symbol column for all rows from combined_symbol_list
                full_df.iloc[:, arbit_config.col_Base_Symbol] = combined_symbol_list
            
            # Update only this script's portion of the full table
            script_data = arbitrage_table_np  # This is already just this script's portion
            
            # Ensure dimensions match
            expected_rows = end_idx - start_idx
            actual_rows = script_data.shape[0]
            
            if expected_rows != actual_rows:
                logging.error(f"Script {script_id}: Dimension mismatch: expected {expected_rows} rows, got {actual_rows} rows")
                await asyncio.sleep(update_redis_arbitrage_table_interval)
                continue
            
            # Update the full table with this script's data
            full_df.iloc[start_idx:end_idx] = script_data
            
            # Save the complete updated table back to Redis
            pickled_data = pickle.dumps(full_df)
            await redis.set(key, pickled_data)
            
            logging.debug(f"Script {script_id}: Updated symbols {start_idx}-{end_idx-1} in full table")
            await asyncio.sleep(update_redis_arbitrage_table_interval)
            
        except asyncio.CancelledError:
            # Task was cancelled during shutdown - this is expected
            break
        except Exception as e:
            # Check if this is a connection error during shutdown (expected behavior)
            error_str = str(e).lower()
            is_connection_error = any(phrase in error_str for phrase in [
                'connection closed', 'connection reset', 'connection refused',
                'connection aborted', 'broken pipe'
            ])
            
            # If it's a connection error and we're shutting down, log as warning instead of error
            if is_connection_error and current_state == ArbitrageState.STOPPED:
                logging.debug(f"Redis connection closed during shutdown (Script {script_id}) - this is expected")
            else:
                logging.error(f"Error updating Redis (Script {script_id}): {e}")
            
            # Only sleep if not cancelled
            try:
                await asyncio.sleep(update_redis_arbitrage_table_interval)
            except asyncio.CancelledError:
                break


async def update_thresholds():

    redis = await get_redis_connection()
    
    # Get thresholds from Redis and parse JSON
    try:
        thresholds_json = await redis.get('maker_arbitrage_thresholds')
    except Exception as e:
        logging.error(f"Error getting thresholds from Redis: {e}")
        return False

    if thresholds_json:
        thresholds_list = ujson.loads(thresholds_json)
        # Convert list of dictionaries to DataFrame
        thresholds_df = pd.DataFrame(thresholds_list)
        #logging.info(f"Thresholds DataFrame:\n{thresholds_df}")

        # Check if 'Symbol' column exists
        if 'Symbol' not in thresholds_df.columns:
            logging.error("The 'Symbol' column is missing from the thresholds DataFrame.")
            return False

        # Ensure the DataFrame is sorted to match the arbitrage table order for this script's symbols
        thresholds_df = thresholds_df.set_index('Symbol').reindex(index=script_symbols).reset_index()
        #logging.info(f"Thresholds DataFrame:\n{thresholds_df}")

        # Track previous maker_type values to detect changes
        previous_maker_types = arbitrage_table_np[:, arbit_config.col_Maker_Type].copy()
        
        # Update the numpy array with the threshold values (only for this script's symbols)
        new_maker_types = thresholds_df['Maker_Type'].values
        arbitrage_table_np[:, arbit_config.col_Maker_Type] = new_maker_types
        
        # Cancel orders for symbols where maker_type changed from non-zero to 0 (or None)
        # Only cancel if BinTR_order_ws is initialized
        if BinTR_order_ws is not None:
            for idx, (prev_type, new_type) in enumerate(zip(previous_maker_types, new_maker_types)):
                # Check if maker_type changed from non-zero to 0 (or None)
                if (prev_type != 0 and prev_type is not None) and (new_type == 0 or new_type is None):
                    try:
                        base_symbol = arbitrage_table_np[idx, arbit_config.col_Base_Symbol]
                        BinTR_symbol = arbitrage_table_np[idx, arbit_config.col_BinTR_Symbol]
                        
                        # Get buy and sell order IDs
                        buy_order_id = int(arbitrage_table_np[idx, arbit_config.col_Buy_Order_ID])
                        sell_order_id = int(arbitrage_table_np[idx, arbit_config.col_Sell_Order_ID])
                        
                        # Cancel individual orders if they exist
                        if buy_order_id > 0:
                            await BinTR_order_ws.cancel_order(buy_order_id)
                            logging.info(f"Cancelling buy order {buy_order_id} for {base_symbol} (maker_type set to 0)")
                        
                        if sell_order_id > 0:
                            await BinTR_order_ws.cancel_order(sell_order_id)
                            logging.info(f"Cancelling sell order {sell_order_id} for {base_symbol} (maker_type set to 0)")
                        
                        # Also cancel all orders for this pair symbol (in case there are any other orders)
                        await BinTR_order_ws.cancel_all_orders(pair_symbol=BinTR_symbol)
                        logging.info(f"Cancelled all open orders for {base_symbol} (BinTR symbol: {BinTR_symbol}) - maker_type set to 0")
                    except Exception as e:
                        logging.error(f"Error cancelling orders for symbol at idx {idx}: {e}")

        arbitrage_table_np[:, arbit_config.col_OpenTriggerMargin] = thresholds_df['OpenTriggerMargin'].values
        arbitrage_table_np[:, arbit_config.col_CloseTriggerMargin] = thresholds_df['CloseTriggerMargin'].values
        arbitrage_table_np[:, arbit_config.col_OpenMarginWindow] = thresholds_df['OpenMarginWindow'].values
        arbitrage_table_np[:, arbit_config.col_CloseMarginWindow] = thresholds_df['CloseMarginWindow'].values
        arbitrage_table_np[:, arbit_config.col_OpenAggression] = thresholds_df['OpenAggression'].values
        arbitrage_table_np[:, arbit_config.col_CloseAggression] = thresholds_df['CloseAggression'].values
        arbitrage_table_np[:, arbit_config.col_OpenStopMargin] = thresholds_df['OpenStopMargin'].values
        arbitrage_table_np[:, arbit_config.col_CloseStopMargin] = thresholds_df['CloseStopMargin'].values
        arbitrage_table_np[:, arbit_config.col_MinBuyOrderAmount_TRY] = thresholds_df['MinBuyOrderAmount_TRY'].values
        arbitrage_table_np[:, arbit_config.col_MinSellOrderAmount_TRY] = thresholds_df['MinSellOrderAmount_TRY'].values
        arbitrage_table_np[:, arbit_config.col_MaxPositionAmount_TRY] = thresholds_df['MaxPositionAmount_TRY'].values  
        arbitrage_table_np[:, arbit_config.col_MoveThreshold] = thresholds_df['MoveThreshold'].values
        return True
    else:
        logging.error("No thresholds found in Redis.")
        return False

async def shutdown():
    logging.info("Arbitrage system shutting down...")

    global current_state
    current_state = ArbitrageState.STOPPED
    try:
        r = await get_redis_connection()
    except Exception as e:
        logging.error(f"Redis connection error: {e}")
        sys.exit()

    # Collect all tasks
    tasks = [
        update_currency_task,
        BinTR_task,
        binance_task,
        arbitrage_task,
        update_balances_task, # Add
        redis_task
    ]

    # Cancel tasks
    try:
        active_tasks = [t for t in tasks if t and not t.cancelled()]
        if active_tasks:
            logging.info(f"Cancelling {len(active_tasks)} tasks...")
            for task in active_tasks:
                task.cancel()
            await asyncio.gather(*active_tasks, return_exceptions=True)
            logging.info("All tasks cancelled")
    except Exception as e:
        logging.error(f"Task cancellation error: {e}")

    # Close connections
    try:
        if redis_pool:
            await redis_pool.aclose()
    except Exception as e:
        logging.error(f"Connection closure error: {e}")
        sys.exit()
   
    # Wait 3 seconds to ensure all connections are closed
    await asyncio.sleep(3)

    # Update Redis state
    try:
        await r.set('maker_arbitrage_state', b'stopped')
        logging.info("Arbitrage state updated to stopped.")
    except Exception as e:
        logging.error(f"Redis state update failed: {e}")
        sys.exit()

    logging.info("Shutdown complete")
    sys.exit()

async def listen_for_commands():
    """Listen for commands from Redis pub/sub"""
    global current_state
    try:
        redis_pool = await get_redis_connection()
        pubsub = redis_pool.pubsub()
        await pubsub.subscribe('arbit_commands')
        
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    command = message['data']
                    logging.info(f"Received command: {command}")
                    
                    if command == b'stop':
                        logging.info("Stop command received, initiating graceful shutdown...")
                    
                        await disable_orders("Stop command")
                        # Set state to stopping
                        current_state = ArbitrageState.STOPPED
                        await shutdown()
                    elif command == b'update_balances':
                        await update_binance_balances()
                    elif command == b'enable_orders':
                        await enable_orders_function()
                    elif command == b'disable_orders':
                        await disable_orders('Disable orders command')
                    elif command == b'cancel_all_orders':
                        await cancel_all_BinTR_open_orders()
                        logging.info("Cancel all orders command executed!")
                    elif command == b'update_thresholds':
                        await update_thresholds()
                        logging.info("Thresholds updated!")
                    elif command.startswith(b"place_manual_order"):
                        await BinTR_order_ws.place_manual_order(command.decode('utf-8'))
                    elif command.startswith(b"cancel_order"):
                        order_id = command.decode('utf-8').split('/')[1]
                        await BinTR_order_ws.cancel_order(order_id)
                
                await asyncio.sleep(0.06)
                 # Re-raise to be caught by main
            except Exception as e:
                logging.error(f"Error processing command: {e}")
                await asyncio.sleep(1)
                 # Re-raise to be caught by main
    except Exception as e:
        logging.error(f"Error in command listener: {e}")

def set_cpu_affinity():
    """Set CPU affinity for this script based on SCRIPT_ID
    
    Benefits of CPU affinity:
    - Reduces CPU contention between scripts
    - Better cache locality (L1/L2 cache stays warm)
    - Better performance isolation (one script's load doesn't affect others)
    - More predictable latency for HFT operations
    """
    if psutil is None:
        logging.warning("psutil not available, CPU affinity not set. Install with: pip install psutil")
        return
    
    try:
        # Get the CPU core for this script
        cpu_core = CPU_CORES.get(SCRIPT_ID, 2)  # Default to core 2 if not found
        
        # Get current process
        current_process = psutil.Process()
        
        # Check if the requested core exists
        available_cores = list(range(psutil.cpu_count()))
        if cpu_core not in available_cores:
            logging.warning(f"Requested CPU core {cpu_core} not available. Available cores: {available_cores}")
            # Fall back to core 2 (first available after C++ clients)
            cpu_core = 2 if 2 in available_cores else available_cores[0] if available_cores else 0
        
        # Set CPU affinity
        current_process.cpu_affinity([cpu_core])
        
        logging.info(f"Script {SCRIPT_ID} process (PID: {os.getpid()}) pinned to CPU Core {cpu_core}")
        logging.info(f"  Note: Cores 0-1 reserved for C++ WebSocket clients (BinTR=0, Binance=1)")

        # Verify the affinity was set correctly
        p = psutil.Process(os.getpid())
        actual_core = p.cpu_num()
        if actual_core == cpu_core:
            logging.info(f"✓ CPU affinity verified: running on Core {actual_core}")
        else:
            logging.warning(f"CPU affinity mismatch: requested Core {cpu_core}, actual Core {actual_core}")
        
    except Exception as e:
        logging.error(f"Error setting CPU affinity: {e}")

async def main():
    try:
        # Set CPU affinity for this script
        set_cpu_affinity()
        
        try:
            # Clear Redis data before initialization
            await initialize()
        except Exception as e:
            logging.error(f"Error in initialize phase: {e}")
            redis = await get_redis_connection()
            await redis.set('maker_arbitrage_state', b'stopped')
            logging.error("Arbitrage system will not start.")
            return

        # Start command listener
        global command_listener_task
        logging.info("Starting command listener...")
        command_listener_task = asyncio.create_task(listen_for_commands())
        await command_listener_task

    except Exception as e:
        logging.error(f"Error in main: {e}")
        sys.exit()

if __name__ == '__main__':
    asyncio.run(main())