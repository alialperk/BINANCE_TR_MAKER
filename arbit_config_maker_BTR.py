import ccxt.async_support as ccxt
import APIkeys
import logging
from tqdm import tqdm
import numpy as np
import pandas as pd
import redis 
import json
import math
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.poolmanager import PoolManager
from cachetools import TTLCache  # For caching
import sys
from functools import lru_cache
import uuid
import time
import base64
import hashlib
import hmac
from typing import Dict
import requests
import ujson
import datetime
import os
import subprocess
from pathlib import Path
import urllib3
from urllib.parse import urlencode

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
np.seterr(invalid='ignore')

# Platform-specific console launch commands
if sys.platform == 'win32':
    CONSOLE_LAUNCH_CMD = 'start'
else:  # Linux/Unix
    CONSOLE_LAUNCH_CMD = 'gnome-terminal --'  # For GNOME
    # Alternative options:
    # CONSOLE_LAUNCH_CMD = 'xterm -e'  # For Xterm
    # CONSOLE_LAUNCH_CMD = 'konsole -e'  # For KDE

# Initialize Redis connection
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.ping()  # Test connection
except Exception as e:
    logging.warning(f"Could not connect to Redis: {e}. Redis operations will be disabled.")
    r = None

COMMON_SYMBOL_INFO_FILE = Path("common_symbol_info.json")

# Load common symbol info from JSON
EXCHANGE_symbol_list = []
binance_symbol_list = []

if COMMON_SYMBOL_INFO_FILE.exists():
    try:
        with open(COMMON_SYMBOL_INFO_FILE, "r", encoding="utf-8") as f:
            common_symbol_data = json.load(f)
        
        symbols = common_symbol_data.get("symbols", [])
        
        # Extract Binance TR symbols and Binance Futures symbols
        EXCHANGE_symbol_list = [symbol.get("binance_tr_symbol") for symbol in symbols if symbol.get("binance_tr_symbol")]
        binance_symbol_list = [symbol.get("binance_futures_symbol") for symbol in symbols if symbol.get("binance_futures_symbol")]
        
        # Convert list to JSON string before storing in Redis
        if r:
            r.set('EXCHANGE_symbol_list', json.dumps(EXCHANGE_symbol_list))

        logging.info(f"Loaded {len(EXCHANGE_symbol_list)} Binance TR symbols from common_symbol_info.json")
        logging.info(f"Loaded {len(binance_symbol_list)} Binance Futures symbols from common_symbol_info.json")
        
        if EXCHANGE_symbol_list:
            logging.info(f"First 5 Binance TR symbols: {EXCHANGE_symbol_list[:5]}")
        if binance_symbol_list:
            logging.info(f"First 5 Binance Futures symbols: {binance_symbol_list[:5]}")
            
    except FileNotFoundError:
        logging.error(f"File not found: {COMMON_SYMBOL_INFO_FILE}")
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing {COMMON_SYMBOL_INFO_FILE}: {e}")
    except Exception as e:
        logging.error(f"Error loading common symbol info: {e}")
else:
    logging.error(f"common_symbol_info.json not found. Please run generate_common_symbol_info.py first.")

if not EXCHANGE_symbol_list or not binance_symbol_list:
    logging.warning("Symbol lists are empty. The application may not work correctly.")

# Exceptional symbol mappings for symbols with different prefixes/suffixes between exchanges
# Format: {base_symbol: binance_cs_symbol}
# These symbols need special handling when mapping between EXCHANGE (Binance TR) and Binance CS (Futures)
EXCEPTIONAL_SYMBOL_MAPPINGS = {
    "SHIB": "1000SHIB",    # SHIB_TRY -> 1000SHIBUSDT
    "BONK": "1000BONK",    # BONK_TRY -> 1000BONKUSDT
    "FLOKI": "1000FLOKI",  # FLOKI_TRY -> 1000FLOKIUSDT
    "PEPE": "1000PEPE",    # PEPE_TRY -> 1000PEPEUSDT
    "LUNC": "1000LUNC",    # LUNC_TRY -> 1000LUNCUSDT
    "XEC": "1000XEC",      # XEC_TRY -> 1000XECUSDT
}

# Reverse mapping: Binance CS symbol -> base symbol
EXCEPTIONAL_SYMBOL_REVERSE_MAPPINGS = {v: k for k, v in EXCEPTIONAL_SYMBOL_MAPPINGS.items()}

def normalize_base_symbol_to_binance_cs(base_symbol: str) -> str:
    """
    Normalize a base symbol from EXCHANGE format to Binance CS format.
    
    Args:
        base_symbol: Base symbol from EXCHANGE (e.g., "SHIB", "LUNA")
    
    Returns:
        Normalized base symbol for Binance CS (e.g., "1000SHIB", "LUNA2")
    """
    return EXCEPTIONAL_SYMBOL_MAPPINGS.get(base_symbol, base_symbol)

def normalize_base_symbol_from_binance_cs(binance_cs_base_symbol: str) -> str:
    """
    Normalize a base symbol from Binance CS format to EXCHANGE format.
    
    Args:
        binance_cs_base_symbol: Base symbol from Binance CS (e.g., "1000SHIB", "LUNA2")
    
    Returns:
        Normalized base symbol for EXCHANGE (e.g., "SHIB", "LUNA")
    """
    return EXCEPTIONAL_SYMBOL_REVERSE_MAPPINGS.get(binance_cs_base_symbol, binance_cs_base_symbol)

def normalize_symbol_for_storage(base_symbol: str) -> str:
    """
    Normalize a base symbol to a common format for Redis/storage.
    Uses EXCHANGE format as the canonical format.
    
    Args:
        base_symbol: Base symbol in any format
    
    Returns:
        Normalized base symbol in EXCHANGE format
    """
    # First try reverse mapping (if it's in Binance CS format)
    normalized = normalize_base_symbol_from_binance_cs(base_symbol)
    return normalized

def is_1000_prefix_symbol(binance_cs_symbol: str) -> bool:
    """
    Check if a Binance CS symbol has the "1000" prefix.
    These symbols need price adjustment (multiply by 0.001) because
    the price shown is for 1000 units, not 1 unit.
    
    Args:
        binance_cs_symbol: Binance CS symbol (e.g., "1000SHIBUSDT", "1000BONKUSDT")
    
    Returns:
        True if symbol has "1000" prefix, False otherwise
    """
    # Remove "USDT" suffix if present
    base_symbol = binance_cs_symbol
    if binance_cs_symbol.endswith("USDT"):
        base_symbol = binance_cs_symbol[:-4]
    
    # Check if it starts with "1000"
    return base_symbol.startswith("1000")

def build_binance_cs_update_map(binance_symbol_list: list, symbol_index_map: dict) -> dict:
    """
    Build a mapping from Binance raw symbol to (index, price_multiplier, amount_multiplier).
    This eliminates all string processing in the hot loop.
    
    Args:
        binance_symbol_list: List of Binance CS symbols (e.g., ["0GUSDT", "1000SHIBUSDT", ...])
        symbol_index_map: Mapping from base symbol to local index (e.g., {"0G": 0, "SHIB": 1, ...})
    
    Returns:
        Dictionary mapping raw_symbol -> (local_index, price_multiplier, amount_multiplier)
        - local_index: Index in arbitrage_table_np
        - price_multiplier: Multiply price by this (0.001 for 1000-prefix, 1.0 otherwise)
        - amount_multiplier: Multiply amount by this when placing orders (1000.0 for 1000-prefix, 1.0 otherwise)
    """
    update_map = {}
    

    # Define 1000-prefix symbols (price is for 1000 units, need to multiply by 0.001)
    # When placing orders, need to multiply amount by 1000.0
    thousand_prefix_symbols = {
        "1000PEPEUSDT": ("PEPE", 0.001, 1000.0),
        "1000BONKUSDT": ("BONK", 0.001, 1000.0),
        "1000SHIBUSDT": ("SHIB", 0.001, 1000.0),
        "1000FLOKIUSDT": ("FLOKI", 0.001, 1000.0),
        "1000LUNCUSDT": ("LUNC", 0.001, 1000.0),
        "1000XECUSDT": ("XEC", 0.001, 1000.0),
    }
    
    # Combine all special cases
    special_cases = {**thousand_prefix_symbols}
    
    # Iterate through all Binance CS symbols
    for raw_symbol in binance_symbol_list:
        if raw_symbol in special_cases:
            base_symbol, price_mult, amount_mult = special_cases[raw_symbol]
        else:
            # Standard symbol: remove USDT suffix
            if raw_symbol.endswith('USDT'):
                base_symbol = raw_symbol[:-4]
            elif raw_symbol.endswith('usdt'):
                base_symbol = raw_symbol[:-4]
            else:
                base_symbol = raw_symbol
            
            price_mult = 1.0
            amount_mult = 1.0
        
        # Get index from symbol_index_map (using normalized base symbol)
        # CRITICAL: base_symbol must match what's in symbol_index_map (EXCHANGE format)
        # For exceptional symbols, the mapping already provides the correct EXCHANGE format base symbol
        idx = symbol_index_map.get(base_symbol)
        if idx is not None:
            update_map[raw_symbol] = (idx, price_mult, amount_mult)
        else:
            # Log warning for debugging (this should be rare if mappings are correct)
            logging.debug(f"Symbol {raw_symbol} (base: {base_symbol}) not found in symbol_index_map. "
                         f"Available keys: {list(symbol_index_map.keys())[:10]}...")
    
    return update_map

def get_binance_cs_amount_multiplier(binance_cs_symbol: str) -> float:
    """
    Get the amount multiplier for a Binance CS symbol when placing orders.
    For 1000-prefix symbols, multiply amount by 1000.0 (since we store normalized amounts).
    
    Args:
        binance_cs_symbol: Binance CS symbol (e.g., "1000SHIBUSDT", "BTCUSDT")
    
    Returns:
        Amount multiplier (1000.0 for 1000-prefix symbols, 1.0 otherwise)
    """
    # Check if it's a 1000-prefix symbol
    base_symbol = binance_cs_symbol
    if binance_cs_symbol.endswith("USDT"):
        base_symbol = binance_cs_symbol[:-4]
    
    if base_symbol.startswith("1000"):
        return 1000.0
    return 1.0

def split_symbols(symbols, num_groups):
    """Split symbols into num_groups groups."""
    symbols_per_group = len(symbols) // num_groups
    return [symbols[i*symbols_per_group:(i+1)*symbols_per_group] for i in range(num_groups)]

groups_no = 10

# Split EXCHANGE_symbol_list into 10 groups
EXCHANGE_symbol_list_groups = split_symbols(EXCHANGE_symbol_list, groups_no)
binance_symbol_list_groups = split_symbols(binance_symbol_list, groups_no)

# Define the columns list
columns = ['BaseSymbol', 'Binance_Symbol', 'Binance_Time', 'Binance_TimeDiff',
           'Binance_free_usdt', 'Binance_free_TRY', 'EXCHANGE_free_TRY',
           'EXCHANGE_Symbol', 'EXCHANGE_Time',
           'OpenMargin', 'OpenTriggerMargin', 'OpenStopMargin', 'OpenMarginWindow', 'OpenAggression',
           'CloseMargin', 'CloseTriggerMargin', 'CloseStopMargin', 'CloseMarginWindow', 'CloseAggression',
           'MakerType', 'BuyOrderLock', 'SellOrderLock', 'BuyActionType', 'SellActionType',
           'AmountPrecision', 'EXCHANGE_price_precision', 'EXCHANGE_price_step', 'EXCHANGE_AmountPrecision',
           'EXCHANGE_orderable_bid_price', 'EXCHANGE_orderable_ask_price',
           'EXCHANGE_aggression_bid_price', 'EXCHANGE_aggression_ask_price',
           'EXCHANGE_next_bid_price', 'EXCHANGE_next_ask_price',
           'BuyOrderID', 'BuyOrderTime', 'BuyOrderAmount', 'BuyOrderPrice', 'BuyOrderBestPrice',
           'SellOrderID', 'SellOrderTime', 'SellOrderAmount', 'SellOrderPrice', 'SellOrderBestPrice',
           'BuyOrderLockTime', 'SellOrderLockTime',
           'OpenArbitAmount_coin', 'OpenArbitAmount_usdt', 'OpenArbitAmount_TRY',
           'CloseArbitAmount_coin', 'CloseArbitAmount_usdt', 'CloseArbitAmount_TRY',
           'OpenOrderAmount_coin', 'OpenOrderAmount_usdt', 'OpenOrderAmount_TRY',
           'CloseOrderAmount_coin', 'CloseOrderAmount_usdt', 'CloseOrderAmount_TRY',
           'MinBuyOrderAmount_TRY', 'MinSellOrderAmount_TRY', 'MaxPositionAmount_TRY', 'CapacityGap_TRY',
           'Binance_PositionAmount_coin', 'Binance_PositionAmount_usdt', 'Binance_PositionAmount_TRY',
           'EXCHANGE_PositionAmount_coin', 'EXCHANGE_PositionAmount_coin_total', 'EXCHANGE_PositionAmount_TRY_total',
           'EXCHANGE_PositionAmount_usdt', 'EXCHANGE_PositionAmount_TRY', 'USDTTRY_bid', 'USDTTRY_ask',
           'DMT_Enabled', 'DMT_OpenBufferBps', 'DMT_CloseBufferBps', 'DMT_UpdateInterval',
           'MA_OpenMargin', 'DMT_MinOpenMarginBps', 
           'MA_CloseMargin', 'DMT_MinCloseMarginBps', 'MoveThreshold']

columns_to_add_Binance = ['Binance_AskP1', 'Binance_AskA1', 'Binance_BidP1', 'Binance_BidA1']

columns_to_add_EXCHANGE = ['EXCHANGE_AskP1', 'EXCHANGE_AskA1', 'EXCHANGE_BidP1', 'EXCHANGE_BidA1']

columns = columns + columns_to_add_Binance + columns_to_add_EXCHANGE

# Create the dictionary mapping column names to their indices
col_idx = {name: idx for idx, name in enumerate(columns)}
logging.info(f"Length of column indices: {len(col_idx)}")

# Note: OrderBookDepthCheck column not defined in columns list - removed lookup

col_Binance_AskP1 = col_idx.get('Binance_AskP1')
col_Binance_AskA1 = col_idx.get('Binance_AskA1')
col_Binance_BidP1 = col_idx.get('Binance_BidP1')
col_Binance_BidA1 = col_idx.get('Binance_BidA1')

col_EXCHANGE_AskP1 = col_idx.get('EXCHANGE_AskP1')
col_EXCHANGE_AskA1 = col_idx.get('EXCHANGE_AskA1')
col_EXCHANGE_BidP1 = col_idx.get('EXCHANGE_BidP1')
col_EXCHANGE_BidA1 = col_idx.get('EXCHANGE_BidA1')

# Create column index variables for the specific Binance-related columns
# Create column index variables for all columns
col_Base_Symbol = col_idx.get('BaseSymbol')
col_Binance_Symbol = col_idx.get('Binance_Symbol')  # Fixed: was 'BinanceSymbol'
col_Binance_Time = col_idx.get('Binance_Time')  # Fixed: was 'BinanceTime'
col_BinanceTimeDiff = col_idx.get('Binance_TimeDiff')  # Fixed: was 'BinanceTimeDiff'

col_Binance_free_usdt = col_idx.get('Binance_free_usdt')
col_Binance_free_TRY = col_idx.get('Binance_free_TRY')
# Note: EXCHANGE_free_usdt column not defined in columns list - removed lookup
col_EXCHANGE_free_TRY = col_idx.get('EXCHANGE_free_TRY')
col_EXCHANGE_Symbol = col_idx.get('EXCHANGE_Symbol')  # Fixed: was 'EXCHANGESymbol'
col_EXCHANGE_Time = col_idx.get('EXCHANGE_Time')  # Fixed: was 'EXCHANGETime'

col_OpenTriggerMargin = col_idx.get('OpenTriggerMargin')
col_CloseTriggerMargin = col_idx.get('CloseTriggerMargin')
col_OpenMargin = col_idx.get('OpenMargin')
col_CloseMargin = col_idx.get('CloseMargin')
col_OpenMarginWindow = col_idx.get('OpenMarginWindow')
col_CloseMarginWindow = col_idx.get('CloseMarginWindow')
col_OpenStopMargin = col_idx.get('OpenStopMargin')
col_CloseStopMargin = col_idx.get('CloseStopMargin')
col_OpenAggression = col_idx.get('OpenAggression')
col_CloseAggression = col_idx.get('CloseAggression')

col_Maker_Type = col_idx.get('MakerType')  # Fixed: was 'Maker_Type'
col_Buy_Order_Lock = col_idx.get('BuyOrderLock')  # Fixed: was 'Buy_Order_Lock'
col_Sell_Order_Lock = col_idx.get('SellOrderLock')  # Fixed: was 'Sell_Order_Lock'


col_AmountPrecision = col_idx.get('AmountPrecision')
col_EXCHANGE_price_precision = col_idx.get('EXCHANGE_price_precision')
col_EXCHANGE_price_step = col_idx.get('EXCHANGE_price_step')
col_EXCHANGE_AmountPrecision = col_idx.get('EXCHANGE_AmountPrecision')

col_EXCHANGE_aggression_bid_price = col_idx.get('EXCHANGE_aggression_bid_price')
col_EXCHANGE_aggression_ask_price = col_idx.get('EXCHANGE_aggression_ask_price')

col_EXCHANGE_next_bid_price = col_idx.get('EXCHANGE_next_bid_price')
col_EXCHANGE_next_ask_price = col_idx.get('EXCHANGE_next_ask_price')

col_EXCHANGE_orderable_bid_price = col_idx.get('EXCHANGE_orderable_bid_price')
col_EXCHANGE_orderable_ask_price = col_idx.get('EXCHANGE_orderable_ask_price')

col_Buy_ActionType = col_idx.get('BuyActionType')  # Fixed: was 'Buy_ActionType'
col_Sell_ActionType = col_idx.get('SellActionType')  # Fixed: was 'Sell_ActionType'

col_MinBuyOrderAmount_TRY = col_idx.get('MinBuyOrderAmount_TRY')
col_MinSellOrderAmount_TRY = col_idx.get('MinSellOrderAmount_TRY')

col_MaxPositionAmount_TRY = col_idx.get('MaxPositionAmount_TRY')
col_BinancePositionAmount_coin = col_idx.get('Binance_PositionAmount_coin')  # Fixed: was 'BinancePositionAmount_coin'
col_BinancePositionAmount_usdt = col_idx.get('Binance_PositionAmount_usdt')  # Fixed: was 'BinancePositionAmount_usdt'
col_BinancePositionAmount_TRY = col_idx.get('Binance_PositionAmount_TRY')  # Fixed: was 'BinancePositionAmount_TRY'
col_EXCHANGE_PositionAmount_coin = col_idx.get('EXCHANGE_PositionAmount_coin')  # Fixed: was 'EXCHANGEPositionAmount_coin'
col_EXCHANGE_PositionAmount_coin_total = col_idx.get('EXCHANGE_PositionAmount_coin_total')  # Fixed: was 'EXCHANGEPositionAmount_coin_total'
col_EXCHANGE_PositionAmount_TRY_total = col_idx.get('EXCHANGE_PositionAmount_TRY_total')  # Fixed: was 'EXCHANGEPositionAmount_TRY_total'
col_EXCHANGE_PositionAmount_usdt = col_idx.get('EXCHANGE_PositionAmount_usdt')  # Fixed: was 'EXCHANGEPositionAmount_usdt'
col_EXCHANGE_PositionAmount_TRY = col_idx.get('EXCHANGE_PositionAmount_TRY')  # Fixed: was 'EXCHANGEPositionAmount_TRY'
col_USDTTRY_bid = col_idx.get('USDTTRY_bid')    
col_USDTTRY_ask = col_idx.get('USDTTRY_ask')

col_CapacityGap_TRY = col_idx.get('CapacityGap_TRY')
# Note: MinOrderAmount_coin column not defined in columns list - removed lookup

col_Buy_Order_ID = col_idx.get('BuyOrderID')  # Fixed: was 'Buy_Order_ID'
col_Buy_Order_Time = col_idx.get('BuyOrderTime')  # Fixed: was 'Buy_Order_Time'
col_Buy_Order_Amount = col_idx.get('BuyOrderAmount')  # Fixed: was 'Buy_Order_Amount'
col_Buy_Order_Price = col_idx.get('BuyOrderPrice')  # Fixed: was 'Buy_Order_Price'
col_Buy_Order_Best_Price = col_idx.get('BuyOrderBestPrice')  # Fixed: was 'Buy_Order_Best_Price'

col_Sell_Order_ID = col_idx.get('SellOrderID')  # Fixed: was 'Sell_Order_ID'
col_Sell_Order_Time = col_idx.get('SellOrderTime')  # Fixed: was 'Sell_Order_Time'
col_Sell_Order_Amount = col_idx.get('SellOrderAmount')  # Fixed: was 'Sell_Order_Amount'
col_Sell_Order_Price = col_idx.get('SellOrderPrice')  # Fixed: was 'Sell_Order_Price'
col_Sell_Order_Best_Price = col_idx.get('SellOrderBestPrice')  # Fixed: was 'Sell_Order_Best_Price'

col_Buy_Order_Lock_Time = col_idx.get('BuyOrderLockTime')  # Fixed: was 'Buy_Order_Lock_Time'
col_Sell_Order_Lock_Time = col_idx.get('SellOrderLockTime')  # Fixed: was 'Sell_Order_Lock_Time'

col_MoveThreshold = col_idx.get('MoveThreshold')


# CryptoStruct Trading Adapter Configuration
CS_TRADING_ADAPTER_HOST = "3.127.121.170"  # Trading Adapter Public IP
CS_TRADING_ADAPTER_PORT = 3000
CS_TRADING_ADAPTER_WS_ENDPOINT = "/api/v2"  # Trading Adapter WebSocket endpoint

# Client identification for Trading Adapter
CLIENT_ORG = "CryptoStruct"
CLIENT_APP_NAME = "arbit_core_taker"
CLIENT_APP_VER = "1.0.0"
CLIENT_PROCESS_ID = "quantxai-algoX"  # Process ID for Trading Adapter

# CS Order timeout (in seconds)
CS_ORDER_PLACE_RESPONSE_TIMEOUT = 1.0  # Timeout for placeResponse
CS_ORDER_FILL_TIMEOUT = 1.0  # Timeout for fill message

CS_HEARTBEAT_SEC = 10
CS_PING_TIMEOUT_SEC = 10

try:
    if r is not None:
        TEST_MODE = r.get('TEST_MODE')
        if TEST_MODE is not None:
            TEST_MODE = int(TEST_MODE.decode('utf-8')) if isinstance(TEST_MODE, bytes) else int(TEST_MODE)
        else:
            TEST_MODE = 1
            r.set('TEST_MODE', TEST_MODE)
            logging.info(f"TEST_MODE set to 1 by default")
    else:
        TEST_MODE = 1
        logging.warning("Redis not available, TEST_MODE set to 1 by default")
except Exception as e:
    logging.error(f"Error getting TEST_MODE from Redis: {e}")
    TEST_MODE = 1
    if r is not None:
        try:
            r.set('TEST_MODE', TEST_MODE)
            logging.info(f"TEST_MODE set to 1 by default")
        except Exception as e2:
            logging.warning(f"Could not set TEST_MODE in Redis: {e2}")
    else:
        logging.warning("Redis not available, TEST_MODE set to 1 by default")

if TEST_MODE == 1:
    ACCOUNT_ID = 201  # Account ID from UI (Binance Test(ALPER)account)
else:
    ACCOUNT_ID = 202  # Account ID from UI (Binance Kerim account)


def fetch_EXCHANGE_balance():
    """
    Fetch EXCHANGE (Binance TR) balance from API.
    Returns balance dictionary in format: {symbol: {'available': float, 'total': float, 'locked': float}, ...}
    Based on send_binance_tr_order_account.py get_account_info function.
    """
    try:
        # Binance TR API endpoints
        BASE_URL = "https://www.binance.tr"
        ACCOUNT_ENDPOINT = "/open/v1/account/spot"
        RECV_WINDOW = 5000
        
        # Get API keys based on TEST_MODE
        if TEST_MODE == 1:
            API_KEY = APIkeys.BINANCE_TR_api_key_ALPER
            API_SECRET = APIkeys.BINANCE_TR_secret_key_ALPER
        else:
            # For production, you may need to add different keys
            API_KEY = APIkeys.BINANCE_TR_api_key_ALPER
            API_SECRET = APIkeys.BINANCE_TR_secret_key_ALPER
        
        API_SECRET_BYTES = API_SECRET.encode('utf-8')
        
        # Create HTTP connection pool
        http = urllib3.PoolManager(
            num_pools=1,
            maxsize=10,
            block=False,
            timeout=urllib3.Timeout(connect=2.0, read=5.0),
            retries=False
        )
        
        url = BASE_URL + ACCOUNT_ENDPOINT
        
        # Prepare parameters
        params = {
            "recvWindow": RECV_WINDOW,
            "timestamp": int(time.time() * 1000),
        }
        
        # Sign payload
        query = urlencode(params, doseq=True)
        query_bytes = query.encode('utf-8')
        signature = hmac.new(API_SECRET_BYTES, query_bytes, hashlib.sha256).hexdigest()
        full_url = f"{url}?{query}&signature={signature}"
        
        headers = {"X-MBX-APIKEY": API_KEY}
        
        # Make API request
        resp = http.request('GET', full_url, headers=headers, timeout=5.0)
        
        # Parse response
        try:
            data = json.loads(resp.data)
        except Exception as e:
            logging.error(f"Error parsing EXCHANGE balance response: {e}")
            return None
        
        # Check if request was successful
        if resp.status != 200 or data.get("code") != 0:
            error_code = data.get("code")
            error_msg = data.get("msg", "Unknown error")
            
            # Handle specific error codes
            if error_code == 1008:
                # Error 1008: Rate limit or temporary API issue
                logging.warning(f"EXCHANGE balance API returned code 1008 (rate limit/temporary issue): {error_msg}. Will retry on next update.")
            else:
                logging.error(f"Error fetching EXCHANGE balance: HTTP {resp.status}, Code {error_code}, Message: {error_msg}")
            
            return None
        
        # Extract account data
        account_data = data.get("data", {})
        assets = account_data.get("accountAssets", [])
        
        # Convert to dictionary format: {symbol: {'available': float, 'total': float, 'locked': float}, ...}
        balance_dict = {}
        for asset in assets:
            asset_name = asset.get("asset", "")
            if asset_name:
                try:
                    free = float(asset.get("free", "0"))
                    locked = float(asset.get("locked", "0"))
                    total = free + locked
                    
                    balance_dict[asset_name] = {
                        'available': free,
                        'total': total,
                        'locked': locked
                    }
                except (ValueError, TypeError) as e:
                    logging.warning(f"Error parsing asset {asset_name}: {e}")
                    continue
        
        return balance_dict
        
    except Exception as e:
        logging.error(f"Error fetching EXCHANGE balance: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None

# Load precision mappings from common_symbol_info.json (done once, cached)
_symbol_to_price_precision = {}
_symbol_to_amount_precision = {}
_symbol_to_price_step = {}
_precision_data_loaded = False

def _load_precision_data():
    """Load precision data from common_symbol_info.json once."""
    global _symbol_to_price_precision, _symbol_to_amount_precision, _symbol_to_price_step, _precision_data_loaded
    
    if _precision_data_loaded:
        return  # Already loaded
    
    try:
        if COMMON_SYMBOL_INFO_FILE.exists():
            with open(COMMON_SYMBOL_INFO_FILE, 'r', encoding='utf-8') as f:
                common_symbol_info = json.load(f)
                symbols_list = common_symbol_info.get('symbols', [])
                
                # Create mapping from binance_tr_symbol to precision values
                for symbol_info in symbols_list:
                    binance_tr_symbol = symbol_info.get('binance_tr_symbol')
                    if binance_tr_symbol:
                        _symbol_to_price_precision[binance_tr_symbol] = symbol_info.get('binance_tr_price_precision', 8)
                        _symbol_to_amount_precision[binance_tr_symbol] = symbol_info.get('binance_tr_amount_precision', 8)
                        _symbol_to_price_step[binance_tr_symbol] = symbol_info.get('binance_tr_price_step_size', 1/np.power(10, 8))
                
                logging.info(f"Loaded precision data for {len(_symbol_to_price_precision)} symbols from common_symbol_info.json")
                _precision_data_loaded = True
        else:
            logging.error(f"common_symbol_info.json not found at {COMMON_SYMBOL_INFO_FILE}")
    except Exception as e:
        logging.error(f"Error loading precision data from common_symbol_info.json: {e}")
        import traceback
        logging.error(traceback.format_exc())

def get_precisions_for_symbols(symbols):
    """
    Get price precisions, amount precisions, and price steps for a list of symbols.
    
    Args:
        symbols: List of binance_tr_symbol strings (e.g., ['ETHTRY', 'BTCTRY', ...])
    
    Returns:
        tuple: (price_precisions, amount_precisions, price_steps) as numpy arrays
    """
    _load_precision_data()  # Ensure data is loaded
    
    price_precisions = np.array([_symbol_to_price_precision.get(symbol, 8) for symbol in symbols])
    amount_precisions = np.array([_symbol_to_amount_precision.get(symbol, 8) for symbol in symbols])
    price_steps = np.array([_symbol_to_price_step.get(symbol, 1/np.power(10, 8)) for symbol in symbols])
    
    return price_precisions, amount_precisions, price_steps

