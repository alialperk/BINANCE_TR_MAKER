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
BinanceTR_symbol_list = []
binance_symbol_list = []

if COMMON_SYMBOL_INFO_FILE.exists():
    try:
        with open(COMMON_SYMBOL_INFO_FILE, "r", encoding="utf-8") as f:
            common_symbol_data = json.load(f)
        
        symbols = common_symbol_data.get("symbols", [])
        
        # Extract Binance TR symbols and Binance Futures symbols
        BinanceTR_symbol_list = [symbol.get("binance_tr_symbol") for symbol in symbols if symbol.get("binance_tr_symbol")]
        binance_symbol_list = [symbol.get("binance_futures_symbol") for symbol in symbols if symbol.get("binance_futures_symbol")]
        
        logging.info(f"Loaded {len(BinanceTR_symbol_list)} Binance TR symbols from common_symbol_info.json")
        logging.info(f"Loaded {len(binance_symbol_list)} Binance Futures symbols from common_symbol_info.json")
        
        if BinanceTR_symbol_list:
            logging.info(f"First 5 Binance TR symbols: {BinanceTR_symbol_list[:5]}")
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

if not BinanceTR_symbol_list or not binance_symbol_list:
    logging.warning("Symbol lists are empty. The application may not work correctly.")


# Define the columns list
columns = ['BaseSymbol', 'Binance_Symbol', 'Binance_Time', 'Binance_TimeDiff',
           'Binance_free_usdt', 'Binance_free_TRY', 'BinTR_free_TRY',
           'BinTR_Symbol', 'BinTR_Time',
           'OpenMargin', 'OpenTriggerMargin', 'OpenStopMargin', 'OpenMarginWindow', 'OpenAggression',
           'CloseMargin', 'CloseTriggerMargin', 'CloseStopMargin', 'CloseMarginWindow', 'CloseAggression',
           'MakerType', 'BuyOrderLock', 'SellOrderLock', 'BuyActionType', 'SellActionType',
           'AmountPrecision', 'BinTR_price_precision', 'BinTR_price_step', 'BinTR_AmountPrecision',
           'BinTR_orderable_bid_price', 'BinTR_orderable_ask_price',
           'BinTR_aggression_bid_price', 'BinTR_aggression_ask_price',
           'BinTR_next_bid_price', 'BinTR_next_ask_price',
           'BuyOrderID', 'BuyOrderTime', 'BuyOrderAmount', 'BuyOrderPrice', 'BuyOrderBestPrice',
           'SellOrderID', 'SellOrderTime', 'SellOrderAmount', 'SellOrderPrice', 'SellOrderBestPrice',
           'BuyOrderLockTime', 'SellOrderLockTime',
           'OpenArbitAmount_coin', 'OpenArbitAmount_usdt', 'OpenArbitAmount_TRY',
           'CloseArbitAmount_coin', 'CloseArbitAmount_usdt', 'CloseArbitAmount_TRY',
           'OpenOrderAmount_coin', 'OpenOrderAmount_usdt', 'OpenOrderAmount_TRY',
           'CloseOrderAmount_coin', 'CloseOrderAmount_usdt', 'CloseOrderAmount_TRY',
           'MinBuyOrderAmount_TRY', 'MinSellOrderAmount_TRY', 'MaxPositionAmount_TRY', 'CapacityGap_TRY',
           'Binance_PositionAmount_coin', 'Binance_PositionAmount_usdt', 'Binance_PositionAmount_TRY',
           'BinTR_PositionAmount_coin', 'BinTR_PositionAmount_coin_total', 'BinTR_PositionAmount_TRY_total',
           'BinTR_PositionAmount_usdt', 'BinTR_PositionAmount_TRY', 'USDTTRY_bid', 'USDTTRY_ask',
           'DMT_Enabled', 'DMT_OpenBufferBps', 'DMT_CloseBufferBps', 'DMT_UpdateInterval',
           'MA_OpenMargin', 'DMT_MinOpenMarginBps', 
           'MA_CloseMargin', 'DMT_MinCloseMarginBps', 'MoveThreshold']

columns_to_add_Binance = ['Binance_AskP1', 'Binance_AskA1', 'Binance_BidP1', 'Binance_BidA1']

columns_to_add_BinTR = ['BinTR_AskP1', 'BinTR_AskA1', 'BinTR_BidP1', 'BinTR_BidA1']

columns = columns + columns_to_add_Binance + columns_to_add_BinTR

# Create the dictionary mapping column names to their indices
col_idx = {name: idx for idx, name in enumerate(columns)}
logging.info(f"Length of column indices: {len(col_idx)}")

# Note: OrderBookDepthCheck column not defined in columns list - removed lookup

col_Binance_AskP1 = col_idx.get('Binance_AskP1')
col_Binance_AskA1 = col_idx.get('Binance_AskA1')
col_Binance_BidP1 = col_idx.get('Binance_BidP1')
col_Binance_BidA1 = col_idx.get('Binance_BidA1')

col_BinTR_AskP1 = col_idx.get('BinTR_AskP1')
col_BinTR_AskA1 = col_idx.get('BinTR_AskA1')
col_BinTR_BidP1 = col_idx.get('BinTR_BidP1')
col_BinTR_BidA1 = col_idx.get('BinTR_BidA1')

# Create column index variables for the specific Binance-related columns
# Create column index variables for all columns
col_Base_Symbol = col_idx.get('BaseSymbol')
col_Binance_Symbol = col_idx.get('Binance_Symbol')  # Fixed: was 'BinanceSymbol'
col_Binance_Time = col_idx.get('Binance_Time')  # Fixed: was 'BinanceTime'
col_BinanceTimeDiff = col_idx.get('Binance_TimeDiff')  # Fixed: was 'BinanceTimeDiff'

col_Binance_free_usdt = col_idx.get('Binance_free_usdt')
col_Binance_free_TRY = col_idx.get('Binance_free_TRY')
# Note: BinTR_free_usdt column not defined in columns list - removed lookup
col_BinTR_free_TRY = col_idx.get('BinTR_free_TRY')
col_BinTR_Symbol = col_idx.get('BinTR_Symbol')  # Fixed: was 'BinTRSymbol'
col_BinTR_Time = col_idx.get('BinTR_Time')  # Fixed: was 'BinTRTime'

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
col_BinTR_price_precision = col_idx.get('BinTR_price_precision')
col_BinTR_price_step = col_idx.get('BinTR_price_step')
col_BinTR_AmountPrecision = col_idx.get('BinTR_AmountPrecision')

col_BinTR_aggression_bid_price = col_idx.get('BinTR_aggression_bid_price')
col_BinTR_aggression_ask_price = col_idx.get('BinTR_aggression_ask_price')

col_BinTR_next_bid_price = col_idx.get('BinTR_next_bid_price')
col_BinTR_next_ask_price = col_idx.get('BinTR_next_ask_price')

col_BinTR_orderable_bid_price = col_idx.get('BinTR_orderable_bid_price')
col_BinTR_orderable_ask_price = col_idx.get('BinTR_orderable_ask_price')

col_Buy_ActionType = col_idx.get('BuyActionType')  # Fixed: was 'Buy_ActionType'
col_Sell_ActionType = col_idx.get('SellActionType')  # Fixed: was 'Sell_ActionType'

col_MinBuyOrderAmount_TRY = col_idx.get('MinBuyOrderAmount_TRY')
col_MinSellOrderAmount_TRY = col_idx.get('MinSellOrderAmount_TRY')

col_MaxPositionAmount_TRY = col_idx.get('MaxPositionAmount_TRY')
col_BinancePositionAmount_coin = col_idx.get('Binance_PositionAmount_coin')  # Fixed: was 'BinancePositionAmount_coin'
col_BinancePositionAmount_usdt = col_idx.get('Binance_PositionAmount_usdt')  # Fixed: was 'BinancePositionAmount_usdt'
col_BinancePositionAmount_TRY = col_idx.get('Binance_PositionAmount_TRY')  # Fixed: was 'BinancePositionAmount_TRY'
col_BinTR_PositionAmount_coin = col_idx.get('BinTR_PositionAmount_coin')  # Fixed: was 'BinTRPositionAmount_coin'
col_BinTR_PositionAmount_coin_total = col_idx.get('BinTR_PositionAmount_coin_total')  # Fixed: was 'BinTRPositionAmount_coin_total'
col_BinTR_PositionAmount_TRY_total = col_idx.get('BinTR_PositionAmount_TRY_total')  # Fixed: was 'BinTRPositionAmount_TRY_total'
col_BinTR_PositionAmount_usdt = col_idx.get('BinTR_PositionAmount_usdt')  # Fixed: was 'BinTRPositionAmount_usdt'
col_BinTR_PositionAmount_TRY = col_idx.get('BinTR_PositionAmount_TRY')  # Fixed: was 'BinTRPositionAmount_TRY'
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


def fetch_BinTR_balance():
    try:
        BinTR_balance = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
        return BinTR_balance
    except Exception as e:
        logging.error(f"Error fetching BinTR balance: {e}")
        return None

