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

r = None
try:
    r = redis.Redis(host='localhost', port=6379)
except Exception as e:
    print(f"Error connecting to Redis: {e}")
    exit()

symbol_list = r.get('BinanceTR_Symbol_List')

if symbol_list is None:
    r.set('BinanceTR_Symbol_List', json.dumps([]))
    symbol_list = r.get('BinanceTR_Symbol_List')

# Decode bytes to string if needed, then parse JSON
if isinstance(symbol_list, bytes):
    symbol_list = symbol_list.decode('utf-8')

try:
    base_symbol_list = json.loads(symbol_list) if isinstance(symbol_list, str) else symbol_list
except (json.JSONDecodeError, TypeError):
    base_symbol_list = []
    
# remove USDT from symbol_list
base_symbol_list = [symbol for symbol in base_symbol_list if symbol != "USDT"]


# Use the original base_symbol_list for both, but apply transformations for Binance
BinanceTR_symbol_list = [f"{symbol}TRY" for symbol in base_symbol_list]
binance_symbol_list = [f"{symbol}USDT" for symbol in base_symbol_list]

combined_symbol_list = base_symbol_list



# Define the columns list
columns = ['BaseSymbol', 'BinanceSymbol', 'BinanceTime', 'BinanceTimeDiff',
           'Binance_free_usdt', 'Binance_free_TRY', 'BTCTURK_free_TRY',
           'BTCTURKSymbol', 'BTCTURKTime',
           'OpenMargin', 'OpenTriggerMargin', 'OpenStopMargin', 'OpenMarginWindow', 'OpenAggression',
           'CloseMargin', 'CloseTriggerMargin', 'CloseStopMargin', 'CloseMarginWindow', 'CloseAggression',
           'Maker_Type', 'Buy_Order_Lock', 'Sell_Order_Lock', 'Buy_ActionType', 'Sell_ActionType',
           'AmountPrecision', 'BTCTURK_price_precision', 'BTCTURK_price_step', 'BTCTURK_AmountPrecision',
           'BTCTURK_orderable_bid_price', 'BTCTURK_orderable_ask_price',
           'BTCTURK_aggression_bid_price', 'BTCTURK_aggression_ask_price',
           'BTCTURK_next_bid_price', 'BTCTURK_next_ask_price',
           'Buy_Order_ID', 'Buy_Order_Time', 'Buy_Order_Amount', 'Buy_Order_Price', 'Buy_Order_Best_Price',
           'Sell_Order_ID', 'Sell_Order_Time', 'Sell_Order_Amount', 'Sell_Order_Price', 'Sell_Order_Best_Price',
           'Buy_Order_Lock_Time', 'Sell_Order_Lock_Time',
           'OpenArbitAmount_coin', 'OpenArbitAmount_usdt', 'OpenArbitAmount_TRY',
           'CloseArbitAmount_coin', 'CloseArbitAmount_usdt', 'CloseArbitAmount_TRY',
           'OpenOrderAmount_coin', 'OpenOrderAmount_usdt', 'OpenOrderAmount_TRY',
           'CloseOrderAmount_coin', 'CloseOrderAmount_usdt', 'CloseOrderAmount_TRY',
           'MinBuyOrderAmount_TRY', 'MinSellOrderAmount_TRY', 'MaxPositionAmount_TRY', 'CapacityGap_TRY',
           'BinancePositionAmount_coin', 'BinancePositionAmount_usdt', 'BinancePositionAmount_TRY',
           'BTCTURKPositionAmount_coin', 'BTCTURKPositionAmount_coin_total', 'BTCTURKPositionAmount_TRY_total',
           'BTCTURKPositionAmount_usdt', 'BTCTURKPositionAmount_TRY', 'USDTTRY_bid', 'USDTTRY_ask',
           'DMT_enabled', 'DMT_open_buffer_bps', 'DMT_close_buffer_bps', 'DMT_update_interval',
           'MA_OpenMargin', 'DMT_min_open_margin_bps', 
           'MA_CloseMargin', 'DMT_min_close_margin_bps', 'MoveThreshold']

columns_to_add_Binance = ['Binance_AskP1', 'Binance_AskA1', 'Binance_BidP1', 'Binance_BidA1']

columns_to_add_BTCTURK = ['BTCTURK_AskP1', 'BTCTURK_AskA1', 'BTCTURK_BidP1', 'BTCTURK_BidA1']

columns = columns + columns_to_add_Binance + columns_to_add_BTCTURK

# Create the dictionary mapping column names to their indices
col_idx = {name: idx for idx, name in enumerate(columns)}
logging.info(f"Length of column indices: {len(col_idx)}")

col_OrderBookDepthCheck = col_idx.get('OrderBookDepthCheck')

col_Binance_AskP1 = col_idx.get('Binance_AskP1')
col_Binance_AskA1 = col_idx.get('Binance_AskA1')
col_Binance_BidP1 = col_idx.get('Binance_BidP1')
col_Binance_BidA1 = col_idx.get('Binance_BidA1')

col_BTCTURK_AskP1 = col_idx.get('BTCTURK_AskP1')
col_BTCTURK_AskA1 = col_idx.get('BTCTURK_AskA1')
col_BTCTURK_BidP1 = col_idx.get('BTCTURK_BidP1')
col_BTCTURK_BidA1 = col_idx.get('BTCTURK_BidA1')

# Create column index variables for the specific Binance-related columns
# Create column index variables for all columns
col_Base_Symbol = col_idx.get('BaseSymbol')
col_Binance_Symbol = col_idx.get('BinanceSymbol')
col_Binance_Time = col_idx.get('BinanceTime')
col_BinanceTimeDiff = col_idx.get('BinanceTimeDiff')

col_Binance_free_usdt = col_idx.get('Binance_free_usdt')
col_Binance_free_TRY = col_idx.get('Binance_free_TRY')
col_BTCTURK_free_usdt = col_idx.get(f'BTCTURK_free_usdt')
col_BTCTURK_free_TRY = col_idx.get(f'BTCTURK_free_TRY')
col_BTCTURK_Symbol = col_idx.get('BTCTURKSymbol')
col_BTCTURK_Time = col_idx.get('BTCTURKTime')

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

col_Maker_Type = col_idx.get('Maker_Type')
col_Buy_Order_Lock = col_idx.get('Buy_Order_Lock')
col_Sell_Order_Lock = col_idx.get('Sell_Order_Lock')


col_AmountPrecision = col_idx.get('AmountPrecision')
col_BTCTURK_price_precision = col_idx.get('BTCTURK_price_precision')
col_BTCTURK_price_step = col_idx.get('BTCTURK_price_step')
col_BTCTURK_AmountPrecision = col_idx.get('BTCTURK_AmountPrecision')

col_BTCTURK_aggression_bid_price = col_idx.get('BTCTURK_aggression_bid_price')
col_BTCTURK_aggression_ask_price = col_idx.get('BTCTURK_aggression_ask_price')

col_BTCTURK_next_bid_price = col_idx.get('BTCTURK_next_bid_price')
col_BTCTURK_next_ask_price = col_idx.get('BTCTURK_next_ask_price')

col_BTCTURK_orderable_bid_price = col_idx.get('BTCTURK_orderable_bid_price')
col_BTCTURK_orderable_ask_price = col_idx.get('BTCTURK_orderable_ask_price')

col_Buy_ActionType = col_idx.get('Buy_ActionType')
col_Sell_ActionType = col_idx.get('Sell_ActionType')

col_MinBuyOrderAmount_TRY = col_idx.get('MinBuyOrderAmount_TRY')
col_MinSellOrderAmount_TRY = col_idx.get('MinSellOrderAmount_TRY')

col_MaxPositionAmount_TRY = col_idx.get('MaxPositionAmount_TRY')
col_BinancePositionAmount_coin = col_idx.get('BinancePositionAmount_coin')
col_BinancePositionAmount_usdt = col_idx.get('BinancePositionAmount_usdt')
col_BinancePositionAmount_TRY = col_idx.get('BinancePositionAmount_TRY')
col_BTCTURK_PositionAmount_coin = col_idx.get('BTCTURKPositionAmount_coin')
col_BTCTURK_PositionAmount_coin_total = col_idx.get('BTCTURKPositionAmount_coin_total')
col_BTCTURK_PositionAmount_TRY_total = col_idx.get('BTCTURKPositionAmount_TRY_total')
col_BTCTURK_PositionAmount_usdt = col_idx.get('BTCTURKPositionAmount_usdt')
col_BTCTURK_PositionAmount_TRY = col_idx.get('BTCTURKPositionAmount_TRY')
col_USDTTRY_bid = col_idx.get('USDTTRY_bid')    
col_USDTTRY_ask = col_idx.get('USDTTRY_ask')

col_CapacityGap_TRY = col_idx.get('CapacityGap_TRY')
col_MinOrderAmount_coin = col_idx.get('MinOrderAmount_coin')

col_Buy_Order_ID = col_idx.get('Buy_Order_ID')
col_Buy_Order_Time = col_idx.get('Buy_Order_Time')
col_Buy_Order_Amount = col_idx.get('Buy_Order_Amount')
col_Buy_Order_Price = col_idx.get('Buy_Order_Price')
col_Buy_Order_Best_Price = col_idx.get('Buy_Order_Best_Price')

col_Sell_Order_ID = col_idx.get('Sell_Order_ID')
col_Sell_Order_Time = col_idx.get('Sell_Order_Time')
col_Sell_Order_Amount = col_idx.get('Sell_Order_Amount')
col_Sell_Order_Price = col_idx.get('Sell_Order_Price')
col_Sell_Order_Best_Price = col_idx.get('Sell_Order_Best_Price')

col_Buy_Order_Lock_Time = col_idx.get('Buy_Order_Lock_Time')
col_Sell_Order_Lock_Time = col_idx.get('Sell_Order_Lock_Time')

col_MoveThreshold = col_idx.get('MoveThreshold')

def generate_signature(timestamp: int, api_key: str, secret_key: str) -> str:
    base_string = "{}{}".format(api_key, timestamp).encode("utf-8")
    signature = hmac.new(
        base64.b64decode(secret_key), 
            base_string, 
            hashlib.sha256
        ).digest()
    return base64.b64encode(signature).decode("utf-8")
    
def get_headers(api_key: str, secret_key: str) -> Dict[str, str]: 
    # Generate unique nonce for each request
    nonce = str(uuid.uuid4())
    timestamp = round(time.time() * 1000)
    signature = generate_signature(timestamp, api_key, secret_key)
    
    return {
        "X-PCK": api_key,
        "X-Nonce": nonce,
        "X-Stamp": str(timestamp),
        "X-Signature": signature,
        "Content-Type": "application/json"
    }

BTCTURK_HFT_REST_API_BASE_URL = "http://quantx-transaction-api.btcturkglobal.com"



def fetch_my_trades(symbol, type: str):
    url = f"{BTCTURK_HFT_REST_API_BASE_URL}/v1/trades"
    url += f"?pairSymbol={symbol}&pageSize=10&createdAtBefore={datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}"

    if type == "TAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_taker_test
        secret_key = APIkeys.BTCTURK_secret_key_taker_test
    elif type == "MAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_maker_test
        secret_key = APIkeys.BTCTURK_secret_key_maker_test
    elif type == "MANUAL_TEST":
        api_key = APIkeys.BTCTURK_api_key_manual_test
        secret_key = APIkeys.BTCTURK_secret_key_manual_test
    elif type == "TAKER":
        api_key = APIkeys.BTCTURK_api_key_taker
        secret_key = APIkeys.BTCTURK_secret_key_taker
    elif type == "MAKER":
        api_key = APIkeys.BTCTURK_api_key_maker
        secret_key = APIkeys.BTCTURK_secret_key_maker
    elif type == "MANUAL":
        api_key = APIkeys.BTCTURK_api_key_manual
        secret_key = APIkeys.BTCTURK_secret_key_manual
    else:
        logging.error(f"Invalid type: {type}")
        return None

    headers = get_headers(api_key, secret_key)
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        logging.error("Authentication failed - check your API keys")
        return None
    elif response.status_code != 200:
        logging.error(f"Error: HTTP {response.status_code}")
        logging.error(f"Response: {response.text}")
        return None
    elif response.status_code == 200:
        try:
            data = response.json()
            return data
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

def fetch_open_orders(symbol, type: str):
    symbol = symbol + "TRY" if not symbol.endswith("TRY") else symbol
    url = f"{BTCTURK_HFT_REST_API_BASE_URL}/v1/orders?pairSymbol={symbol}&orderIdAfter=0&status=Untouched&status=Partial"
   
    if type == "TAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_taker_test
        secret_key = APIkeys.BTCTURK_secret_key_taker_test
    elif type == "MAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_maker_test
        secret_key = APIkeys.BTCTURK_secret_key_maker_test
    elif type == "MANUAL_TEST":
        api_key = APIkeys.BTCTURK_api_key_manual_test
        secret_key = APIkeys.BTCTURK_secret_key_manual_test
    elif type == "TAKER":
        api_key = APIkeys.BTCTURK_api_key_taker
        secret_key = APIkeys.BTCTURK_secret_key_taker
    elif type == "MAKER":
        api_key = APIkeys.BTCTURK_api_key_maker
        secret_key = APIkeys.BTCTURK_secret_key_maker
    elif type == "MANUAL":
        api_key = APIkeys.BTCTURK_api_key_manual
        secret_key = APIkeys.BTCTURK_secret_key_manual
    else:
        logging.error(f"Invalid type: {type}")
        return None

    headers = get_headers(api_key, secret_key)
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        logging.error("Authentication failed - check your API keys")
        return None
    elif response.status_code != 200:
        logging.error(f"Error: HTTP {response.status_code}")
        logging.error(f"Response: {response.text}")
        return None
    elif response.status_code == 200:
        try:
            data = response.json()
            return data
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

def fetch_BTCTURK_balance(type: str):
    url = f"{BTCTURK_HFT_REST_API_BASE_URL}/v1/account-wallets"
    
    if type == "TAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_taker_test
        secret_key = APIkeys.BTCTURK_secret_key_taker_test
    elif type == "MAKER_TEST":
        api_key = APIkeys.BTCTURK_api_key_maker_test
        secret_key = APIkeys.BTCTURK_secret_key_maker_test
    elif type == "MANUAL_TEST":
        api_key = APIkeys.BTCTURK_api_key_manual_test
        secret_key = APIkeys.BTCTURK_secret_key_manual_test
    elif type == "TAKER":
        api_key = APIkeys.BTCTURK_api_key_taker
        secret_key = APIkeys.BTCTURK_secret_key_taker
    elif type == "MAKER":
        api_key = APIkeys.BTCTURK_api_key_maker
        secret_key = APIkeys.BTCTURK_secret_key_maker
    elif type == "MANUAL":
        api_key = APIkeys.BTCTURK_api_key_manual
        secret_key = APIkeys.BTCTURK_secret_key_manual
    else:
        logging.error(f"Invalid type: {type}")
        return None

    headers = get_headers(api_key, secret_key)
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401:
        logging.error("Authentication failed - check your API keys")
        return None
    elif response.status_code != 200:
        logging.error(f"Error: HTTP {response.status_code}")
        logging.error(f"Response: {response.text}")
        return None
    elif response.status_code == 200:
        try:
            data = response.json()
            
            # Parse the wallet data
            if isinstance(data, list):
                balances = {}
                
                for wallet in data:
                    currency = wallet.get('currencySymbol', '')
                    total_fund = float(wallet.get('totalFund', 0))
                    available_balance = float(wallet.get('availableBalance', 0))
                    order_fund = float(wallet.get('orderFund', 0))
                    request_fund = float(wallet.get('requestFund', 0))
                    account_id = wallet.get('accountId', '')
                    
                    # Store balance info for this currency
                    balances[currency] = {
                        'accountID': account_id,
                        'total': total_fund,
                        'available': available_balance,
                        'order': order_fund,
                        'request': request_fund
                    }

                return balances
            else:
                logging.error("Unexpected response format")
                return None
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

#BTCTURK_ws_uri = "wss://ws-feed-pro.btcturk.com"
BTCTURK_HFT_uri = "ws://quantx-transaction-api.btcturkglobal.com:8080"

# Split symbols into 4 groups
def split_symbols(symbols, num_groups=4):
    """Split symbols into equal groups for parallel websocket connections"""
    n = len(symbols)
    group_size = n // num_groups
    remainder = n % num_groups
    
    groups = []
    start = 0
    for i in range(num_groups):
        end = start + group_size + (1 if i < remainder else 0)
        groups.append(symbols[start:end])
        start = end
    
    return groups



TOTAL_PROCESSORS = 1

# Symbol distribution across cores - no coin_host_mapping.json dependency
# Each core will connect to both hosts and divide symbols equally
# Use default split for symbol distribution across cores
binance_symbol_groups = split_symbols(binance_symbol_list, TOTAL_PROCESSORS)

BINANCE_ws_uris_ticker = {}
BINANCE_ws_uris_depth = {}

for i in range(TOTAL_PROCESSORS):
    BINANCE_ws_uris_ticker[i] = "wss://fstream.binance.com/stream?streams=" + "/".join(
        [f"{symbol.lower()}@bookTicker" for symbol in binance_symbol_groups[i]])
    BINANCE_ws_uris_depth[i] = "wss://fstream.binance.com/stream?streams=" + "/".join(
        [f"{symbol.lower()}@depth5@100ms" for symbol in binance_symbol_groups[i]])

# Split BTCTURK symbols into 3 groups for parallel websockets
BinanceTR_symbol_groups = split_symbols(BinanceTR_symbol_list, TOTAL_PROCESSORS)

# Load CS instruments mapping (instrument_id -> symbol)
def load_cs_instruments_mapping():
    """Load CS instruments mapping from CS_btcturk_instruments.json"""
    try:
        with open("CS_btcturk_instruments.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            # Create mapping: instrument_id -> (code, type, exchange_id, exchange_code, exchange_name, hosts)
            instrument_map = {}
            for item in data:
                instrument_id = item.get("instrument_id")
                code = item.get("code", "")
                inst_type = item.get("type", "")
                exchange_id = item.get("exchange_id")
                exchange_code = item.get("exchange_code", "")
                exchange_name = item.get("exchange_name", "")
                hosts = item.get("hosts", [])
                if instrument_id:
                    instrument_map[instrument_id] = {
                        "code": code,
                        "type": inst_type,
                        "exchange_id": exchange_id,
                        "exchange_code": exchange_code,
                        "exchange_name": exchange_name,
                        "hosts": hosts
                    }
            logging.info(f"Loaded CS instruments mapping: {len(instrument_map)} instruments")
            return instrument_map
    except FileNotFoundError:
        logging.warning("CS_btcturk_instruments.json not found.")
        return {}
    except Exception as e:
        logging.warning(f"Error loading CS instruments mapping: {e}")
        return {}

# Load CS instruments mapping
cs_instruments_map = load_cs_instruments_mapping()

# CS websocket configuration
# Each core connects to both hosts automatically (determined at runtime from CS_INSTRUMENTS_MAP)
# Hosts are no longer statically configured - they're discovered and distributed dynamically
CS_WS_HOSTS = {}  # Keep for backward compatibility, but not used
for i in range(1, TOTAL_PROCESSORS + 1):
    CS_WS_HOSTS[i] = []  # Empty - hosts determined dynamically at runtime

logging.info("CS_WS_HOSTS initialized (hosts determined dynamically at runtime)")


async def calculate_amount_precision_for_script(exchange_BINANCE, exchange_BTCTURK, script_symbols):
    """Calculate amount precision for only the script's symbols"""
    try:
        precision_values = []
        btcturk_precision_values = []
        
        for base_symbol in script_symbols:
            if base_symbol != "USDT":
                logging.info(f"Calculating amount precision for {base_symbol}")
                if base_symbol == "LUNA":
                    base_symbol_binance = "LUNA2"
                    logging.info(f"Symbol {base_symbol} is exceptional, mapped to {base_symbol_binance} for Binance")
                elif base_symbol == "BEAM":
                    base_symbol_binance = "BEAMX"
                    logging.info(f"Symbol {base_symbol} is exceptional, mapped to {base_symbol_binance} for Binance")
                elif base_symbol in ["PEPE", "BONK", "SHIB", "FLOKI"]:
                    base_symbol_binance = f"1000{base_symbol}"
                    logging.info(f"Symbol {base_symbol} is exceptional, mapped to {base_symbol_binance} for Binance")
                else:
                    logging.info(f"Symbol {base_symbol} is not exceptional")
                    base_symbol_binance = base_symbol

                binance_symbol = f"{base_symbol_binance}/USDT"
                btcturk_symbol = f"{base_symbol}/TRY"

                binance_market_info = exchange_BINANCE.market(binance_symbol)  
                btcturk_market_info = exchange_BTCTURK.market(btcturk_symbol)
                
                # Get precision from both exchanges
                binance_precision = abs(math.log10(binance_market_info['precision']['amount']))
                btcturk_precision = abs(math.log10(btcturk_market_info['precision']['amount']))
                
                # Use the minimum precision between the two exchanges
                precision_values.append(min(binance_precision, btcturk_precision))
                btcturk_precision_values.append(btcturk_precision)
            else:   
                precision_values.append(0)
                btcturk_precision_values.append(0)
        
        return precision_values, btcturk_precision_values
    except Exception as e:
        logging.error(f"Error in calculate_amount_precision_for_script: {e}")
        # Return default precision values
        return [0] * len(script_symbols), [0] * len(script_symbols)

async def fetch_BTCTURK_price_precisions_for_script(exchange_BTCTURK, script_symbols):
    """Fetch BTCTURK price precisions for only the script's symbols"""
    try:
        price_precision_values = []
        
        for base_symbol in script_symbols:
            btcturk_symbol = f"{base_symbol}/TRY"

            # Fetch markets directly  
            btcturk_market_info = exchange_BTCTURK.market(btcturk_symbol)
            
            # Get precision from BTCTURK
            btcturk_price_precision = abs(math.log10(btcturk_market_info['precision']['price']))
            price_precision_values.append(btcturk_price_precision)
        
        return price_precision_values
    except Exception as e:
        logging.error(f"Error in fetch_BTCTURK_price_precisions_for_script: {e}")
        # Return default precision values
        return [0] * len(script_symbols)


# CryptoStruct Trading Adapter Configuration
CS_TRADING_ADAPTER_HOST = "3.127.121.170"  # Trading Adapter Public IP
CS_TRADING_ADAPTER_PORT = 3000
CS_TRADING_ADAPTER_WS_ENDPOINT = "/api/v2"  # Trading Adapter WebSocket endpoint

# Client identification for Trading Adapter
CLIENT_ORG = "CryptoStruct"
CLIENT_APP_NAME = "arbit_core_taker"
CLIENT_APP_VER = "1.0.0"
CLIENT_PROCESS_ID = "quantxai-algoX"  # Process ID for Trading Adapter


TEST_MODE = r.get('TEST_MODE')
if TEST_MODE is not None:
    TEST_MODE = int(TEST_MODE.decode('utf-8')) if isinstance(TEST_MODE, bytes) else int(TEST_MODE)
else:
    TEST_MODE = 1
    r.set('TEST_MODE', TEST_MODE)
    logging.info(f"TEST_MODE set to 1 by default")

if TEST_MODE == 1:
    ACCOUNT_ID = 201  # Account ID from UI (Binance Test(ALPER)account)
else:
    ACCOUNT_ID = 202  # Account ID from UI (Binance Kerim account)

# CS Order timeout (in seconds)
CS_ORDER_PLACE_RESPONSE_TIMEOUT = 1.0  # Timeout for placeResponse
CS_ORDER_FILL_TIMEOUT = 1.0  # Timeout for fill message

# CS WebSocket constants (aligned with CS_subscribe_instruments.py)
CS_ORG = "Quantx"
CS_APP_NAME = "WSClient"
CS_APP_VER = "1.0.0"
CS_HEARTBEAT_SEC = 20  # aiohttp ws_connect heartbeat (ping)
CS_PING_TIMEOUT_SEC = 10  # Timeout for ping
CS_RETRY_BASE_DELAY = 1.0
CS_RETRY_MAX_DELAY = 30.0

CS_SUB_OPTIONS = {
    "depthTopic": False,
    "tradesTopic": False,
    "topOfBookTopic": True,
    "indexPriceTopic": False,
    "markPriceTopic": False,
    "fundingRateTopic": False,
    "liquidationsTopic": False,
    "topOfBookCoalescing": False
}
