#!/usr/bin/env python3
"""
Extract common symbols between Binance TR and Binance Futures,
and generate JSON with price and amount precision for Binance TR symbols.

Uses Binance TR API: GET /open/v1/common/symbols
Reference: https://www.binance.tr/apidocs/#get-all-supported-trading-symbol
Uses CS_all_instruments.json for Binance Futures symbols
"""

import requests
import json
import logging
from typing import Dict, List, Set, Optional
import time
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API endpoints
BINANCE_TR_API_BASE = "https://www.binance.tr/open/v1"
BINANCE_FUTURES_API_BASE = "https://fapi.binance.com/fapi/v1"

def fetch_binance_tr_symbols() -> List[Dict]:
    """
    Fetch all supported trading symbols from Binance TR API.
    Endpoint: GET /open/v1/common/symbols
    """
    try:
        url = f"{BINANCE_TR_API_BASE}/common/symbols"
        logging.info(f"Fetching Binance TR symbols from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check response code
        if data.get("code") != 0:
            error_msg = data.get("msg", "Unknown error")
            logging.error(f"Binance TR API returned error: code={data.get('code')}, msg={error_msg}")
            return []
        
        symbols = data.get("data", {}).get("list", [])
        logging.info(f"Fetched {len(symbols)} symbols from Binance TR")
        return symbols
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Binance TR symbols: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching Binance TR symbols: {e}")
        return []

def load_binance_futures_symbols() -> List[Dict]:
    """
    Load Binance Futures symbols from CS_all_instruments.json.
    """
    try:
        if not os.path.exists("CS_all_instruments.json"):
            logging.error("CS_all_instruments.json not found!")
            return []
        
        with open("CS_all_instruments.json", "r") as f:
            instruments = json.load(f)
        
        # Filter for Binance Futures (binance_swap)
        futures_instruments = [
            inst for inst in instruments 
            if inst.get("exchange_code") == "binance_swap" and inst.get("type") == "perpetual"
        ]
        
        logging.info(f"Loaded {len(futures_instruments)} Binance Futures instruments from CS_all_instruments.json")
        return futures_instruments
    except Exception as e:
        logging.error(f"Error loading Binance Futures symbols: {e}")
        return []

def extract_base_symbol(symbol: str, quote: str = "TRY") -> str:
    """
    Extract base symbol from trading pair.
    Example: "ADA_TRY" -> "ADA", "ETHTRY" -> "ETH"
    """
    # Handle underscore format: "ADA_TRY"
    if "_" in symbol:
        return symbol.split("_")[0]
    # Handle concatenated format: "ETHTRY"
    if symbol.endswith(quote):
        return symbol[:-len(quote)]
    return symbol

def find_common_symbols(binance_tr_symbols: List[Dict], 
                        binance_futures_instruments: List[Dict]) -> Dict:
    """
    Find common symbols between Binance TR and Binance Futures.
    Returns a dictionary with common symbols and their information.
    Handles exceptional symbol mappings (e.g., LUNA_TRY -> LUNA2USDT).
    """
    # Define exceptional symbol mappings: TR_base_symbol -> Futures_code
    exceptional_mappings = {
        "LUNA": "LUNA2",      # LUNA_TRY -> LUNA2USDT
        "PEPE": "1000PEPE",   # PEPE_TRY -> 1000PEPEUSDT
        "BONK": "1000BONK",   # BONK_TRY -> 1000BONKUSDT
        "SHIB": "1000SHIB",   # SHIB_TRY -> 1000SHIBUSDT
        "FLOKI": "1000FLOKI", # FLOKI_TRY -> 1000FLOKIUSDT
        "LUNC" : "1000LUNC",      # LUNC_TRY -> LUNCUSDT
        "XEC" : "1000XEC",        # XEC_TRY -> 1000XECUSDT
    }
    
    # Extract TR symbols (quote currency is TRY)
    tr_symbols_map = {}
    for symbol_info in binance_tr_symbols:
        symbol = symbol_info.get("symbol", "")
        quote_asset = symbol_info.get("quoteAsset", "")
        
        # Handle both "ADA_TRY" and "ETHTRY" formats
        if quote_asset == "TRY" or symbol.endswith("TRY") or "_TRY" in symbol:
            base_symbol = extract_base_symbol(symbol, "TRY")
            # Handle special cases where TR symbol starts with "1000" (e.g., "1000SATS_TRY" -> "SATS")
            original_base = base_symbol
            if base_symbol.startswith("1000"):
                base_symbol = base_symbol[4:]  # Remove "1000" prefix
            tr_symbols_map[base_symbol] = {
                "binance_tr_symbol": symbol,
                "symbol_info": symbol_info,
                "original_base": original_base  # Keep original for reference
            }
    
    logging.info(f"Found {len(tr_symbols_map)} Binance TR symbols (TRY pairs)")
    
    # Extract Futures symbols from CS_all_instruments.json
    futures_symbols_map = {}
    futures_code_to_base = {}  # Map futures code to base symbol for reverse lookup
    for instrument in binance_futures_instruments:
        code = instrument.get("code", "")  # e.g., "SKLUSDT", "GRTUSDT", "LUNA2USDT"
        instrument_id = instrument.get("instrument_id")
        
        if code and code.endswith("USDT"):
            # Extract base symbol from code (remove USDT)
            base_symbol = code[:-4]  # Remove "USDT"
            
            # Store original code for exceptional mappings
            futures_code_to_base[code] = base_symbol
            
            # Handle special cases like 1000PEPE, 1000BONK, etc.
            if base_symbol.startswith("1000"):
                base_symbol = base_symbol[4:]  # Remove "1000" prefix
            
            futures_symbols_map[base_symbol] = {
                "binance_futures_symbol": code,
                "instrument_id": instrument_id,
                "instrument_info": instrument
            }
    
    logging.info(f"Found {len(futures_symbols_map)} Binance Futures symbols (USDT pairs)")
    
    # Find common base symbols (direct matches)
    common_base_symbols = set(tr_symbols_map.keys()) & set(futures_symbols_map.keys())
    
    # Handle exceptional mappings
    exceptional_matched = {}
    for tr_base, futures_base in exceptional_mappings.items():
        if tr_base in tr_symbols_map:
            # Construct the full futures code (add USDT suffix)
            futures_code = f"{futures_base}USDT"
            # Find the instrument for this exceptional futures code
            for instrument in binance_futures_instruments:
                if instrument.get("code") == futures_code:
                    exceptional_matched[tr_base] = {
                        "binance_futures_symbol": futures_code,
                        "instrument_id": instrument.get("instrument_id"),
                        "instrument_info": instrument
                    }
                    logging.info(f"Matched exceptional symbol: {tr_base}_TRY -> {futures_code}")
                    break
    
    logging.info(f"Found {len(common_base_symbols)} direct common symbols")
    logging.info(f"Found {len(exceptional_matched)} exceptional symbol mappings")
    total_common = len(common_base_symbols) + len(exceptional_matched)
    logging.info(f"Total common symbols: {total_common}")
    
    # Build result dictionary
    result = {
        "common_symbols": [],
        "binance_tr_only": [],
        "binance_futures_only": []
    }
    
    # Add direct matches
    for base_symbol in sorted(common_base_symbols):
        tr_data = tr_symbols_map[base_symbol]
        futures_data = futures_symbols_map[base_symbol]
        
        common_symbol = {
            "base_symbol": base_symbol,
            "binance_tr_symbol": tr_data["binance_tr_symbol"],
            "binance_futures_symbol": futures_data["binance_futures_symbol"],
            "binance_futures_instrument_id": futures_data["instrument_id"],
            "binance_tr_info": tr_data["symbol_info"],
            "binance_futures_info": futures_data["instrument_info"],
            "is_exceptional": False
        }
        
        result["common_symbols"].append(common_symbol)
    
    # Add exceptional matches
    for tr_base, futures_data in exceptional_matched.items():
        if tr_base not in common_base_symbols:  # Only add if not already matched directly
            tr_data = tr_symbols_map[tr_base]
            
            common_symbol = {
                "base_symbol": tr_base,
                "binance_tr_symbol": tr_data["binance_tr_symbol"],
                "binance_futures_symbol": futures_data["binance_futures_symbol"],
                "binance_futures_instrument_id": futures_data["instrument_id"],
                "binance_tr_info": tr_data["symbol_info"],
                "binance_futures_info": futures_data["instrument_info"],
                "is_exceptional": True
            }
            
            result["common_symbols"].append(common_symbol)
    
    # Sort common symbols by base_symbol
    result["common_symbols"].sort(key=lambda x: x["base_symbol"])
    
    # Find TR-only symbols (excluding exceptional ones that were matched)
    tr_only = set(tr_symbols_map.keys()) - set(futures_symbols_map.keys()) - set(exceptional_matched.keys())
    for base_symbol in sorted(tr_only):
        result["binance_tr_only"].append({
            "base_symbol": base_symbol,
            "binance_tr_symbol": tr_symbols_map[base_symbol]["binance_tr_symbol"]
        })
    
    # Find Futures-only symbols
    futures_only = set(futures_symbols_map.keys()) - set(tr_symbols_map.keys())
    for base_symbol in sorted(futures_only):
        result["binance_futures_only"].append({
            "base_symbol": base_symbol,
            "binance_futures_symbol": futures_symbols_map[base_symbol]["binance_futures_symbol"],
            "instrument_id": futures_symbols_map[base_symbol]["instrument_id"]
        })
    
    return result

def fetch_binance_futures_exchange_info() -> Dict[str, Dict]:
    """
    Fetch exchange info from Binance Futures API to get precision data.
    Returns a dictionary mapping symbol (e.g., "BTCUSDT") to its precision info.
    """
    try:
        url = f"{BINANCE_FUTURES_API_BASE}/exchangeInfo"
        logging.info(f"Fetching Binance Futures exchange info from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        symbols_info = {}
        for symbol_info in data.get("symbols", []):
            symbol = symbol_info.get("symbol", "")
            if not symbol:
                continue
            
            # Extract amount precision from LOT_SIZE filter
            amount_precision = None
            step_size = None
            min_qty = None
            max_qty = None
            
            for filter_item in symbol_info.get("filters", []):
                if filter_item.get("filterType") == "LOT_SIZE":
                    step_size_str = filter_item.get("stepSize", "0")
                    min_qty_str = filter_item.get("minQty", "0")
                    max_qty_str = filter_item.get("maxQty", "0")
                    
                    try:
                        step_size = float(step_size_str)
                        min_qty = float(min_qty_str)
                        max_qty = float(max_qty_str)
                        
                        # Calculate amount precision from step size
                        if "." in str(step_size):
                            step_str = str(step_size).rstrip("0")
                            if "." in step_str:
                                amount_precision = len(step_str.split(".")[1])
                            else:
                                amount_precision = 0
                        else:
                            amount_precision = 0
                    except (ValueError, TypeError):
                        pass
                    break
            
            symbols_info[symbol] = {
                "amount_precision": amount_precision,
                "step_size": step_size,
                "min_qty": min_qty,
                "max_qty": max_qty
            }
        
        logging.info(f"Fetched precision info for {len(symbols_info)} Binance Futures symbols")
        return symbols_info
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Binance Futures exchange info: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error fetching Binance Futures exchange info: {e}")
        return {}

def extract_precision_from_binance_tr_filters(filters: List[Dict]) -> Dict:
    """
    Extract price and quantity precision from Binance TR symbol filters.
    Uses the actual Binance TR API response format.
    """
    precision = {
        "price_precision": 8,  # Default
        "amount_precision": 8,  # Default
        "base_precision": 8,  # From basePrecision field
        "quote_precision": 8,  # From quotePrecision field
        "min_price": None,
        "max_price": None,
        "tick_size": None,
        "min_qty": None,
        "max_qty": None,
        "step_size": None,
        "min_notional": None
    }
    
    if not filters:
        return precision
    
    for filter_item in filters:
        filter_type = filter_item.get("filterType", "")
        
        if filter_type == "PRICE_FILTER":
            min_price = filter_item.get("minPrice", "0")
            max_price = filter_item.get("maxPrice", "0")
            tick_size = filter_item.get("tickSize", "0")
            
            try:
                precision["min_price"] = float(min_price)
                precision["max_price"] = float(max_price)
                precision["tick_size"] = float(tick_size)
                
                # Calculate price precision from tick size
                if "." in str(tick_size):
                    tick_str = str(tick_size).rstrip("0")
                    if "." in tick_str:
                        precision["price_precision"] = len(tick_str.split(".")[1])
                    else:
                        precision["price_precision"] = 0
                else:
                    precision["price_precision"] = 0
            except (ValueError, TypeError):
                pass
        
        elif filter_type == "LOT_SIZE":
            min_qty = filter_item.get("minQty", "0")
            max_qty = filter_item.get("maxQty", "0")
            step_size = filter_item.get("stepSize", "0")
            
            try:
                precision["min_qty"] = float(min_qty)
                precision["max_qty"] = float(max_qty)
                precision["step_size"] = float(step_size)
                
                # Calculate amount precision from step size
                if "." in str(step_size):
                    step_str = str(step_size).rstrip("0")
                    if "." in step_str:
                        precision["amount_precision"] = len(step_str.split(".")[1])
                    else:
                        precision["amount_precision"] = 0
                else:
                    precision["amount_precision"] = 0
            except (ValueError, TypeError):
                pass
        
        elif filter_type == "NOTIONAL":
            min_notional = filter_item.get("minNotional", "0")
            try:
                precision["min_notional"] = float(min_notional)
            except (ValueError, TypeError):
                pass
    
    return precision

def generate_precision_json(common_symbols_data: Dict, futures_exchange_info: Dict[str, Dict], output_file: str = "binance_tr_precision.json"):
    """
    Generate JSON file with price and amount precision for Binance TR symbols.
    Uses actual Binance TR API precision data and adds Binance Futures amount precision.
    """
    precision_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Binance TR API - GET /open/v1/common/symbols",
        "futures_source": "Binance Futures API - GET /fapi/v1/exchangeInfo",
        "reference": "https://www.binance.tr/apidocs/#get-all-supported-trading-symbol",
        "symbols": []
    }
    
    for symbol_data in common_symbols_data["common_symbols"]:
        base_symbol = symbol_data["base_symbol"]
        binance_tr_symbol = symbol_data["binance_tr_symbol"]
        binance_futures_symbol = symbol_data.get("binance_futures_symbol", "")
        tr_info = symbol_data["binance_tr_info"]
        
        # Extract precision from Binance TR filters
        precision = extract_precision_from_binance_tr_filters(tr_info.get("filters", []))
        
        # Get base and quote precision from symbol info
        base_precision = tr_info.get("basePrecision", precision["base_precision"])
        quote_precision = tr_info.get("quotePrecision", precision["quote_precision"])
        
        # Get Binance Futures amount precision
        binance_futures_amount_precision = None
        binance_futures_step_size = None
        binance_futures_min_qty = None
        binance_futures_max_qty = None
        
        if binance_futures_symbol and binance_futures_symbol in futures_exchange_info:
            futures_info = futures_exchange_info[binance_futures_symbol]
            binance_futures_amount_precision = futures_info.get("amount_precision")
            binance_futures_step_size = futures_info.get("step_size")
            binance_futures_min_qty = futures_info.get("min_qty")
            binance_futures_max_qty = futures_info.get("max_qty")
        
        symbol_precision = {
            "base_symbol": base_symbol,
            "binance_tr_symbol": binance_tr_symbol,
            "binance_futures_symbol": symbol_data.get("binance_futures_symbol", ""),
            "binance_futures_instrument_id": symbol_data.get("binance_futures_instrument_id"),
            "base_asset": tr_info.get("baseAsset", base_symbol),
            "quote_asset": tr_info.get("quoteAsset", "TRY"),
            "base_precision": base_precision,
            "quote_precision": quote_precision,
            "price_precision": precision["price_precision"],
            "amount_precision": precision["amount_precision"],
            "binance_futures_amount_precision": binance_futures_amount_precision,
            "price_filters": {
                "min_price": precision["min_price"],
                "max_price": precision["max_price"],
                "tick_size": precision["tick_size"]
            },
            "lot_size_filters": {
                "min_qty": precision["min_qty"],
                "max_qty": precision["max_qty"],
                "step_size": precision["step_size"]
            },
            "binance_futures_lot_size_filters": {
                "min_qty": binance_futures_min_qty,
                "max_qty": binance_futures_max_qty,
                "step_size": binance_futures_step_size
            },
            "min_notional": precision["min_notional"],
            "status": {
                "spot_trading_enable": tr_info.get("spotTradingEnable", 0),
                "margin_trading_enable": tr_info.get("marginTradingEnable", 0),
                "iceberg_enable": tr_info.get("icebergEnable", 0),
                "oco_enable": tr_info.get("ocoEnable", 0)
            },
            "order_types": tr_info.get("orderTypes", []),
            "type": tr_info.get("type", 1)  # 1 - Main, 2 - Next
        }
        
        precision_data["symbols"].append(symbol_precision)
    
    # Write to JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(precision_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Generated precision JSON file: {output_file} with {len(precision_data['symbols'])} symbols")
    return precision_data

def generate_common_symbols_json(common_symbols_data: Dict, output_file: str = "common_symbols_binance_tr_futures.json"):
    """
    Generate JSON file with common symbols information.
    """
    output_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Binance TR API - GET /open/v1/common/symbols",
        "futures_source": "CS_all_instruments.json",
        "reference": "https://www.binance.tr/apidocs/#get-all-supported-trading-symbol",
        "summary": {
            "total_common_symbols": len(common_symbols_data["common_symbols"]),
            "binance_tr_only": len(common_symbols_data["binance_tr_only"]),
            "binance_futures_only": len(common_symbols_data["binance_futures_only"])
        },
        "common_symbols": common_symbols_data["common_symbols"],
        "binance_tr_only": common_symbols_data["binance_tr_only"],
        "binance_futures_only": common_symbols_data["binance_futures_only"]
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Generated common symbols JSON file: {output_file}")
    return output_data

def main():
    """Main function to extract common symbols and generate precision JSON."""
    logging.info("=" * 80)
    logging.info("Extracting common symbols between Binance TR and Binance Futures")
    logging.info("Using Binance TR API: GET /open/v1/common/symbols")
    logging.info("Using CS_all_instruments.json for Binance Futures")
    logging.info("=" * 80)
    
    # Fetch symbols from APIs
    logging.info("\nStep 1: Fetching/loading symbols...")
    binance_tr_symbols = fetch_binance_tr_symbols()
    binance_futures_instruments = load_binance_futures_symbols()
    
    if not binance_tr_symbols:
        logging.error("Failed to fetch Binance TR symbols. Exiting.")
        return
    
    if not binance_futures_instruments:
        logging.error("Failed to load Binance Futures symbols from CS_all_instruments.json. Exiting.")
        return
    
    # Find common symbols
    logging.info("\nStep 2: Finding common symbols...")
    common_symbols_data = find_common_symbols(binance_tr_symbols, binance_futures_instruments)
    
    # Fetch Binance Futures exchange info for precision data
    logging.info("\nStep 3: Fetching Binance Futures exchange info for precision data...")
    futures_exchange_info = fetch_binance_futures_exchange_info()
    
    # Generate common symbols JSON
    logging.info("\nStep 4: Generating common symbols JSON...")
    generate_common_symbols_json(common_symbols_data, "common_symbols_binance_tr_futures.json")
    
    # Generate precision JSON
    logging.info("\nStep 5: Generating precision JSON for Binance TR symbols...")
    precision_data = generate_precision_json(common_symbols_data, futures_exchange_info, "binance_tr_precision.json")
    
    # Generate list of common Binance TR symbols with TRY suffix
    logging.info("\nStep 6: Generating Binance TR symbols list...")
    binance_tr_symbols_list = []
    for symbol_data in common_symbols_data["common_symbols"]:
        binance_tr_symbol = symbol_data["binance_tr_symbol"]
        # Ensure format is with underscore (e.g., "ADA_TRY")
        if "_" not in binance_tr_symbol and binance_tr_symbol.endswith("TRY"):
            # Convert "ADATRY" to "ADA_TRY" format
            base = binance_tr_symbol[:-3]
            binance_tr_symbol = f"{base}_TRY"
        binance_tr_symbols_list.append(binance_tr_symbol)
    
    # Sort the list
    binance_tr_symbols_list.sort()
    
    # Save as JSON array
    with open("binance_tr_common_symbols.json", "w", encoding="utf-8") as f:
        json.dump(binance_tr_symbols_list, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Generated binance_tr_common_symbols.json with {len(binance_tr_symbols_list)} symbols")
    
    # Print summary
    logging.info("\n" + "=" * 80)
    logging.info("SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Total common symbols: {len(common_symbols_data['common_symbols'])}")
    logging.info(f"Binance TR only: {len(common_symbols_data['binance_tr_only'])}")
    logging.info(f"Binance Futures only: {len(common_symbols_data['binance_futures_only'])}")
    logging.info(f"\nGenerated files:")
    logging.info(f"  - common_symbols_binance_tr_futures.json")
    logging.info(f"  - binance_tr_precision.json")
    logging.info(f"  - binance_tr_common_symbols.json (list format)")
    logging.info("=" * 80)
    
    # Print first few common symbols as example
    if common_symbols_data["common_symbols"]:
        logging.info("\nFirst 10 common symbols:")
        for symbol in common_symbols_data["common_symbols"][:10]:
            logging.info(f"  {symbol['base_symbol']}: {symbol['binance_tr_symbol']} <-> {symbol['binance_futures_symbol']} (ID: {symbol.get('binance_futures_instrument_id')})")
    
    # Print first few symbols from the list
    logging.info(f"\nFirst 10 symbols in binance_tr_common_symbols.json:")
    for symbol in binance_tr_symbols_list[:10]:
        logging.info(f"  {symbol}")

if __name__ == "__main__":
    main()
