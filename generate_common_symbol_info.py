#!/usr/bin/env python3
"""
Generate a single JSON file with common symbol information.
Fetches data directly from Binance TR API and CS API.
Includes only: base_symbol, binance_tr_symbol, binance_futures_symbol, 
CS_instrument_id, binance_tr_price_precision, binance_tr_price_step_size, 
binance_tr_amount_precision.
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
CS_MASTERDATA = "http://masterdata.cryptostruct.com"
CS_INSTRUMENTS_URL = f"{CS_MASTERDATA}/api/instruments"

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

def load_cs_instruments_from_json(json_file: str = "CS_all_instruments.json") -> List[Dict]:
    """
    Load CS instruments from JSON file as fallback when API is unavailable.
    Converts instrument_id to id to match API response format.
    """
    try:
        if not os.path.exists(json_file):
            logging.error(f"Fallback JSON file not found: {json_file}")
            return []
        
        logging.info(f"Loading CS instruments from {json_file}...")
        with open(json_file, "r", encoding="utf-8") as f:
            instruments = json.load(f)
        
        # Convert instrument_id to id to match API response format
        # Also filter for USDT pairs and perpetual type
        usdt_instruments = []
        for inst in instruments:
            code = inst.get("code", "")
            inst_type = inst.get("type", "")
            
            if code.upper().endswith("USDT") and inst_type == "perpetual":
                # Convert instrument_id to id for compatibility
                converted_inst = inst.copy()
                if "instrument_id" in converted_inst:
                    converted_inst["id"] = converted_inst.pop("instrument_id")
                usdt_instruments.append(converted_inst)
        
        logging.info(f"Loaded {len(usdt_instruments)} Binance Futures USDT instruments from {json_file}")
        return usdt_instruments
    except FileNotFoundError:
        logging.error(f"Fallback JSON file not found: {json_file}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON file {json_file}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error loading from {json_file}: {e}")
        return []

def fetch_cs_instruments() -> List[Dict]:
    """
    Fetch Binance Futures instruments from CS API.
    Falls back to CS_all_instruments.json if API connection fails.
    """
    try:
        params = {
            "exchange_id": 8,  # Binance Perp
            "state": "open",
        }
        logging.info(f"Fetching CS instruments from: {CS_INSTRUMENTS_URL}")
        response = requests.get(CS_INSTRUMENTS_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        instruments = data.get("data", [])
        
        # Filter for USDT pairs only
        usdt_instruments = [
            inst for inst in instruments 
            if inst.get("code", "").upper().endswith("USDT") and inst.get("type") == "perpetual"
        ]
        
        logging.info(f"Fetched {len(usdt_instruments)} Binance Futures USDT instruments from CS API")
        return usdt_instruments
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error fetching CS instruments from API: {e}")
        logging.info("Attempting to load from CS_all_instruments.json as fallback...")
        return load_cs_instruments_from_json()
    except Exception as e:
        logging.warning(f"Unexpected error fetching CS instruments: {e}")
        logging.info("Attempting to load from CS_all_instruments.json as fallback...")
        return load_cs_instruments_from_json()

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

def extract_precision_from_filters(filters: List[Dict]) -> Dict:
    """
    Extract price and amount precision from Binance TR symbol filters.
    """
    precision = {
        "price_precision": 8,  # Default
        "amount_precision": 8,  # Default
        "tick_size": None,
        "step_size": None
    }
    
    if not filters:
        return precision
    
    for filter_item in filters:
        filter_type = filter_item.get("filterType", "")
        
        if filter_type == "PRICE_FILTER":
            tick_size = filter_item.get("tickSize", "0")
            try:
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
            step_size = filter_item.get("stepSize", "0")
            try:
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
    
    return precision

def find_common_symbols(binance_tr_symbols: List[Dict], 
                        cs_instruments: List[Dict]) -> List[Dict]:
    """
    Find common symbols between Binance TR and Binance Futures (CS).
    Returns a list of common symbols with all required information.
    """
    # Define exceptional symbol mappings: TR_base_symbol -> Futures_code
    exceptional_mappings = {
        "PEPE": "1000PEPE",   # PEPE_TRY -> 1000PEPEUSDT
        "BONK": "1000BONK",   # BONK_TRY -> 1000BONKUSDT
        "SHIB": "1000SHIB",   # SHIB_TRY -> 1000SHIBUSDT
        "FLOKI": "1000FLOKI", # FLOKI_TRY -> 1000FLOKIUSDT
        "LUNC": "1000LUNC",   # LUNC_TRY -> 1000LUNCUSDT
        "XEC": "1000XEC",     # XEC_TRY -> 1000XECUSDT
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
                "original_base": original_base
            }
    
    logging.info(f"Found {len(tr_symbols_map)} Binance TR symbols (TRY pairs)")
    
    # Extract Futures symbols from CS instruments
    futures_symbols_map = {}
    for instrument in cs_instruments:
        code = instrument.get("code", "")  # e.g., "SKLUSDT", "GRTUSDT", "LUNA2USDT"
        instrument_id = instrument.get("id")  # CS instrument_id
        
        if code and code.endswith("USDT"):
            # Extract base symbol from code (remove USDT)
            base_symbol = code[:-4]  # Remove "USDT"
            
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
            for instrument in cs_instruments:
                if instrument.get("code") == futures_code:
                    exceptional_matched[tr_base] = {
                        "binance_futures_symbol": futures_code,
                        "instrument_id": instrument.get("id"),
                        "instrument_info": instrument
                    }
                    logging.info(f"Matched exceptional symbol: {tr_base}_TRY -> {futures_code}")
                    break
    
    logging.info(f"Found {len(common_base_symbols)} direct common symbols")
    logging.info(f"Found {len(exceptional_matched)} exceptional symbol mappings")
    total_common = len(common_base_symbols) + len(exceptional_matched)
    logging.info(f"Total common symbols: {total_common}")
    
    # Build result list
    result = []
    
    # Add direct matches
    for base_symbol in sorted(common_base_symbols):
        tr_data = tr_symbols_map[base_symbol]
        futures_data = futures_symbols_map[base_symbol]
        
        # Extract precision from Binance TR filters
        tr_info = tr_data["symbol_info"]
        precision = extract_precision_from_filters(tr_info.get("filters", []))
        
        symbol_data = {
            "base_symbol": base_symbol,
            "binance_tr_symbol": tr_data["binance_tr_symbol"],
            "binance_futures_symbol": futures_data["binance_futures_symbol"],
            "CS_instrument_id": futures_data["instrument_id"],
            "binance_tr_price_precision": precision["price_precision"],
            "binance_tr_price_step_size": precision["tick_size"],
            "binance_tr_amount_precision": precision["amount_precision"]
        }
        
        result.append(symbol_data)
    
    # Add exceptional matches
    for tr_base, futures_data in exceptional_matched.items():
        if tr_base not in common_base_symbols:  # Only add if not already matched directly
            tr_data = tr_symbols_map[tr_base]
            
            # Extract precision from Binance TR filters
            tr_info = tr_data["symbol_info"]
            precision = extract_precision_from_filters(tr_info.get("filters", []))
            
            symbol_data = {
                "base_symbol": tr_base,
                "binance_tr_symbol": tr_data["binance_tr_symbol"],
                "binance_futures_symbol": futures_data["binance_futures_symbol"],
                "CS_instrument_id": futures_data["instrument_id"],
                "binance_tr_price_precision": precision["price_precision"],
                "binance_tr_price_step_size": precision["tick_size"],
                "binance_tr_amount_precision": precision["amount_precision"]
            }
            
            result.append(symbol_data)
    
    # Sort by base_symbol
    result.sort(key=lambda x: x["base_symbol"])
    
    return result

def generate_common_symbol_info_json(output_file: str = "common_symbol_info.json"):
    """
    Generate JSON file with common symbol information.
    Includes the count at the beginning of the file.
    """
    logging.info("=" * 80)
    logging.info("Generating common symbol info JSON file")
    logging.info("Fetching data directly from exchanges")
    logging.info("=" * 80)
    
    # Fetch symbols from APIs
    logging.info("\nStep 1: Fetching Binance TR symbols...")
    binance_tr_symbols = fetch_binance_tr_symbols()
    
    if not binance_tr_symbols:
        logging.error("Failed to fetch Binance TR symbols. Exiting.")
        return None
    
    logging.info("\nStep 2: Fetching CS instruments...")
    cs_instruments = fetch_cs_instruments()
    
    if not cs_instruments:
        logging.error("Failed to fetch CS instruments. Exiting.")
        return None
    
    # Find common symbols
    logging.info("\nStep 3: Finding common symbols and extracting precision...")
    common_symbols = find_common_symbols(binance_tr_symbols, cs_instruments)
    
    if not common_symbols:
        logging.error("No common symbols found. Exiting.")
        return None
    
    # Create output structure with count at the beginning
    output_data = {
        "total_common_symbols": len(common_symbols),
        "symbols": common_symbols
    }
    
    # Write to JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"\nGenerated common symbol info JSON file: {output_file}")
    logging.info(f"Total common symbols: {len(common_symbols)}")
    
    return output_data

def main():
    """Main function."""
    output_data = generate_common_symbol_info_json("common_symbol_info.json")
    
    if output_data:
        logging.info("\n" + "=" * 80)
        logging.info("SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Total common symbols: {output_data['total_common_symbols']}")
        logging.info(f"Output file: common_symbol_info.json")
        
        # Print first few symbols as example
        if output_data["symbols"]:
            logging.info("\nFirst 5 common symbols:")
            for symbol in output_data["symbols"][:5]:
                logging.info(f"  {symbol['base_symbol']}: {symbol['binance_tr_symbol']} <-> {symbol['binance_futures_symbol']} (ID: {symbol['CS_instrument_id']})")
        logging.info("=" * 80)

if __name__ == "__main__":
    main()
