# build_frankfurt_host_map_multi.py
import json
import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

MASTERDATA = "http://masterdata.cryptostruct.com"
INSTRUMENTS_URL = f"{MASTERDATA}/api/instruments"
MARKETDATA_URL  = f"{MASTERDATA}/api/v2/marketdata"

INSTRUMENT_PARAMS = {
    # Example filters:
    "exchange_id": 8,   # Binance Perp
    "state": "open",
}
REGION = "eu-central-1"
BATCH_SIZE = 200
TIMEOUT = 15
RETRY = 2
MAX_WORKERS = 10  # Number of concurrent HTTP requests

OUT_JSON = "CS_all_instruments.json"

# Global session for connection pooling (reuse connections for better performance)
_session = None

def get_session():
    """Get or create a requests session with connection pooling"""
    global _session
    if _session is None:
        _session = requests.Session()
        # Connection pooling settings
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0  # We handle retries manually
        )
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)
    return _session

def http_get(url: str, params: dict) -> Any:
    """HTTP GET with retry logic and connection pooling"""
    session = get_session()
    last = None
    for attempt in range(RETRY + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT, stream=False)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if attempt < RETRY:
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
    raise last

def fetch_instruments(params: dict) -> List[dict]:
    """Fetch instruments from API with timing and progress logging"""
    logging.info(f"Fetching instruments from {INSTRUMENTS_URL}...")
    fetch_start = time.time()
    
    payload = http_get(INSTRUMENTS_URL, params)
    instruments = payload.get("data", [])
    
    fetch_time = time.time() - fetch_start
    logging.info(f"Fetched {len(instruments)} instruments in {fetch_time:.2f} seconds")
    
    return instruments

def chunked(ids: List[int], n: int):
    for i in range(0, len(ids), n):
        yield ids[i:i+n]

def fetch_batch_hosts(batch: List[int], region: str) -> Dict[int, List[dict]]:
    """Fetch hosts for a single batch of instrument IDs"""
    batch_result: Dict[int, List[dict]] = {}
    try:
        payload = http_get(MARKETDATA_URL, {
            "instruments": ",".join(map(str, batch)),
            "region": region
        })
        for item in payload.get("data", []):
            iid = int(item["instrument_id"])
            hosts = item.get("hosts", [])
            # Only take region matches; otherwise leave all (fallback)
            filtered = [h for h in hosts if (h.get("location") == region)]
            batch_result[iid] = filtered or hosts
    except Exception as e:
        logging.error(f"Error fetching batch {batch[:5]}...: {e}")
    return batch_result

def fetch_market_hosts(ids: List[int], region: str) -> Dict[int, List[dict]]:
    """Fetch market hosts for all instrument IDs using concurrent requests"""
    result: Dict[int, List[dict]] = {}
    batches = list(chunked(ids, BATCH_SIZE))
    total_batches = len(batches)
    
    logging.info(f"Fetching hosts for {len(ids)} instruments in {total_batches} batches (max {MAX_WORKERS} concurrent)")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all batch requests
        future_to_batch = {
            executor.submit(fetch_batch_hosts, batch, region): batch 
            for batch in batches
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_batch):
            completed += 1
            batch_result = future.result()
            result.update(batch_result)
            if completed % 10 == 0 or completed == total_batches:
                logging.info(f"Progress: {completed}/{total_batches} batches completed")
    
    return result

def main():
    start_time = time.time()
    
    # Fetch instruments
    fetch_instr_start = time.time()
    instruments = fetch_instruments(INSTRUMENT_PARAMS)
    fetch_instr_time = time.time() - fetch_instr_start
    
    # Filter instruments: only keep those with codes ending in "USDT"
    instruments_filtered = [
        x for x in instruments 
        if x.get("code", "").upper().endswith("USDT")
    ]
    logging.info(f"Filtered to {len(instruments_filtered)} instruments with USDT pairs (from {len(instruments)} total)")
    
    # Process instrument data
    process_start = time.time()
    id2code = {int(x["id"]): x.get("code") for x in instruments_filtered if "id" in x}
    id2type = {int(x["id"]): x.get("type", "") for x in instruments_filtered if "id" in x}
    id2exchange_id = {int(x["id"]): x.get("exchange_id") for x in instruments_filtered if "id" in x}
    id2exchange_code = {int(x["id"]): x.get("exchange_code", "") for x in instruments_filtered if "id" in x}
    id2exchange_name = {int(x["id"]): x.get("exchange_name", "") for x in instruments_filtered if "id" in x}
    iids = list(id2code.keys())
    process_time = time.time() - process_start
    logging.info(f"Processed {len(iids)} instrument IDs in {process_time:.2f} seconds (fetch took {fetch_instr_time:.2f}s)")

    id2hosts = fetch_market_hosts(iids, REGION)

    # Build JSON output (optimized: pre-allocate list size)
    # Deduplicate hosts per instrument to ensure unique host:port combinations
    rows_json = [None] * len(iids)
    for idx, iid in enumerate(iids):
        hosts_raw = id2hosts.get(iid, [])
        
        # Deduplicate hosts: keep unique host:port combinations
        # Same host IP can appear multiple times with different ports
        # Use a set to track seen host:port combinations
        seen_hosts = set()
        unique_hosts = []
        
        for host_obj in hosts_raw:
            # Extract host IP and port
            host_ip = host_obj.get("host") or host_obj.get("ip", "")
            port = host_obj.get("port")
            
            if host_ip and port:
                # Create unique key: host:port (same IP with different ports are unique)
                host_key = f"{host_ip}:{port}"
                
                # Only add if we haven't seen this exact host:port combination
                # Same IP with different ports will be kept (e.g., 63.180.141.87:10000 and 63.180.141.87:10010)
                if host_key not in seen_hosts:
                    seen_hosts.add(host_key)
                    unique_hosts.append(host_obj)
        
        rows_json[idx] = {
            "instrument_id": iid,
            "type": id2type.get(iid, ""),
            "code": id2code.get(iid),
            "exchange_id": id2exchange_id.get(iid),
            "exchange_code": id2exchange_code.get(iid, ""),
            "exchange_name": id2exchange_name.get(iid, ""),
            "hosts": unique_hosts,  # Unique hosts only (deduplicated by host:port)
        }
    
    # Write to temporary file first, then rename (atomic write)
    import tempfile
    import shutil
    temp_file = OUT_JSON + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(rows_json, f, ensure_ascii=False, indent=2)
            f.flush()  # Ensure data is written to disk
            os.fsync(f.fileno())  # Force write to disk
        
        # Atomic rename (ensures file is complete)
        shutil.move(temp_file, OUT_JSON)
        logging.info(f"JSON written atomically: {OUT_JSON}")
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise

    with_hosts = sum(1 for iid in iids if id2hosts.get(iid))
    elapsed_time = time.time() - start_time
    logging.info(f"Hosts found: {with_hosts}/{len(iids)} instruments")
    logging.info(f"Total time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()