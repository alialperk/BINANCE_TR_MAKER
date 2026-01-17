# ws_sharded_multi_subscribe_aiohttp.py
# Purpose: Connect to all coins from hosts_frankfurt_usdt.json file efficiently (using aiohttp)
# - Sharding: Deterministic primary host selection per coin/iid (hash/first/roundrobin)
# - Multi-subscribe: Many instruments per WS connection (MAX_INSTR_PER_CONN)
# - Auto reconnect + exponential backoff
# - Log every N-th TOB message per instrument (LOG_EVERY_N)

import asyncio
import json
import logging
import time
import random
import hashlib
from pathlib import Path
from collections import defaultdict
from typing import List, Tuple, Dict, DefaultDict

import aiohttp  # pip install aiohttp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


HOST_FILE = Path("CS_btcturk_instruments.json")
HOST_MAPPING_FILE = Path("coin_host_mapping.json")  # Balanced host distribution

ORG        = "Quantix"
APP_NAME   = "WSClient"
APP_VER    = "1.0.0"
PROCESS_ID = f"client_{int(time.time())}"

MAX_INSTR_PER_CONN = 60           # Number of subscribed instruments per WS
HOST_PICK_STRATEGY = "balanced"   # "balanced" | "hash" | "first" | "roundrobin"
LOG_EVERY_N        = 10           # Log every N-th TOB message
HEARTBEAT_SEC      = 20           # aiohttp ws_connect heartbeat (ping)
PING_TIMEOUT_SEC   = 10           # Timeout for ping
RETRY_BASE_DELAY   = 1.0
RETRY_MAX_DELAY    = 30.0

SUB_OPTIONS = {
 
    "depthTopic": False,
    "tradesTopic": False,
    "topOfBookTopic": True,
    "indexPriceTopic": False,
    "markPriceTopic": False,
    "fundingRateTopic": False,
    "liquidationsTopic": False,
    "topOfBookCoalescing": False
}

class InstrumentEntry:
    __slots__ = ("coin", "code", "iid", "hosts")
    def __init__(self, coin: str, code: str, iid: int, hosts: List[Tuple[str, int]]):
        self.coin = coin  # Base symbol (e.g., "SKL")
        self.code = code  # Full symbol code (e.g., "SKLUSDT")
        self.iid  = iid
        self.hosts= hosts  # [(host,port), ...]

def parse_hosts_file(path: Path) -> List[InstrumentEntry]:
    """
    Parse JSON file with format:
    [
      {
        "instrument_id": 8000,
        "code": "SKLUSDT",
        "hosts": [
          {
            "host": "63.180.84.140",
            "port": 10000
          },
          ...
        ]
      },
      ...
    ]
    """
    entries: List[InstrumentEntry] = []
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            logging.error(f"Expected JSON array, got {type(data)}")
            return entries
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            code = item.get("code", "").strip()
            iid = item.get("instrument_id")
            hosts_raw = item.get("hosts", [])
            
            if not code or iid is None:
                continue
            
            # Extract base symbol from code (e.g., "SKLUSDT" -> "SKL")
            coin = code.replace("USDT", "").replace("_USDT", "").upper()
            
            host_list: List[Tuple[str, int]] = []
            if isinstance(hosts_raw, list):
                for host_obj in hosts_raw:
                    if isinstance(host_obj, dict):
                        # Handle object format: {"host": "63.180.84.140", "port": 10000}
                        host_ip = host_obj.get("host") or host_obj.get("ip", "")
                        port = host_obj.get("port")
                        if host_ip and port is not None:
                            try:
                                host_list.append((host_ip.strip(), int(port)))
                            except (ValueError, TypeError):
                                continue
                    elif isinstance(host_obj, str):
                        # Handle string format: "63.180.84.140:10000"
                        host_str = host_obj.strip()
                        if ":" not in host_str:
                            continue
                        try:
                            h, p = host_str.split(":", 1)
                            host_list.append((h.strip(), int(p.strip())))
                        except (ValueError, TypeError):
                            continue
            
            if host_list:
                entries.append(InstrumentEntry(coin, code, iid, host_list))
        
        logging.info(f"Parsed {len(entries)} entries from {path}")
        return entries
        
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {path}: {e}")
        return []
    except Exception as e:
        logging.error(f"Error parsing {path}: {e}")
        return []

# Load balanced host mapping (global cache)
_balanced_host_mapping: Dict[str, str] = None

def load_balanced_host_mapping() -> Dict[str, str]:
    """Load balanced host mapping from JSON file."""
    global _balanced_host_mapping
    if _balanced_host_mapping is not None:
        return _balanced_host_mapping
    
    _balanced_host_mapping = {}
    if HOST_MAPPING_FILE.exists():
        try:
            with open(HOST_MAPPING_FILE, "r", encoding="utf-8") as f:
                mapping_data = json.load(f)
                _balanced_host_mapping = mapping_data.get("distribution", {})
                logging.info(f"Loaded balanced host mapping: {len(_balanced_host_mapping)} coins")
        except Exception as e:
            logging.warning(f"Failed to load balanced host mapping: {e}. Using fallback strategy.")
            _balanced_host_mapping = {}
    else:
        logging.warning(f"Balanced host mapping file not found: {HOST_MAPPING_FILE}. Using fallback strategy.")
    
    return _balanced_host_mapping

def pick_primary_host(hosts: List[Tuple[str,int]], code: str, coin: str, key: str, rr_state: Dict[str,int]) -> Tuple[str,int]:
    """
    Pick primary host for a coin. Uses balanced mapping if available and strategy is "balanced".
    
    Args:
        hosts: List of available hosts for this coin
        code: Full symbol code (e.g., "SKLUSDT") for balanced mapping lookup
        coin: Base coin name (e.g., "SKL") for fallback
        key: Key for hash/roundrobin strategies
        rr_state: Round-robin state dict
    """
    if not hosts: 
        raise ValueError("empty hosts")
    
    # Use balanced mapping if strategy is "balanced" and mapping is available
    if HOST_PICK_STRATEGY == "balanced":
        balanced_mapping = load_balanced_host_mapping()
        # Try code first (e.g., "SKLUSDT"), then fallback to coin (e.g., "SKL")
        symbol_key = code if code and code in balanced_mapping else coin
        if symbol_key and symbol_key in balanced_mapping:
            mapped_host_str = balanced_mapping[symbol_key]
            # Find the mapped host in the available hosts list
            for host, port in hosts:
                if f"{host}:{port}" == mapped_host_str:
                    return (host, port)
            # If mapped host not in available hosts, fall back to hash
            logging.warning(f"Mapped host {mapped_host_str} for {symbol_key} not in available hosts. Using fallback.")
    
    # Fallback strategies
    if HOST_PICK_STRATEGY == "first" or len(hosts) == 1:
        return hosts[0]
    if HOST_PICK_STRATEGY == "hash":
        h = hashlib.blake2b(key.encode(), digest_size=2).digest()
        idx = int.from_bytes(h, "big") % len(hosts)
        return hosts[idx]
    # Round-robin
    cur = rr_state.get(key, 0)
    rr_state[key] = (cur + 1) % len(hosts)
    return hosts[cur]

def shard_by_endpoint(entries: List[InstrumentEntry]) -> Dict[Tuple[str,int], List[InstrumentEntry]]:
    """
    Group instruments by endpoint. Each instrument is added to ALL its available hosts,
    so every symbol subscribes to both hosts.
    """
    groups: Dict[Tuple[str,int], List[InstrumentEntry]] = defaultdict(list)
    for e in entries:
        # Add this instrument to ALL its hosts (subscribe to both hosts for every symbol)
        for host, port in e.hosts:
            groups[(host, port)].append(e)
    return dict(groups)

def bucketize(instruments: List[InstrumentEntry], max_per: int) -> List[List[InstrumentEntry]]:
    return [instruments[i:i+max_per] for i in range(0, len(instruments), max_per)]

async def ws_worker(session: aiohttp.ClientSession, endpoint: Tuple[str,int], instrs: List[InstrumentEntry]):
    host, port = endpoint
    url = f"ws://{host}:{port}/api/v6"
    backoff = RETRY_BASE_DELAY
    msg_counters: DefaultDict[int,int] = defaultdict(int)

    logging.info(f"WS worker → {host}:{port} | {len(instrs)} instruments")

    while True:
        try:
            async with session.ws_connect(
                url,
                heartbeat=HEARTBEAT_SEC,  # Automatic ping
                autoping=True,
                max_msg_size=8*1024*1024,
                timeout=PING_TIMEOUT_SEC,
            ) as ws:
                # Login
                await ws.send_json([13, ORG, APP_NAME, APP_VER, PROCESS_ID])
                await asyncio.sleep(0.2)
                # Multi-subscribe
                for e in instrs:
                    await ws.send_json([11, e.iid, SUB_OPTIONS])
                    await asyncio.sleep(0.01)
                logging.info(f"Subscribed on {host}:{port} ({len(instrs)} instruments)")
                backoff = RETRY_BASE_DELAY

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            # Only log snapshot messages (type 0) for instrument 8037
                            if isinstance(data, list) and len(data) >= 2:
                                msg_type = data[0]  # First field: message type
                                instrument = data[1]  # Second field: instrument id
                                if instrument in [8329]:
                                    logging.info(f"[{host}:{port}] TOB message for instrument {instrument}: {data}")
#                          
                        except Exception:
                            continue
                        # tob = extract_tob(data)
                        # if not tob:
                        #     continue
                        # iid, bid_p, ask_p = tob
                        # msg_counters[iid] += 1
                        # if (msg_counters[iid] % LOG_EVERY_N) == 0:
                        #     logging.info(f"[{host}:{port}] id={iid} bid={bid_p} ask={ask_p} (#{msg_counters[iid]})")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError(f"WS closed/error: {msg.type}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.warning(f"{host}:{port} error: {e}")
            # Backoff + jitter
            delay = backoff + random.random()
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, RETRY_MAX_DELAY)
            logging.info(f"Reconnect {host}:{port} in ~{delay:.1f}s (next cap {backoff:.1f}s)")

async def main():
    if not HOST_FILE.exists():
        logging.error(f"Hosts file not found: {HOST_FILE}")
        return

    entries = parse_hosts_file(HOST_FILE)
    if not entries:
        logging.error("No instruments found in file.")
        return

    # Shard by endpoint
    by_endpoint = shard_by_endpoint(entries)

    tasks = []
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
    connector = aiohttp.TCPConnector(limit=0)  # Unlimited concurrent connections (watch OS limits)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for endpoint, instrs in by_endpoint.items():
            for bucket in bucketize(instrs, MAX_INSTR_PER_CONN):
                tasks.append(asyncio.create_task(ws_worker(session, endpoint, bucket)))

        logging.info(f"{len(entries)} instruments → {len(by_endpoint)} endpoints → {len(tasks)} WS connections")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Stopped")