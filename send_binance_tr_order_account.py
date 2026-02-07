#!/usr/bin/env python3
import time
import hmac
import hashlib
import urllib3
from urllib.parse import urlencode
from statistics import mean
from APIkeys import BINANCE_TR_api_key_ALPER, BINANCE_TR_secret_key_ALPER

# ======= OPTIMIZATIONS =======
# 1. Use urllib3 PoolManager (faster than requests.Session)
# 2. Pre-compile reusable components
# 3. Minimize object creation
# 4. Use bytes operations where possible
# =============================

# ======= SABİT AYARLAR =======
BASE_URL = "https://www.binance.tr"
ORDER_ENDPOINT = "/open/v1/orders"
ACCOUNT_ENDPOINT = "/open/v1/account/spot"

API_KEY = BINANCE_TR_api_key_ALPER
API_SECRET = BINANCE_TR_secret_key_ALPER
API_SECRET_BYTES = API_SECRET.encode('utf-8')  # Pre-encode for HMAC

SYMBOL = "ACE_TRY"
SIDE = 0
ORDER_TYPE = 1
PRICE = "10"
QUANTITY = "22"
ORDER_COUNT = 1
RECV_WINDOW = 5000
WAIT_BETWEEN_ORDERS = 1.30

# Pre-create headers (immutable parts)
HEADERS_BASE = {
    "X-MBX-APIKEY": API_KEY,
    "Content-Type": "application/x-www-form-urlencoded",
}

# =============================


def sign_payload_fast(params: dict) -> tuple:
    """Optimized signing - uses pre-encoded secret"""
    query = urlencode(params, doseq=True)
    query_bytes = query.encode('utf-8')
    sig = hmac.new(API_SECRET_BYTES, query_bytes, hashlib.sha256).hexdigest()
    return sig, query


def get_account_info(http: urllib3.PoolManager):
    """Get account information from Binance TR API"""
    url = BASE_URL + ACCOUNT_ENDPOINT
    
    params = {
        "recvWindow": RECV_WINDOW,
        "timestamp": int(time.time() * 1000),
    }
    
    signature, qs = sign_payload_fast(params)
    full_url = f"{url}?{qs}&signature={signature}"
    
    headers = {"X-MBX-APIKEY": API_KEY}
    
    t0 = time.perf_counter()
    resp = http.request('GET', full_url, headers=headers, timeout=5.0)
    t1 = time.perf_counter()
    
    round_trip = (t1 - t0) * 1000
    
    import json
    try:
        data = json.loads(resp.data)
    except Exception:
        data = {"raw": resp.data.decode('utf-8')}
    
    return {
        "round_trip_ms": round_trip,
        "http_status": resp.status,
        "response": data,
    }


def send_limit_order_fast(http: urllib3.PoolManager):
    """Optimized order sending"""
    url = BASE_URL + ORDER_ENDPOINT

    # Capture order sent time
    order_sent_time = int(time.time() * 1000)
    
    params = {
        "symbol": SYMBOL,
        "side": SIDE,
        "type": ORDER_TYPE,
        "price": PRICE,
        "quantity": QUANTITY,
        "recvWindow": RECV_WINDOW,
        "timestamp": order_sent_time,
    }

    signature, qs = sign_payload_fast(params)
    body = f"{qs}&signature={signature}"

    t0 = time.perf_counter()
    resp = http.request('POST', url, headers=HEADERS_BASE, body=body, timeout=5.0)
    t1 = time.perf_counter()

    round_trip = (t1 - t0) * 1000

    import json
    try:
        data = json.loads(resp.data)
    except Exception:
        data = {"raw": resp.data.decode('utf-8')}

    response_timestamp = data.get("timestamp")
    order_create_time = None
    if isinstance(data.get("data"), dict):
        order_create_time = data.get("data", {}).get("createTime")
    
    diff_sent_to_response = None
    diff_sent_to_create = None
    diff_create_to_response = None
    
    if isinstance(response_timestamp, (int, float)):
        diff_sent_to_response = response_timestamp - order_sent_time
    
    if isinstance(order_create_time, (int, float)):
        diff_sent_to_create = order_create_time - order_sent_time
        
        if isinstance(response_timestamp, (int, float)):
            diff_create_to_response = response_timestamp - order_create_time

    return {
        "round_trip_ms": round_trip,
        "order_sent_time": order_sent_time,
        "order_create_time": order_create_time,
        "response_timestamp": response_timestamp,
        "diff_sent_to_response_ms": diff_sent_to_response,
        "diff_sent_to_create_ms": diff_sent_to_create,
        "diff_create_to_response_ms": diff_create_to_response,
        "http_status": resp.status,
        "response": data,
    }


def main():
    print("== BINANCE TR ORDER TEST (OPTIMIZED PYTHON) ==\n")
    
    # Create connection pool with optimizations
    http = urllib3.PoolManager(
        num_pools=1,
        maxsize=10,  # Keep connections alive
        block=False,
        timeout=urllib3.Timeout(connect=2.0, read=5.0),
        retries=False
    )
    
    print("=== STEP 1: GETTING ACCOUNT INFORMATION ===")
    account_result = get_account_info(http)
    
    print(f"HTTP Status: {account_result['http_status']}")
    print(f"Round-trip: {account_result['round_trip_ms']:.2f} ms")
    
    resp = account_result["response"]
    
    if resp.get("code") == 0:
        data = resp.get("data", {})
        print(f"\n✓ Account Info Retrieved Successfully")
        print(f"Can Trade: {data.get('canTrade', 'N/A')}")
        
        assets = data.get("accountAssets", [])
        if assets:
            print(f"\nAccount Assets:")
            for asset in assets:
                asset_name = asset.get("asset", "N/A")
                free = asset.get("free", "0")
                locked = asset.get("locked", "0")
                total = float(free) + float(locked)
                if total > 0:
                    print(f"  {asset_name}: Free={free}, Locked={locked}, Total={total}")
    else:
        print(f"✗ Error getting account info:")
        print(f"  Code: {resp.get('code', 'N/A')}")
        print(f"  Message: {resp.get('msg', 'N/A')}")
        return
    
    print("\n" + "="*50)
    print("=== STEP 2: SENDING ORDERS ===")
    print(f"Symbol={SYMBOL}, Price={PRICE}, Qty={QUANTITY}, Side={'BUY' if SIDE == 0 else 'SELL'}")
    print(f"{ORDER_COUNT} emir gönderilecek...\n")

    rts, diff_sent_to_create_list = [], []

    for i in range(ORDER_COUNT):
        print(f"--- Order {i+1}/{ORDER_COUNT} ---")
        result = send_limit_order_fast(http)

        rt = result["round_trip_ms"]
        rts.append(rt)
        
        order_sent_time = result.get("order_sent_time")
        order_create_time = result.get("order_create_time")
        response_timestamp = result.get("response_timestamp")
        
        diff_sent_to_response = result.get("diff_sent_to_response_ms")
        diff_sent_to_create = result.get("diff_sent_to_create_ms")
        diff_create_to_response = result.get("diff_create_to_response_ms")
        
        if diff_sent_to_create is not None:
            diff_sent_to_create_list.append(diff_sent_to_create)

        resp = result["response"]
        print(f"HTTP Status: {result['http_status']}")
        print(f"Round-trip: {rt:.2f} ms")
        print()
        print("=== TIMING INFORMATION ===")
        print(f"Order Sent Time (client): {order_sent_time}")
        print(f"Order Create Time (server): {order_create_time}")
        print(f"Response Timestamp (server): {response_timestamp}")
        print()
        print("=== TIME DIFFERENCES (ms) ===")
        if diff_sent_to_response is not None:
            print(f"Sent → Response: {diff_sent_to_response} ms")
        if diff_sent_to_create is not None:
            print(f"⏱️  CRITICAL: Sent → Create: {diff_sent_to_create} ms")
        if diff_create_to_response is not None:
            print(f"Create → Response: {diff_create_to_response} ms")
        print()
        print("RESPONSE:", resp)
        print()

        if i < ORDER_COUNT - 1:
            time.sleep(WAIT_BETWEEN_ORDERS)

    print("=== SUMMARY ===")
    print(f"Round-trip → min {min(rts):.2f} ms | avg {mean(rts):.2f} ms | max {max(rts):.2f} ms")
    if diff_sent_to_create_list:
        print(f"⏱️  CRITICAL METRIC - Sent → Create: min {min(diff_sent_to_create_list)} | avg {mean(diff_sent_to_create_list):.2f} | max {max(diff_sent_to_create_list)}")


if __name__ == "__main__":
    main()