#!/usr/bin/env python3
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from statistics import mean

# ======= SABİT AYARLAR =======
BASE_URL = "https://www.binance.tr"
ORDER_ENDPOINT = "/open/v1/orders"

# 🔐 API ANAHTARLARINI BURAYA YAZIYORSUN
API_KEY = "3fF6b102Ce7e0A7ABd449BA1A8F1A46CVbm7W3Vix82GQ8QrRuHHM9bVgp11WRPD"
API_SECRET = "74D15d11403E97ba8794a725E6D79962dAiomwRKBUAH77ObUiffc9XRXqm63qvN"

# LIMIT ORDER parametreleri
SYMBOL = "2Z_TRY"     # Örnek sembol, istersen BTC_USDT vb. yap
SIDE = 0               # 0=BUY, 1=SELL
ORDER_TYPE = 1         # 1 = LIMIT

PRICE = "5.14"        # Mantıklı bir limit fiyat yaz
QUANTITY = "10"         # Adet

ORDER_COUNT = 10
RECV_WINDOW = 5000
WAIT_BETWEEN_ORDERS = 1.30   # saniye

# =============================


def sign_payload(params: dict, secret: str):
    query = urlencode(params, doseq=True)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return sig, query


def send_limit_order(session: requests.Session):
    url = BASE_URL + ORDER_ENDPOINT

    params = {
        "symbol": SYMBOL,
        "side": SIDE,
        "type": ORDER_TYPE,   # LIMIT
        "price": PRICE,
        "quantity": QUANTITY,
        "recvWindow": RECV_WINDOW,
        "timestamp": int(time.time() * 1000),
    }

    signature, qs = sign_payload(params, API_SECRET)
    body = f"{qs}&signature={signature}"

    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    t0 = time.perf_counter()
    resp = session.post(url, headers=headers, data=body, timeout=5)
    t1 = time.perf_counter()

    round_trip = (t1 - t0) * 1000  # ms

    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    server_ts = data.get("timestamp")
    delta = None
    if isinstance(server_ts, (int, float)):
        delta = server_ts - params["timestamp"]

    return {
        "round_trip_ms": round_trip,
        "server_delta_ms": delta,
        "http_status": resp.status_code,
        "response": data,
    }


def main():
    print("== LIMIT ORDER TEST ==")
    print(f"Symbol={SYMBOL}, Price={PRICE}, Qty={QUANTITY}, Side={SIDE}")
    print(f"{ORDER_COUNT} emir gönderilecek...\n")

    rts, deltas = [], []

    with requests.Session() as s:
        for i in range(ORDER_COUNT):
            print(f"--- Order {i+1}/{ORDER_COUNT} ---")
            result = send_limit_order(s)

            rt = result["round_trip_ms"]
            sd = result["server_delta_ms"]
            rts.append(rt)
            if sd is not None:
                deltas.append(sd)

            resp = result["response"]
            print(f"HTTP Status: {result['http_status']}")
            print(f"Round-trip : {rt:.2f} ms")
            if sd is not None:
                print(f"Server delta: {sd} ms")

            print("RESPONSE:", resp)
            print()

            time.sleep(WAIT_BETWEEN_ORDERS)

    print("=== SUMMARY ===")
    print(f"Round-trip → min {min(rts):.2f} ms | avg {mean(rts):.2f} ms | max {max(rts):.2f} ms")
    if deltas:
        print(f"Server delta → min {min(deltas)} | avg {mean(deltas):.2f} | max {max(deltas)} ms")


if __name__ == "__main__":
    main()