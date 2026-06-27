import os
import requests
import json
import time
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()


KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
TOPIC = "indian_stocks"

API_KEY = os.environ.get("UPSTOX_API_KEY")
ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

if not API_KEY or not ACCESS_TOKEN:
    print("ERROR: UPSTOX_API_KEY and UPSTOX_ACCESS_TOKEN must be set in your .env file.")
    print("Copy .env.example to .env and fill in your credentials.")
    sys.exit(1)


INSTRUMENT_KEYS = {
    "RELIANCE": "NSE_EQ|INE002A01018",
    "TCS": "NSE_EQ|INE467B01029",
    "HDFCBANK": "NSE_EQ|INE040A01034",
    "INFY": "NSE_EQ|INE009A01021",
}

INDIAN_SYMBOLS = list(INSTRUMENT_KEYS.keys())

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=15_000,
    retries=3,
)


def get_stock_quote(symbol: str) -> dict | None:
    """
    Fetch the latest market quote for one symbol from Upstox v2 API.

    Returns the raw API JSON on success, or None on any failure so the
    caller can skip this tick without crashing the whole pipeline.
    """
    url = "https://api.upstox.com/v2/market-quote/quotes"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = {"instrument_key": INSTRUMENT_KEYS[symbol]}

    try:
        print(f"Fetching {symbol}...")
        response = requests.get(url, headers=headers, params=params, timeout=10)

        # 401 means the token has expired (Upstox tokens expire daily)
        if response.status_code == 401:
            print("Access token expired. Re-generate it at:")
            print(
                f"https://api.upstox.com/v2/login/authorization/dialog"
                f"?response_type=code&client_id={API_KEY}&redirect_uri=https://127.0.0.1"
            )
            sys.exit(1)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        print(f"Timeout fetching {symbol} — skipping this tick.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for {symbol}: {e} — skipping this tick.")
        return None


def build_payload(symbol: str, quote: dict) -> dict:
    """
    Flatten the nested Upstox quote dict into a flat payload dict
    that Spark's schema can parse directly.
    """
    ohlc = quote.get("ohlc", {})
    return {
        "symbol": symbol,
        "exchange": "NSE",
        "ltp": quote.get("last_price", 0),
        "open": ohlc.get("open", 0),
        "high": ohlc.get("high", 0),
        "low": ohlc.get("low", 0),
        "close": ohlc.get("close", 0),
        "volume": quote.get("volume", 0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def produce_data():
    """
    Round-robin through all symbols, fetch a quote, and publish to Kafka.

    We cycle one symbol per iteration (15-second sleep between each) so
    all four symbols get a fresh price roughly every 60 seconds — well
    within the 5-minute moving average window.
    """
    symbol_index = 0

    while True:
        symbol = INDIAN_SYMBOLS[symbol_index]
        print(f"\nProcessing {symbol}...")

        data = get_stock_quote(symbol)

        if data and "data" in data:
            response_key = f"NSE_EQ:{symbol}"
            quote = data["data"].get(response_key, {})

            if quote:
                payload = build_payload(symbol, quote)
                producer.send(TOPIC, payload)
                # flush() ensures the message is actually sent to the broker
                # before we move on — without this, buffered messages can be
                # lost if the process is interrupted between ticks
                producer.flush()
                print(f"Sent {symbol}: ₹{payload['ltp']:.2f}")
            else:
                print(f"No quote for {symbol}. Available keys: {list(data['data'].keys())}")
        else:
            print(f"Empty or malformed response for {symbol} — skipping.")

        symbol_index = (symbol_index + 1) % len(INDIAN_SYMBOLS)
        time.sleep(15)


if __name__ == "__main__":
    print("Starting Kafka producer")
    print(f"Kafka broker : {KAFKA_SERVER}")
    print(f"Topic : {TOPIC}")
    print(f"Symbols : {INDIAN_SYMBOLS}")
    produce_data()
