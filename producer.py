import requests
import json
import time
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer



# Configuration
KAFKA_SERVER = "localhost:9092"   ## the server where the producer will send the data (in this laptop at port 9092)
TOPIC = "indian_stocks"  ## it will send to the topic "indian_stocks"
API_KEY = "32dd0b97-6765-4f98-9615-25230cc150e3"  ## Key and token given by Upstox API to connect with it
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzV0NVTEUiLCJqdGkiOiI2ODZjYmUyMzUxOWUzYjA4Zjc5NWI0NTIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1MTk1NzAyNywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzUyMDEyMDAwfQ.UOg5pABKVt76zTLPgE3bT--RObRuQLbCoasGTMf6zr8"

# Upstox works using Instrument Keys for different companies/symbols
INSTRUMENT_KEYS = {
    "RELIANCE": "NSE_EQ|INE002A01018",
    "TCS": "NSE_EQ|INE467B01029",
    "HDFCBANK": "NSE_EQ|INE040A01034",
    "INFY": "NSE_EQ|INE009A01021"
}

# List of symbols to process
INDIAN_SYMBOLS = list(INSTRUMENT_KEYS.keys())

# Kafka producer setup
## KafkaProducer is used to connect the producer in Kafka Application with the python file
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetching quote using instrument_key
## We are using REST API to fetch the data 
def get_stock_quote(symbol):
    url = "https://api.upstox.com/v2/market-quote/quotes"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = {"instrument_key": INSTRUMENT_KEYS[symbol]}
    
    try:
        print(f"🔍 Fetching {symbol}...")
        response = requests.get(url, headers=headers, params=params, timeout=10)
        print(f"Response: {response.status_code} {response.text[:200]}")
        
        if response.status_code == 401:
            print("Token expired. Regenerate it here:")
            print(f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri=https://127.0.0.1")
            return None

        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return None

# Continuously producing data
def produce_data():
    symbol_index = 0
    while True:
        symbol = INDIAN_SYMBOLS[symbol_index]
        print(f"\nProcessing {symbol}...")
        data = get_stock_quote(symbol)
        
        if data and 'data' in data:
            response_key = f"NSE_EQ:{symbol}"
            quote = data['data'].get(response_key, {})

            if quote:
                ohlc = quote.get('ohlc', {})
                payload = {
                    "symbol": symbol,
                    "exchange": "NSE",
                    "ltp": quote.get('last_price', 0),
                    "open": ohlc.get('open', 0),
                    "high": ohlc.get('high', 0),
                    "low": ohlc.get('low', 0),
                    "close": ohlc.get('close', 0),
                    "volume": quote.get('volume', 0),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                producer.send(TOPIC, payload)
                print(f"Sent {symbol}: ₹{payload['ltp']:.2f}")
            else:
                print(f"No quote data in response")
                print("Available keys in response:", list(data['data'].keys()))
        
        symbol_index = (symbol_index + 1) % len(INDIAN_SYMBOLS)
        time.sleep(15)


## Main entry point
if __name__ == "__main__":
    print("Starting Producer")
    print("Using Upstox instrument keys")
    produce_data()

