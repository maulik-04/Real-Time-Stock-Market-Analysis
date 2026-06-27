# Real-Time Stock Market Data Processing

A real-time streaming pipeline that ingests live NSE stock prices, processes them with Apache Spark Structured Streaming, and displays results on a live interactive dashboard.

**Tracked symbols:** RELIANCE · TCS · HDFCBANK · INFY

---

## Architecture

```
Upstox API
    │  (REST, every 15 s per symbol)
    ▼
producer.py  ──► Kafka topic: indian_stocks
                      │
                      ▼
              spark_inference.py
              (5-min sliding window, 30-s step)
                      │
                      ▼
              Kafka topic: indian_avg
                      │
                      ▼
              dashboard.py (Flask)
                      │
                      ▼
              dashboard.html (Chart.js live chart)
```

---

## How It Works

### 1. Data Ingestion (`producer.py`)
Fetches live NSE prices via the Upstox v2 Market Quote API. Round-robins through all four symbols with a 15-second pause between each, giving a fresh price for every symbol roughly every 60 seconds. Publishes each price tick as a JSON message to the `indian_stocks` Kafka topic.

### 2. Stream Processing (`spark_inference.py`)
Apache Spark Structured Streaming consumes `indian_stocks` and applies:
- **5-minute sliding window** (advances every 30 seconds) per symbol
- **3-minute watermark** to gracefully handle late-arriving messages
- **`avg("ltp")`** → the moving average to plot
- **`last("ltp")`** → the most recent price seen in the window (not `max`, which would give the highest price, not the latest)

Results are written to the `indian_avg` Kafka topic.

### 3. Dashboard (`dashboard.py` + `dashboard.html`)
Flask backend consumes `indian_avg` into per-symbol ring buffers (500 points each). A `/data` endpoint returns pre-grouped JSON. The frontend polls every 3 seconds, updates per-symbol stat cards, and redraws the Chart.js time-series line chart.

---

## Setup

### Prerequisites
- Python 3.10+
- Apache Kafka running on `localhost:9092`
- Java 8 or 11 (required by PySpark)
- An [Upstox developer account](https://developer.upstox.com/) with API credentials

### Installation

```bash
# 1. Clone the repo
git clone https://github.com/youruser/real-time-stock-market-analysis.git
cd real-time-stock-market-analysis

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Configure credentials (never commit .env to Git)
cp .env.example .env
# Edit .env and fill in your UPSTOX_API_KEY and UPSTOX_ACCESS_TOKEN
```

### Running

Open **three separate terminals**:

```bash
# Terminal 1 — Kafka producer
python producer.py

# Terminal 2 — Spark streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  spark_inference.py

# Terminal 3 — Flask dashboard
python dashboard.py
```

Then open `http://localhost:5000` in your browser.

> **Note:** The Upstox access token expires daily. Re-generate it from your Upstox developer console and update `UPSTOX_ACCESS_TOKEN` in `.env`.

---

## Key Design Decisions

| Decision | Why |
|---|---|
| Kafka between every stage | Decouples producer, Spark, and dashboard; each can restart independently |
| Write Spark output back to Kafka | Dashboard stays stateless — it just reads a topic |
| Per-symbol ring buffers in Flask | Pre-groups data so `/data` is O(1) per symbol; avoids re-grouping on every poll |
| `last("ltp")` not `max("ltp")` | `max` gives the highest price in the window, not the most recent one |
| `outputMode("update")` | Only writes rows that changed this trigger; `complete` rewrites everything every 30 s |
| Watermark of 3 minutes | Handles API/network delays without keeping windows open forever |
| `debug=False` in Flask | `debug=True` spawns a reloader thread, which starts a second Kafka consumer and doubles every message |

---

## Project Structure

```
├── producer.py          # Fetches NSE prices → publishes to Kafka
├── spark_inference.py   # Spark Structured Streaming: windowed avg
├── dashboard.py         # Flask backend: Kafka consumer + REST API
├── dashboard.html       # Frontend: Chart.js live dashboard
├── requirements.txt     # Python dependencies
├── .env.example         # Template for secrets (copy to .env)
├── .gitignore           # Excludes .env and other non-source files
└── README.md
```
