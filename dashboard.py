import json
import threading
import logging
from collections import deque, defaultdict

import pytz
from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Logging — so we know what the background thread is doing
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Per-symbol ring buffers (500 data points each ≈ ~4 hours at 30s intervals)
# Using defaultdict so a new symbol is handled automatically
#
# WHY per-symbol buffers instead of one shared deque?
# → A shared deque mixes all symbols together; the dashboard has to re-group
#   them on every poll. Per-symbol buffers make the /data endpoint O(1) per
#   symbol and make the grouping logic trivial.
# ---------------------------------------------------------------------------
symbol_data: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

# A simple lock to protect symbol_data from concurrent read/write between
# the Kafka consumer thread and Flask request threads.
data_lock = threading.Lock()


def consume_kafka():
    """
    Background thread: continuously reads from the 'indian_avg' Kafka topic
    and appends each message into the correct per-symbol ring buffer.

    Includes retry logic: if the connection drops, we wait 5 seconds and
    reconnect instead of silently dying and leaving the dashboard stale.
    """
    while True:
        try:
            log.info("Connecting to Kafka topic 'indian_avg'...")
            consumer = KafkaConsumer(
                "indian_avg",
                bootstrap_servers="localhost:9092",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                # "latest" — only process new messages (not the full history)
                auto_offset_reset="latest",
                # If the broker doesn't hear from us in 30s, it drops our session
                session_timeout_ms=30_000,
                # We send heartbeats every 10s to stay in the consumer group
                heartbeat_interval_ms=10_000,
            )
            log.info("Kafka consumer connected.")

            for message in consumer:
                data = message.value
                symbol = data.get("symbol")
                if not symbol:
                    log.warning("Received message with no symbol field — skipping.")
                    continue

                with data_lock:
                    symbol_data[symbol].append(data)

                log.info(
                    "Received %-10s  ltp=%-10.2f  avg=%-10.2f",
                    symbol,
                    data.get("ltp", 0),
                    data.get("avg", 0),
                )

        except KafkaError as e:
            log.error("Kafka error: %s — reconnecting in 5 seconds...", e)
        except Exception as e:
            log.error("Unexpected error in consumer thread: %s — reconnecting in 5 seconds...", e)

        # Wait before retrying so we don't spam reconnection attempts
        threading.Event().wait(5)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    # Serve the dashboard HTML file directly
    with open("dashboard.html", encoding="utf-8") as f:
        return render_template_string(f.read())


@app.route("/data")
def data():
    """
    Return the latest buffered data for all symbols as JSON.

    Response shape:
    {
      "RELIANCE": [ {symbol, avg, ltp, start, end}, ... ],
      "TCS":      [ ... ],
      ...
    }

    The dashboard groups by symbol on the frontend, so returning pre-grouped
    data reduces the JS work and makes the structure explicit.
    """
    with data_lock:
        # Convert deques to lists for JSON serialisation
        payload = {sym: list(buf) for sym, buf in symbol_data.items()}
    return jsonify(payload)


@app.route("/health")
def health():
    """Simple health check — useful for monitoring or a load balancer."""
    with data_lock:
        counts = {sym: len(buf) for sym, buf in symbol_data.items()}
    return jsonify({"status": "ok", "buffer_sizes": counts})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the Kafka consumer as a daemon thread so it exits automatically
    # when the main Flask process exits (Ctrl+C)
    t = threading.Thread(target=consume_kafka, daemon=True, name="kafka-consumer")
    t.start()

    # debug=False in "production" (even local) because debug=True spawns a
    # reloader subprocess which starts a second consumer thread and doubles
    # every Kafka message
    app.run(host="0.0.0.0", port=5000, debug=False)
