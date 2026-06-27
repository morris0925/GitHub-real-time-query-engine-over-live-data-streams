"""
GitHub Events API Producer → Kafka

Polls the GitHub Events API every POLL_INTERVAL seconds and publishes
each event to a Kafka topic as a separate message.

Configuration (via .env or environment variables):
    KAFKA_BROKER       Kafka bootstrap server  (default: localhost:9092)
    KAFKA_TOPIC        Topic to publish to     (default: github-events)
    POLL_INTERVAL      Seconds between polls   (default: 5)
    GITHUB_TOKEN       Optional personal access token — raises rate limit
                       from 60 req/hr (unauthenticated) to 5000 req/hr
"""

import os
import json
import time

import requests
import structlog
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

GITHUB_EVENTS_URL: str = "https://api.github.com/events"
KAFKA_BROKER:      str = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC:       str = os.getenv("KAFKA_TOPIC", "github-events")
POLL_INTERVAL:     int = int(os.getenv("POLL_INTERVAL", "5"))
GITHUB_TOKEN:      str | None = os.getenv("GITHUB_TOKEN")


# ── Kafka setup ───────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """
    Connect to Kafka and return a producer.

    value_serializer: automatically converts each Python dict to JSON bytes.
    key_serializer:   encodes the event ID string to bytes for use as a key.

    Using event ID as the message key means Kafka routes all events with the
    same ID to the same partition — useful if you ever need ordered processing
    per event.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


# ── GitHub API ────────────────────────────────────────────────────────────────

def fetch_events(etag: str | None) -> tuple[list[dict], str | None]:
    """
    Fetch the latest public events from the GitHub Events API.

    Uses ETags for conditional requests: if the data hasn't changed since
    our last poll, GitHub returns 304 Not Modified with an empty body.
    This avoids wasting our rate-limit quota on unchanged data.

    Args:
        etag: The ETag value from the previous response, or None on first call.

    Returns:
        (events, new_etag): Empty list if nothing changed (304).
    """
    headers: dict[str, str] = {
        "Accept": "application/vnd.github.v3+json",
        **({"If-None-Match": etag} if etag else {}),
        **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}),
    }

    response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)

    if response.status_code == 304:
        return [], etag

    response.raise_for_status()
    return response.json(), response.headers.get("ETag")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("producer_starting", broker=KAFKA_BROKER, topic=KAFKA_TOPIC, poll_interval=POLL_INTERVAL)

    try:
        producer = create_producer()
        log.info("producer_connected", broker=KAFKA_BROKER)
    except NoBrokersAvailable:
        log.error("no_brokers", broker=KAFKA_BROKER, hint="Is docker-compose up?")
        return

    etag: str | None = None
    poll_count: int = 0

    while True:
        poll_count += 1
        log.debug("polling", poll=poll_count)

        try:
            events, etag = fetch_events(etag)

            if events:
                for event in events:
                    producer.send(
                        KAFKA_TOPIC,
                        key=event.get("id"),
                        value=event,
                    )
                producer.flush()
                log.info("events_published", count=len(events), topic=KAFKA_TOPIC)

            else:
                log.debug("no_new_events", reason="304 Not Modified")

        except requests.exceptions.ConnectionError:
            log.warning("github_connection_error")
        except requests.exceptions.Timeout:
            log.warning("github_timeout")
        except requests.exceptions.HTTPError as exc:
            log.warning("github_http_error", status=exc.response.status_code)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
