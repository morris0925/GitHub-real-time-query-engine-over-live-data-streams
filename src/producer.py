"""
Day 3: GitHub Events API Producer → Kafka

Goal: Poll the GitHub Events API every 5 seconds and publish each event
      to a Kafka topic called "github-events".

New concepts vs Day 2:
- KafkaProducer: A client that connects to Kafka and sends messages
- Topic: A named channel in Kafka. Think of it like a named queue.
  Producers write to topics; consumers read from topics.
- Message key: Optional identifier for a message. We use event["id"] as
  the key so Kafka can route related events to the same partition.
- Serialization: Kafka only stores bytes. We convert our Python dicts
  to JSON strings, then encode to bytes before sending.
"""

import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


GITHUB_EVENTS_URL = "https://api.github.com/events"
POLL_INTERVAL = 5
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "github-events"


def create_producer() -> KafkaProducer:
    """
    Connect to Kafka and return a producer.

    value_serializer: Automatically converts each message (a Python dict)
    to JSON bytes before sending. We don't have to call json.dumps() manually.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Also serialize the key (event ID) to bytes
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def fetch_events(etag: str | None) -> tuple[list[dict], str | None]:
    """
    Fetch the latest events from GitHub Events API.
    Same as Day 2 — returns (events, new_etag).
    """
    headers = {
        "Accept": "application/vnd.github.v3+json",
        **({"If-None-Match": etag} if etag else {}),
    }

    response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)

    if response.status_code == 304:
        return [], etag

    response.raise_for_status()
    return response.json(), response.headers.get("ETag")


def main():
    print("=== StreamLens GitHub Producer (Day 3) ===")
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")

    # Connect to Kafka — fail loudly if broker is not running
    try:
        producer = create_producer()
        print(f"Connected! Publishing to topic: {KAFKA_TOPIC}\n")
    except NoBrokersAvailable:
        print("[error] Cannot connect to Kafka. Is docker-compose up?")
        return

    etag = None
    poll_count = 0

    while True:
        poll_count += 1
        print(f"--- Poll #{poll_count} ---")

        try:
            events, etag = fetch_events(etag)

            if events:
                print(f"[poll] Received {len(events)} events → sending to Kafka")
                for event in events:
                    # Send each event as a separate Kafka message
                    # key=event ID, value=full event dict
                    producer.send(
                        KAFKA_TOPIC,
                        key=event.get("id"),
                        value=event,
                    )
                    print(f"  → {event['type']:20s} | {event['repo']['name']}")

                # Flush ensures all buffered messages are actually sent
                producer.flush()
                print(f"[kafka] {len(events)} messages sent to '{KAFKA_TOPIC}'")

            else:
                print("[poll] No new events (304)")

        except requests.exceptions.ConnectionError:
            print("[error] Cannot connect to GitHub API.")
        except requests.exceptions.Timeout:
            print("[error] Request timed out.")
        except requests.exceptions.HTTPError as e:
            print(f"[error] HTTP error: {e}")

        print(f"[poll] Waiting {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
