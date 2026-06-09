"""
Day 2: GitHub Events API Producer

Goal: Poll the GitHub Events API every 5 seconds and print raw JSON events.

Concepts:
- API polling: Repeatedly asking an API for new data at a fixed interval
- GitHub Events API: Public endpoint that returns recent events (push, star, fork, etc.)
  URL: https://api.github.com/events
  No authentication required, returns up to 30 events per request
- ETag: A header GitHub sends back that lets us avoid re-downloading the same data
  If nothing has changed, GitHub returns 304 (Not Modified) and saves bandwidth
"""

import requests
import json
import time


# The API endpoint we'll be polling
GITHUB_EVENTS_URL = "https://api.github.com/events"

# How many seconds to wait between each API call
POLL_INTERVAL = 5


def fetch_events(etag: str | None) -> tuple[list[dict], str | None]:
    """
    Fetch the latest events from GitHub Events API.

    Args:
        etag: The ETag from the previous response. If provided, GitHub will
              return 304 (Not Modified) if there are no new events, saving
              us from downloading the same data twice.

    Returns:
        A tuple of (events_list, new_etag).
        - events_list: List of event dicts (empty if no new events)
        - new_etag: The ETag from this response (pass it to the next call)
    """
    headers = {
        # Tell GitHub we accept JSON
        "Accept": "application/vnd.github.v3+json",
        # If we have an ETag from last time, send it — GitHub will skip
        # sending the full response if nothing changed (saves bandwidth)
        **({"If-None-Match": etag} if etag else {}),
    }

    response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)

    # 304 means "nothing changed since your last request" — no new events
    if response.status_code == 304:
        print("[poll] No new events (304 Not Modified)")
        return [], etag

    # Raise an exception for any other error (4xx, 5xx)
    response.raise_for_status()

    # Extract the new ETag to use in our next request
    new_etag = response.headers.get("ETag")

    # Parse the JSON response body into a Python list of dicts
    events = response.json()

    return events, new_etag


def main():
    """
    Main polling loop.
    Runs forever: fetch events → print → wait 5 seconds → repeat.
    Press Ctrl+C to stop.
    """
    print("=== StreamLens GitHub Producer (Day 2) ===")
    print(f"Polling {GITHUB_EVENTS_URL} every {POLL_INTERVAL} seconds")
    print("Press Ctrl+C to stop\n")

    etag = None       # Will store the ETag from the last response
    poll_count = 0    # Track how many polls we've done

    while True:
        poll_count += 1
        print(f"--- Poll #{poll_count} ---")

        try:
            events, etag = fetch_events(etag)

            if events:
                print(f"[poll] Received {len(events)} events")
                # Print each event as formatted JSON
                for event in events:
                    print(json.dumps(event, indent=2))
                    print()  # Blank line between events for readability
            else:
                print("[poll] 0 new events")

        except requests.exceptions.ConnectionError:
            print("[error] Cannot connect to GitHub API. Check your internet.")
        except requests.exceptions.Timeout:
            print("[error] Request timed out. Will retry next poll.")
        except requests.exceptions.HTTPError as e:
            print(f"[error] HTTP error: {e}")

        print(f"[poll] Waiting {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
