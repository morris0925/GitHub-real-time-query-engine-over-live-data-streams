"""
tests/test_producer.py — Unit tests for producer.py stream-scope selection

The producer itself (Kafka loop) is exercised live; what's unit-testable is
the endpoint selection: global firehose vs single-repo events feed.

Run with:
    PYTHONPATH=src pytest tests/test_producer.py -v
"""

from producer import events_url


def test_events_url_global_mode() -> None:
    assert events_url(mode="global", repo="acme/widgets") == "https://api.github.com/events"


def test_events_url_repo_mode() -> None:
    assert (
        events_url(mode="repo", repo="kubernetes/kubernetes")
        == "https://api.github.com/repos/kubernetes/kubernetes/events"
    )


def test_events_url_defaults_resolve() -> None:
    # Whatever the env says, the default call must produce a valid endpoint.
    url = events_url()
    assert url.startswith("https://api.github.com/")
    assert url.endswith("/events")
