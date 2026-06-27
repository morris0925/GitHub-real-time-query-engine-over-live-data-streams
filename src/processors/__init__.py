"""
processors/ — Event-type-specific validation and enrichment

Public API:
    get_processor(event_type)  →  EventProcessor instance for that type
    REGISTRY                   →  dict mapping event_type string → processor class

Usage in consumer.py:
    from processors import get_processor
    from processors.base import ValidationError

    result = get_processor(event["type"]).process(event)
    if not result.skipped:
        write_batch([result.event], data_dir=DATA_DIR)

How the registry works:
    REGISTRY maps event type strings to processor classes. When a new event
    arrives, get_processor() looks up the right class by type name, falls back
    to DefaultProcessor if the type isn't registered, and returns an instance.

    This is the "Strategy" design pattern — the algorithm (processing logic)
    is selected at runtime based on the event type string.

Adding a new processor:
    1. Create src/processors/my_event.py with class MyEventProcessor(EventProcessor)
    2. Add it to REGISTRY below: "MyEvent": MyEventProcessor
    3. Add tests to tests/test_processors.py
    That's it. consumer.py doesn't need to change.
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError
from processors.push_event import PushEventProcessor
from processors.watch_event import WatchEventProcessor
from processors.pull_request_event import PullRequestEventProcessor
from processors.default import DefaultProcessor

# ── Registry ──────────────────────────────────────────────────────────────────
# Maps GitHub event type strings → processor classes.
# DefaultProcessor is the fallback for any type not listed here.

REGISTRY: dict[str, type[EventProcessor]] = {
    "PushEvent":          PushEventProcessor,
    "WatchEvent":         WatchEventProcessor,
    "PullRequestEvent":   PullRequestEventProcessor,
}

# Singleton instances — we only need one per type, no state between calls
_instances: dict[str, EventProcessor] = {}


def get_processor(event_type: str) -> EventProcessor:
    """
    Return the processor for the given event type.

    Falls back to DefaultProcessor for unknown types so the pipeline
    never drops an event just because we haven't written a processor for it.

    Args:
        event_type: GitHub event type string, e.g. "PushEvent".

    Returns:
        An EventProcessor instance (cached — same instance reused each call).

    Example:
        processor = get_processor("PushEvent")
        result = processor.process(raw_event)
    """
    if event_type not in _instances:
        cls = REGISTRY.get(event_type, DefaultProcessor)
        _instances[event_type] = cls()
    return _instances[event_type]


__all__ = [
    "EventProcessor",
    "ProcessorResult",
    "ValidationError",
    "REGISTRY",
    "get_processor",
]
