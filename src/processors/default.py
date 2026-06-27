"""
processors/default.py — Fallback processor for unknown event types

GitHub has 30+ event types. We write dedicated processors for the most common
ones (PushEvent, WatchEvent, PullRequestEvent). For everything else — ForkEvent,
CreateEvent, DeleteEvent, IssuesEvent, etc. — this processor handles them.

Strategy: pass-through with minimal validation.
We still require the core fields (id, type, actor, repo) that every event
must have. We don't touch the payload — it's stored as-is in payload_json.

When to add a dedicated processor:
    If you notice a high volume of a particular event type in the dashboard's
    Event Types panel, consider writing a dedicated processor for it. Good
    candidates: IssuesEvent, CreateEvent, ForkEvent.
"""

from processors.base import EventProcessor, ProcessorResult


class DefaultProcessor(EventProcessor):
    """Handles any event type that doesn't have a dedicated processor."""

    event_type = "default"

    def process(self, event: dict) -> ProcessorResult:
        # Validate only the universal fields every GitHub event must have
        self._require(event, "id", "type")

        metrics = {"event_type": event.get("type", "unknown")}
        return ProcessorResult(event=event, metrics=metrics)
