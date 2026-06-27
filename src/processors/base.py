"""
processors/base.py — Abstract base class for event processors

Why a processors/ layer?
─────────────────────────
The GitHub Events API returns 30+ different event types. Each has its own
payload structure — a PushEvent has commits, a WatchEvent has an action field,
a PullRequestEvent has a full PR object. If we shove all of this into the
consumer loop, it becomes a tangle of if/elif branches.

The processor pattern separates concerns:

    consumer.py            →  "read from Kafka, batch, commit"
    processors/<Type>.py   →  "understand this specific event type"
    storage/writer.py      →  "write rows to Parquet"

Each processor is responsible for exactly one event type:
  1. VALIDATE  — check the event has the fields we expect
  2. ENRICH    — pull useful fields out of the nested payload into a flat dict
                 so the writer doesn't have to deal with event-specific logic

The writer still uses the same flat schema (schema.py). Enriched data goes
into `payload_json` as before — but now it's been validated and optionally
pre-processed by the relevant processor.

Design: Abstract Base Class (ABC)
──────────────────────────────────
Python's `abc.ABC` + `@abstractmethod` lets us define a contract that all
processors must follow. If someone writes a new processor and forgets to
implement `process()`, Python raises a TypeError at import time — not at
runtime when a real event arrives. This is "fail fast."

    class EventProcessor(ABC):
        @abstractmethod
        def process(self, event: dict) -> dict: ...

    class PushEventProcessor(EventProcessor):
        def process(self, event: dict) -> dict: ...  # must implement this
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


class ValidationError(Exception):
    """
    Raised by a processor when a required field is missing or malformed.

    The consumer catches this and decides whether to skip the event or send
    it to a dead-letter queue. We do NOT raise for mildly odd events (e.g.
    a missing optional field) — only for events that are structurally broken.
    """
    def __init__(self, event_type: str, reason: str, event_id: str = ""):
        self.event_type = event_type
        self.reason = reason
        self.event_id = event_id
        super().__init__(f"[{event_type}] id={event_id!r}: {reason}")


@dataclass
class ProcessorResult:
    """
    What a processor returns after handling one event.

    Fields:
        event:    The original event dict, possibly with enrichments added
                  under the "_enriched" key (consumer may log these).
        metrics:  Extracted numeric/categorical values useful for monitoring.
                  e.g. {"commit_count": 3, "is_fork": False}
                  These are NOT written to Parquet — they're for logging/alerting.
        skipped:  True if the event was valid but intentionally not stored
                  (e.g. a bot account we want to filter out).
    """
    event:   dict
    metrics: dict = field(default_factory=dict)
    skipped: bool = False


class EventProcessor(ABC):
    """
    Abstract base class that all event-type processors must inherit from.

    Subclasses implement `process()` for their specific event type.
    The `event_type` class attribute is used by the registry to route
    incoming events to the right processor.
    """

    #: Override in each subclass, e.g. event_type = "PushEvent"
    event_type: str = ""

    @abstractmethod
    def process(self, event: dict) -> ProcessorResult:
        """
        Validate and enrich one raw GitHub event dict.

        Args:
            event: Raw event dict from the GitHub Events API (via Kafka).

        Returns:
            ProcessorResult with the (possibly enriched) event and metrics.

        Raises:
            ValidationError: If a required field is absent or malformed.
        """

    # ── Shared helpers available to all subclasses ─────────────────────────

    def _require(self, event: dict, *keys: str) -> None:
        """
        Assert that every key in `keys` is present and non-empty in `event`.

        Usage inside a processor:
            self._require(event, "id", "type", "actor")
            self._require(event["payload"], "commits")

        Raises:
            ValidationError if any key is missing or None.
        """
        for key in keys:
            if event.get(key) is None:
                raise ValidationError(
                    event_type=self.event_type,
                    reason=f"required field missing: '{key}'",
                    event_id=str(event.get("id", "")),
                )

    def _payload(self, event: dict) -> dict:
        """Return the event's payload dict (defaulting to {})."""
        return event.get("payload") or {}
