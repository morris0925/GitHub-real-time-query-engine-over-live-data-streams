"""
processors/watch_event.py — Processor for WatchEvent

WatchEvent fires when someone stars a repository. Despite the name ("watch"),
the GitHub API only sends this for starring — not for watching (subscribing).

Payload structure:
    {
        "action": "started"   ← always "started" (un-starring doesn't fire an event)
    }

What we validate:
    - payload.action must be "started" (anything else would be unexpected)

What we extract as metrics:
    - action: the action string (always "started" in practice)
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError


class WatchEventProcessor(EventProcessor):
    event_type = "WatchEvent"

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)
        action = payload.get("action")

        if not action:
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.action is missing",
                event_id=str(event.get("id", "")),
            )

        metrics = {"action": action}
        return ProcessorResult(event=event, metrics=metrics)
