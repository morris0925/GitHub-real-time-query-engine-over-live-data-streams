"""
processors/issues_event.py — Processor for IssuesEvent

IssuesEvent fires when an issue is opened, closed, reopened, edited,
labelled, unlabelled, assigned, or unassigned. It's a high-volume event
type on active repositories.

Payload structure (simplified):
    {
        "action": "opened",   ← "opened", "closed", "reopened", "edited",
                                 "labeled", "unlabeled", "assigned", "unassigned"
        "issue": {
            "number":     42,
            "title":      "Bug: segfault on startup",
            "state":      "open",
            "comments":   3,
            "body":       "...",
            "labels":     [{"name": "bug"}, {"name": "urgent"}],
            "user":       {"login": "alice"},
        }
    }

What we validate:
    - payload.action must be present (which lifecycle event happened)
    - payload.issue must be present and contain a number

What we extract as metrics:
    - action:        "opened" / "closed" / "reopened" / etc.
    - issue_number:  integer issue number
    - is_closed:     True if this action marks the issue as done
    - label_count:   how many labels are on the issue (0 if none)
    - comment_count: existing comment count at time of event
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError


class IssuesEventProcessor(EventProcessor):
    event_type = "IssuesEvent"

    # Actions that close an issue (resolved, done, won't fix all use "closed")
    _CLOSING_ACTIONS = {"closed"}

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)

        if not payload.get("action"):
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.action is missing",
                event_id=str(event.get("id", "")),
            )

        issue: dict = payload.get("issue") or {}
        if issue.get("number") is None:
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.issue.number is missing",
                event_id=str(event.get("id", "")),
            )

        action: str = payload["action"]
        labels: list = issue.get("labels") or []

        metrics = {
            "action":        action,
            "issue_number":  issue["number"],
            "is_closed":     action in self._CLOSING_ACTIONS,
            "label_count":   len(labels),
            "comment_count": issue.get("comments", 0),
        }

        return ProcessorResult(event=event, metrics=metrics)
