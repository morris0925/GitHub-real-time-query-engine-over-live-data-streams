"""
processors/pull_request_event.py — Processor for PullRequestEvent

PullRequestEvent fires on PR open, close, reopen, edit, and several other
actions. It's one of the richer event types — the payload contains the full
PR object with title, body, labels, assignees, and more.

Payload structure (simplified):
    {
        "action": "opened",   ← "opened", "closed", "reopened", "edited", ...
        "number": 42,         ← PR number
        "pull_request": {
            "title":    "Fix the thing",
            "state":    "open",
            "merged":   false,
            "draft":    false,
            "additions": 10,
            "deletions":  3,
            "changed_files": 2,
            "base": {"ref": "main"},
            "head": {"ref": "fix/the-thing"},
        }
    }

What we validate:
    - payload.action must be present
    - payload.number must be present (the PR number)

What we extract as metrics:
    - action:         "opened" / "closed" / "merged" / ...
    - pr_number:      integer PR number
    - is_merged:      True if this is a "closed" + merged=True event
    - is_draft:       True if the PR is a draft
    - target_branch:  the base branch (e.g. "main")
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError


class PullRequestEventProcessor(EventProcessor):
    event_type = "PullRequestEvent"

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)

        if not payload.get("action"):
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.action is missing",
                event_id=str(event.get("id", "")),
            )
        if payload.get("number") is None:
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.number is missing",
                event_id=str(event.get("id", "")),
            )

        action: str = payload["action"]
        pr: dict = payload.get("pull_request") or {}

        is_merged = action == "closed" and bool(pr.get("merged"))

        metrics = {
            "action":        action,
            "pr_number":     payload["number"],
            "is_merged":     is_merged,
            "is_draft":      bool(pr.get("draft", False)),
            "target_branch": (pr.get("base") or {}).get("ref", ""),
            "additions":     pr.get("additions"),
            "deletions":     pr.get("deletions"),
            "changed_files": pr.get("changed_files"),
        }

        return ProcessorResult(event=event, metrics=metrics)
