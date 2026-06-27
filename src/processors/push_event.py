"""
processors/push_event.py — Processor for PushEvent

PushEvent is the most common GitHub event type. It fires every time
someone pushes commits to a repository.

Payload structure (simplified):
    {
        "push_id": 12345,
        "size": 3,           ← number of commits in this push
        "distinct_size": 3,  ← number of NEW commits (size - force-pushed)
        "ref": "refs/heads/main",
        "commits": [
            {"sha": "abc123", "message": "fix bug", "author": {...}},
            ...
        ]
    }

What we validate:
    - payload.ref must be present (which branch was pushed to)
    - payload.commits must be a list (even if empty, GitHub sends [])

What we extract as metrics:
    - commit_count: how many commits in this push
    - branch:       short branch name (strips "refs/heads/" prefix)
    - is_default_branch: True if pushed to main/master
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError


class PushEventProcessor(EventProcessor):
    event_type = "PushEvent"

    # Branches commonly considered "default" — used for the is_default_branch metric
    _DEFAULT_BRANCHES = {"main", "master", "trunk", "develop"}

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)

        # ref is required — without it we can't tell which branch was pushed
        if not payload.get("ref"):
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.ref is missing",
                event_id=str(event.get("id", "")),
            )

        # GitHub's public events API omits commits for large pushes (>20 commits)
        # or force-pushes. Treat missing/null as empty list instead of an error.
        commits = payload.get("commits") or []

        # Strip "refs/heads/" to get a readable branch name
        ref: str = payload["ref"]
        branch = ref.removeprefix("refs/heads/").removeprefix("refs/tags/")

        metrics = {
            "commit_count":      len(commits),
            "branch":            branch,
            "is_default_branch": branch in self._DEFAULT_BRANCHES,
            "distinct_size":     payload.get("distinct_size", len(commits)),
        }

        return ProcessorResult(event=event, metrics=metrics)
