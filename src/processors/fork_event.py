"""
processors/fork_event.py — Processor for ForkEvent

ForkEvent fires when a user forks a repository. It's a reliable signal
that someone found the project interesting enough to fork (stronger than
a star/WatchEvent, which is passive).

Payload structure:
    {
        "forkee": {
            "id":        987654,
            "name":      "myrepo",
            "full_name": "bob/myrepo",
            "owner":     {"login": "bob"},
            "private":   false,
            "fork":      true,
        }
    }

What we validate:
    - payload.forkee must be present (the newly-created fork)
    - payload.forkee.full_name must be present (who forked where)

What we extract as metrics:
    - fork_full_name: "bob/myrepo" — the new fork's full name
    - fork_owner:     "bob" — who created the fork
    - is_private:     True if the fork was made private (unusual)

Interview note:
    ForkEvent is interesting analytically — a spike in ForkEvent volume for
    a repo usually means it got featured somewhere (Hacker News, newsletters).
    Unlike WatchEvent (stars), forks suggest the user plans to *contribute*
    or *build on* the project.
"""

from processors.base import EventProcessor, ProcessorResult, ValidationError


class ForkEventProcessor(EventProcessor):
    event_type = "ForkEvent"

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)

        forkee: dict = payload.get("forkee") or {}
        if not forkee:
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.forkee is missing",
                event_id=str(event.get("id", "")),
            )
        if not forkee.get("full_name"):
            raise ValidationError(
                event_type=self.event_type,
                reason="payload.forkee.full_name is missing",
                event_id=str(event.get("id", "")),
            )

        owner: dict = forkee.get("owner") or {}

        metrics = {
            "fork_full_name": forkee["full_name"],
            "fork_owner":     owner.get("login", ""),
            "is_private":     bool(forkee.get("private", False)),
        }

        return ProcessorResult(event=event, metrics=metrics)
