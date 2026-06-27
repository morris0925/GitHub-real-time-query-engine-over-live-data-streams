"""
processors/create_event.py — Processor for CreateEvent

CreateEvent fires when a new branch, tag, or repository is created.
It's useful for spotting release activity (new tags = new versions)
and development patterns (how often are branches created?).

Payload structure:
    {
        "ref":        "v1.2.3",          ← the name of the branch/tag created
                                            null if ref_type is "repository"
        "ref_type":   "tag",             ← "branch", "tag", or "repository"
        "master_branch": "main",         ← the default branch of the repo
        "description":   "My cool repo", ← repo description (for repository type)
    }

What we validate:
    - payload.ref_type must be one of "branch", "tag", "repository"

What we extract as metrics:
    - ref_type:      "branch" / "tag" / "repository"
    - ref_name:      the name of the created ref (empty for new repos)
    - is_tag:        True if this looks like a version release
    - is_semver_tag: True if ref follows vX.Y.Z convention (release signal)

Interview note:
    Tagging a release is a strong signal: CreateEvent with ref_type="tag"
    and ref matching vN.N.N means a team just shipped something. Aggregating
    these lets you track release cadence per repo over time.
"""

import re

from processors.base import EventProcessor, ProcessorResult, ValidationError

# Regex for semantic-version tags: v1.2.3, v10.0.0-beta, 2.1.0, etc.
_SEMVER_RE = re.compile(r"^v?\d+\.\d+(\.\d+)?", re.IGNORECASE)

_VALID_REF_TYPES = {"branch", "tag", "repository"}


class CreateEventProcessor(EventProcessor):
    event_type = "CreateEvent"

    def process(self, event: dict) -> ProcessorResult:
        self._require(event, "id", "type", "actor", "repo")

        payload = self._payload(event)

        ref_type: str = payload.get("ref_type", "")
        if ref_type not in _VALID_REF_TYPES:
            raise ValidationError(
                event_type=self.event_type,
                reason=f"payload.ref_type invalid or missing: {ref_type!r}",
                event_id=str(event.get("id", "")),
            )

        ref_name: str = payload.get("ref") or ""
        is_tag = ref_type == "tag"
        is_semver_tag = is_tag and bool(_SEMVER_RE.match(ref_name))

        metrics = {
            "ref_type":      ref_type,
            "ref_name":      ref_name,
            "is_tag":        is_tag,
            "is_semver_tag": is_semver_tag,
        }

        return ProcessorResult(event=event, metrics=metrics)
