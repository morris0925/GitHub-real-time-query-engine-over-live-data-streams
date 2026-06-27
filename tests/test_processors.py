"""
tests/test_processors.py — Unit tests for the processors/ layer

Tests cover:
  1. Registry — get_processor() returns the right class, falls back to default
  2. PushEventProcessor — valid event, missing ref, missing commits
  3. WatchEventProcessor — valid event, missing action
  4. PullRequestEventProcessor — valid event, merged detection, missing fields
  5. DefaultProcessor — unknown types pass through without error
  6. Base class helpers — _require() raises ValidationError correctly

Run with:
    pytest tests/test_processors.py -v
"""

import pytest

from processors import get_processor, REGISTRY, ValidationError, ProcessorResult
from processors.base import EventProcessor
from processors.push_event import PushEventProcessor
from processors.watch_event import WatchEventProcessor
from processors.pull_request_event import PullRequestEventProcessor
from processors.default import DefaultProcessor


# ── Fixtures / helpers ────────────────────────────────────────────────────────

def base_event(event_type: str = "PushEvent", event_id: str = "evt-1") -> dict:
    """Minimal valid event shell — subfields added per test."""
    return {
        "id":         event_id,
        "type":       event_type,
        "actor":      {"id": 1, "login": "alice"},
        "repo":       {"id": 99, "name": "alice/repo"},
        "public":     True,
        "created_at": "2026-06-27T10:00:00Z",
        "payload":    {},
    }


def push_event(**payload_overrides) -> dict:
    event = base_event("PushEvent")
    event["payload"] = {
        "ref":           "refs/heads/main",
        "size":          2,
        "distinct_size": 2,
        "commits":       [
            {"sha": "abc", "message": "fix bug"},
            {"sha": "def", "message": "add test"},
        ],
        **payload_overrides,
    }
    return event


def watch_event(**payload_overrides) -> dict:
    event = base_event("WatchEvent")
    event["payload"] = {"action": "started", **payload_overrides}
    return event


def pull_request_event(action: str = "opened", merged: bool = False, **payload_overrides) -> dict:
    event = base_event("PullRequestEvent")
    event["payload"] = {
        "action": action,
        "number": 42,
        "pull_request": {
            "title":         "Fix the thing",
            "state":         "open" if action != "closed" else "closed",
            "merged":        merged,
            "draft":         False,
            "additions":     10,
            "deletions":     3,
            "changed_files": 2,
            "base":          {"ref": "main"},
            "head":          {"ref": "fix/the-thing"},
        },
        **payload_overrides,
    }
    return event


# ── 1. Registry tests ─────────────────────────────────────────────────────────

class TestRegistry:
    def test_get_processor_returns_push_processor(self):
        assert isinstance(get_processor("PushEvent"), PushEventProcessor)

    def test_get_processor_returns_watch_processor(self):
        assert isinstance(get_processor("WatchEvent"), WatchEventProcessor)

    def test_get_processor_returns_pr_processor(self):
        assert isinstance(get_processor("PullRequestEvent"), PullRequestEventProcessor)

    def test_get_processor_falls_back_to_default(self):
        """Unknown event types should get the DefaultProcessor, not raise."""
        proc = get_processor("ForkEvent")
        assert isinstance(proc, DefaultProcessor)

    def test_get_processor_is_cached(self):
        """Same type → same instance (singleton behaviour)."""
        assert get_processor("PushEvent") is get_processor("PushEvent")

    def test_registry_contains_known_types(self):
        for t in ("PushEvent", "WatchEvent", "PullRequestEvent"):
            assert t in REGISTRY


# ── 2. PushEventProcessor ─────────────────────────────────────────────────────

class TestPushEventProcessor:
    def test_valid_event_returns_result(self):
        result = get_processor("PushEvent").process(push_event())
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_metrics_commit_count(self):
        result = get_processor("PushEvent").process(push_event())
        assert result.metrics["commit_count"] == 2

    def test_metrics_branch_stripped(self):
        """'refs/heads/main' should be reported as 'main'."""
        result = get_processor("PushEvent").process(push_event())
        assert result.metrics["branch"] == "main"

    def test_metrics_is_default_branch_true(self):
        result = get_processor("PushEvent").process(push_event(ref="refs/heads/main"))
        assert result.metrics["is_default_branch"] is True

    def test_metrics_is_default_branch_false(self):
        result = get_processor("PushEvent").process(push_event(ref="refs/heads/feature/foo"))
        assert result.metrics["is_default_branch"] is False

    def test_missing_ref_raises_validation_error(self):
        event = push_event()
        del event["payload"]["ref"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("PushEvent").process(event)
        assert "ref" in str(exc_info.value)

    def test_missing_commits_raises_validation_error(self):
        event = push_event()
        del event["payload"]["commits"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("PushEvent").process(event)
        assert "commits" in str(exc_info.value)

    def test_missing_top_level_id_raises_validation_error(self):
        event = push_event()
        del event["id"]
        with pytest.raises(ValidationError):
            get_processor("PushEvent").process(event)

    def test_empty_commits_list_is_valid(self):
        """An empty commit list is valid (force-push that rewrites history)."""
        result = get_processor("PushEvent").process(push_event(commits=[]))
        assert result.metrics["commit_count"] == 0


# ── 3. WatchEventProcessor ────────────────────────────────────────────────────

class TestWatchEventProcessor:
    def test_valid_event_returns_result(self):
        result = get_processor("WatchEvent").process(watch_event())
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_metrics_action(self):
        result = get_processor("WatchEvent").process(watch_event())
        assert result.metrics["action"] == "started"

    def test_missing_action_raises_validation_error(self):
        event = watch_event()
        del event["payload"]["action"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("WatchEvent").process(event)
        assert "action" in str(exc_info.value)

    def test_missing_actor_raises_validation_error(self):
        event = watch_event()
        del event["actor"]
        with pytest.raises(ValidationError):
            get_processor("WatchEvent").process(event)


# ── 4. PullRequestEventProcessor ─────────────────────────────────────────────

class TestPullRequestEventProcessor:
    def test_valid_open_event(self):
        result = get_processor("PullRequestEvent").process(pull_request_event("opened"))
        assert isinstance(result, ProcessorResult)
        assert result.metrics["action"] == "opened"

    def test_merged_event_detected(self):
        """A closed + merged=True PR should have is_merged=True."""
        result = get_processor("PullRequestEvent").process(
            pull_request_event("closed", merged=True)
        )
        assert result.metrics["is_merged"] is True

    def test_closed_not_merged(self):
        """Closed without merge (e.g. declined PR) should have is_merged=False."""
        result = get_processor("PullRequestEvent").process(
            pull_request_event("closed", merged=False)
        )
        assert result.metrics["is_merged"] is False

    def test_metrics_pr_number(self):
        result = get_processor("PullRequestEvent").process(pull_request_event())
        assert result.metrics["pr_number"] == 42

    def test_metrics_target_branch(self):
        result = get_processor("PullRequestEvent").process(pull_request_event())
        assert result.metrics["target_branch"] == "main"

    def test_missing_action_raises(self):
        event = pull_request_event()
        del event["payload"]["action"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("PullRequestEvent").process(event)
        assert "action" in str(exc_info.value)

    def test_missing_number_raises(self):
        event = pull_request_event()
        del event["payload"]["number"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("PullRequestEvent").process(event)
        assert "number" in str(exc_info.value)


# ── 5. DefaultProcessor ───────────────────────────────────────────────────────

class TestDefaultProcessor:
    def test_unknown_type_does_not_raise(self):
        event = base_event("ForkEvent")
        result = get_processor("ForkEvent").process(event)
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_create_event_passes_through(self):
        event = base_event("CreateEvent")
        event["payload"] = {"ref_type": "branch", "ref": "feature/x"}
        result = get_processor("CreateEvent").process(event)
        assert result.event is event   # same dict, untouched

    def test_missing_id_raises_even_for_default(self):
        """Even the default processor requires 'id'."""
        event = base_event("ForkEvent")
        del event["id"]
        with pytest.raises(ValidationError):
            get_processor("ForkEvent").process(event)


# ── 6. ValidationError structure ─────────────────────────────────────────────

class TestValidationError:
    def test_error_includes_event_type(self):
        err = ValidationError(event_type="PushEvent", reason="missing ref", event_id="e1")
        assert "PushEvent" in str(err)

    def test_error_includes_reason(self):
        err = ValidationError(event_type="PushEvent", reason="missing ref", event_id="e1")
        assert "missing ref" in str(err)

    def test_error_is_exception(self):
        err = ValidationError(event_type="PushEvent", reason="oops")
        assert isinstance(err, Exception)
