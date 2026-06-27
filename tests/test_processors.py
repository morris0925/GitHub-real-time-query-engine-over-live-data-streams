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
from processors.issues_event import IssuesEventProcessor
from processors.fork_event import ForkEventProcessor
from processors.create_event import CreateEventProcessor
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

    def test_get_processor_returns_issues_processor(self):
        assert isinstance(get_processor("IssuesEvent"), IssuesEventProcessor)

    def test_get_processor_returns_fork_processor(self):
        assert isinstance(get_processor("ForkEvent"), ForkEventProcessor)

    def test_get_processor_returns_create_processor(self):
        assert isinstance(get_processor("CreateEvent"), CreateEventProcessor)

    def test_get_processor_falls_back_to_default(self):
        """Truly unknown event types (not in REGISTRY) → DefaultProcessor."""
        proc = get_processor("GollumEvent")
        assert isinstance(proc, DefaultProcessor)

    def test_get_processor_is_cached(self):
        """Same type → same instance (singleton behaviour)."""
        assert get_processor("PushEvent") is get_processor("PushEvent")

    def test_registry_contains_known_types(self):
        for t in ("PushEvent", "WatchEvent", "PullRequestEvent",
                  "IssuesEvent", "ForkEvent", "CreateEvent"):
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


# ── 5. IssuesEventProcessor ───────────────────────────────────────────────────

def issues_event(action: str = "opened", issue_number: int = 42, **overrides) -> dict:
    event = base_event("IssuesEvent")
    event["payload"] = {
        "action": action,
        "issue": {
            "number":   issue_number,
            "title":    "Bug: segfault",
            "state":    "open",
            "comments": 3,
            "labels":   [{"name": "bug"}, {"name": "urgent"}],
        },
        **overrides,
    }
    return event


class TestIssuesEventProcessor:
    def test_valid_event_returns_result(self):
        result = get_processor("IssuesEvent").process(issues_event())
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_metrics_action(self):
        result = get_processor("IssuesEvent").process(issues_event("opened"))
        assert result.metrics["action"] == "opened"

    def test_metrics_issue_number(self):
        result = get_processor("IssuesEvent").process(issues_event(issue_number=99))
        assert result.metrics["issue_number"] == 99

    def test_metrics_is_closed_true(self):
        result = get_processor("IssuesEvent").process(issues_event("closed"))
        assert result.metrics["is_closed"] is True

    def test_metrics_is_closed_false_for_opened(self):
        result = get_processor("IssuesEvent").process(issues_event("opened"))
        assert result.metrics["is_closed"] is False

    def test_metrics_label_count(self):
        result = get_processor("IssuesEvent").process(issues_event())
        assert result.metrics["label_count"] == 2

    def test_metrics_comment_count(self):
        result = get_processor("IssuesEvent").process(issues_event())
        assert result.metrics["comment_count"] == 3

    def test_missing_action_raises(self):
        event = issues_event()
        del event["payload"]["action"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("IssuesEvent").process(event)
        assert "action" in str(exc_info.value)

    def test_missing_issue_number_raises(self):
        event = issues_event()
        del event["payload"]["issue"]["number"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("IssuesEvent").process(event)
        assert "number" in str(exc_info.value)

    def test_empty_labels_is_valid(self):
        event = issues_event()
        event["payload"]["issue"]["labels"] = []
        result = get_processor("IssuesEvent").process(event)
        assert result.metrics["label_count"] == 0


# ── 6. ForkEventProcessor ─────────────────────────────────────────────────────

def fork_event(**forkee_overrides) -> dict:
    event = base_event("ForkEvent")
    event["payload"] = {
        "forkee": {
            "id":        987654,
            "name":      "myrepo",
            "full_name": "bob/myrepo",
            "owner":     {"login": "bob"},
            "private":   False,
            "fork":      True,
            **forkee_overrides,
        }
    }
    return event


class TestForkEventProcessor:
    def test_valid_event_returns_result(self):
        result = get_processor("ForkEvent").process(fork_event())
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_metrics_fork_full_name(self):
        result = get_processor("ForkEvent").process(fork_event())
        assert result.metrics["fork_full_name"] == "bob/myrepo"

    def test_metrics_fork_owner(self):
        result = get_processor("ForkEvent").process(fork_event())
        assert result.metrics["fork_owner"] == "bob"

    def test_metrics_is_private_false(self):
        result = get_processor("ForkEvent").process(fork_event())
        assert result.metrics["is_private"] is False

    def test_metrics_is_private_true(self):
        result = get_processor("ForkEvent").process(fork_event(private=True))
        assert result.metrics["is_private"] is True

    def test_missing_forkee_raises(self):
        event = fork_event()
        del event["payload"]["forkee"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("ForkEvent").process(event)
        assert "forkee" in str(exc_info.value)

    def test_missing_forkee_full_name_raises(self):
        event = fork_event()
        del event["payload"]["forkee"]["full_name"]
        with pytest.raises(ValidationError) as exc_info:
            get_processor("ForkEvent").process(event)
        assert "full_name" in str(exc_info.value)


# ── 7. CreateEventProcessor ───────────────────────────────────────────────────

def create_event(ref_type: str = "branch", ref: str = "feature/x") -> dict:
    event = base_event("CreateEvent")
    event["payload"] = {
        "ref_type":      ref_type,
        "ref":           ref,
        "master_branch": "main",
    }
    return event


class TestCreateEventProcessor:
    def test_valid_branch_event(self):
        result = get_processor("CreateEvent").process(create_event("branch", "feature/x"))
        assert isinstance(result, ProcessorResult)
        assert result.metrics["ref_type"] == "branch"
        assert result.metrics["is_tag"] is False

    def test_valid_tag_event(self):
        result = get_processor("CreateEvent").process(create_event("tag", "v1.2.3"))
        assert result.metrics["is_tag"] is True

    def test_semver_tag_detected(self):
        result = get_processor("CreateEvent").process(create_event("tag", "v1.2.3"))
        assert result.metrics["is_semver_tag"] is True

    def test_non_semver_tag_not_flagged(self):
        result = get_processor("CreateEvent").process(create_event("tag", "hotfix-login"))
        assert result.metrics["is_semver_tag"] is False

    def test_semver_without_v_prefix(self):
        result = get_processor("CreateEvent").process(create_event("tag", "2.0.0"))
        assert result.metrics["is_semver_tag"] is True

    def test_repository_ref_type_is_valid(self):
        event = create_event("repository", "")
        result = get_processor("CreateEvent").process(event)
        assert result.metrics["ref_type"] == "repository"

    def test_invalid_ref_type_raises(self):
        event = create_event("unknown_type", "foo")
        with pytest.raises(ValidationError) as exc_info:
            get_processor("CreateEvent").process(event)
        assert "ref_type" in str(exc_info.value)

    def test_missing_ref_type_raises(self):
        event = base_event("CreateEvent")
        event["payload"] = {}   # no ref_type at all
        with pytest.raises(ValidationError):
            get_processor("CreateEvent").process(event)


# ── 8. DefaultProcessor ───────────────────────────────────────────────────────

class TestDefaultProcessor:
    def test_truly_unknown_type_does_not_raise(self):
        """GollumEvent (wiki edit) is not in REGISTRY → DefaultProcessor."""
        event = base_event("GollumEvent")
        result = get_processor("GollumEvent").process(event)
        assert isinstance(result, ProcessorResult)
        assert not result.skipped

    def test_unknown_event_passes_through_unchanged(self):
        event = base_event("MemberEvent")
        event["payload"] = {"action": "added"}
        result = get_processor("MemberEvent").process(event)
        assert result.event is event   # same dict, untouched

    def test_missing_id_raises_even_for_default(self):
        """Even the default processor requires 'id'."""
        event = base_event("GollumEvent")
        del event["id"]
        with pytest.raises(ValidationError):
            get_processor("GollumEvent").process(event)


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
