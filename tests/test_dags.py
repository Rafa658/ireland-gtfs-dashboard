import subprocess
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from prefect.client.schemas.objects import ConcurrencyLimitStrategy
from prefect.states import Completed

from dags import poll_gtfs as poll_module
from dags import dbt as dbt_module
from dags.dbt import PROJECT_ROOT, dbt_ci, dbt_prod, dbt_run, git_pull, publish_manifest
from dags.poll_gtfs import poll_gtfs
from dags.serve import build_deployments
from poller.api_client import ApiPayload

POLL_ENV = {
    "API_KEY": "secret-key",
    "POLL_INTERVAL_SECONDS": "120",
    "API_TIMEOUT_SECONDS": "30",
    "POSTGRES_CONNECT_TIMEOUT_SECONDS": "10",
    "POSTGRES_HOST": "db.internal",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "gtfs",
    "POSTGRES_USER": "gtfs",
    "POSTGRES_PASSWORD": "secret",
    "POSTGRES_SCHEMA": "gtfs",
    "POSTGRES_TABLE": "snapshots",
    "POSTGRES_SSLMODE": "prefer",
}


@pytest.fixture
def poll_env(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    for key, value in POLL_ENV.items():
        monkeypatch.setenv(key, value)
    logger = MagicMock()
    monkeypatch.setattr(poll_module, "get_run_logger", lambda: logger)
    monkeypatch.setattr(poll_module, "_lateness_seconds", lambda: 0.5)
    return logger


def stub_poll_dependencies(
    monkeypatch: pytest.MonkeyPatch, payload: ApiPayload | Exception
) -> MagicMock:
    repository = MagicMock()
    client = MagicMock()
    if isinstance(payload, Exception):
        client.fetch.side_effect = payload
    else:
        client.fetch.return_value = payload
    monkeypatch.setattr(poll_module, "PostgresRepository", lambda _config: repository)
    monkeypatch.setattr(poll_module, "ApiClient", lambda _key, _timeout: client)
    return repository


def test_poll_stores_one_snapshot_per_run(
    poll_env: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = ApiPayload(header={"version": "2"}, entity=[], entity_present=True)
    repository = stub_poll_dependencies(monkeypatch, payload)

    result = poll_gtfs.fn()

    repository.ensure_storage.assert_called_once()
    (ingested_at, stored), _ = repository.insert.call_args
    assert stored is payload
    assert result == {"ingested_at": ingested_at.isoformat()}


def test_poll_failure_is_raised_without_inserting(
    poll_env: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = stub_poll_dependencies(monkeypatch, ConnectionError("upstream down"))

    with pytest.raises(ConnectionError):
        poll_gtfs.fn()

    repository.insert.assert_not_called()
    assert "api_fetch" in poll_env.error.call_args.args[1]


def test_poll_skips_a_slot_that_started_a_full_interval_late(
    poll_env: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = stub_poll_dependencies(
        monkeypatch, ApiPayload(header={}, entity=None, entity_present=False)
    )
    monkeypatch.setattr(poll_module, "_lateness_seconds", lambda: 120.0)

    result = poll_gtfs.fn()

    assert result.is_completed() and result.name == "Skipped"
    repository.ensure_storage.assert_not_called()
    repository.insert.assert_not_called()


def test_dbt_run_logs_output_and_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = MagicMock()
    process = MagicMock()
    process.stdout = iter(["dbt output\n"])
    process.wait.return_value = 1
    popen = MagicMock()
    popen.return_value.__enter__.return_value = process
    monkeypatch.setattr("dags.dbt.get_run_logger", lambda: logger)
    monkeypatch.setattr("dags.dbt.subprocess.Popen", popen)
    monkeypatch.setattr(dbt_module, "concurrency", lambda *_a, **_k: nullcontext())

    with pytest.raises(subprocess.CalledProcessError) as error:
        dbt_run.fn()

    assert error.value.returncode == 1
    popen.assert_called_once_with(
        ["poetry", "run", "dbt", "run", "-t", "prod"],
        cwd=PROJECT_ROOT,
        env=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    logger.info.assert_called_once_with("%s", "dbt output")


def test_git_pull_fetches_then_pulls_main(monkeypatch: pytest.MonkeyPatch) -> None:
    run_command = MagicMock()
    monkeypatch.setattr(dbt_module, "_run_command", run_command)

    git_pull.fn()

    assert [c.args[0] for c in run_command.call_args_list] == [
        ["git", "fetch", "origin"],
        ["git", "pull", "origin", "main"],
    ]


def test_git_pull_stops_when_fetch_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    run_command = MagicMock(side_effect=subprocess.CalledProcessError(1, ["git"]))
    monkeypatch.setattr(dbt_module, "_run_command", run_command)

    with pytest.raises(subprocess.CalledProcessError):
        git_pull.fn()

    run_command.assert_called_once()


def test_publish_manifest_copies_into_prod_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    manifest = tmp_path / "target" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text('{"nodes": {}}')
    prod_dir = tmp_path / "artifacts" / "prod"
    monkeypatch.setattr(dbt_module, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(dbt_module, "PROD_ARTIFACTS_DIR", prod_dir)
    monkeypatch.setattr(dbt_module, "get_run_logger", MagicMock)

    destination = publish_manifest.fn()

    assert destination == prod_dir / "manifest.json"
    assert destination.read_text() == '{"nodes": {}}'


def test_dbt_flow_pulls_before_run_and_publishes_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = MagicMock()
    for name in ("git_pull", "dbt_run", "publish_manifest"):
        monkeypatch.setattr(dbt_module, name, getattr(calls, name))

    dbt_prod.fn()

    assert [c[0] for c in calls.mock_calls] == ["git_pull", "dbt_run", "publish_manifest"]


def stub_ci(monkeypatch: pytest.MonkeyPatch, tmp_path) -> MagicMock:
    run_command = MagicMock()
    (tmp_path / "manifest.json").write_text("{}")
    monkeypatch.setattr(dbt_module, "_run_command", run_command)
    monkeypatch.setattr(dbt_module, "PROD_ARTIFACTS_DIR", tmp_path)
    monkeypatch.setattr(dbt_module, "concurrency", lambda *_a, **_k: nullcontext())
    return run_command


def test_ci_runs_changed_models_in_a_worktree_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    run_command = stub_ci(monkeypatch, tmp_path)

    dbt_ci.fn("abc1234")

    commands = [c.args[0] for c in run_command.call_args_list]
    assert commands[0] == ["git", "fetch", "origin"]
    assert commands[1][:4] == ["git", "worktree", "add", "--detach"] and commands[1][-1] == "abc1234"
    assert commands[2][1:] == [
        "run", "--target", "stg", "--select", "state:modified+",
        "--defer", "--state", str(tmp_path),
    ]
    assert commands[3][:4] == ["git", "worktree", "remove", "--force"]
    assert run_command.call_args_list[2].args[2]["PROD_DUCKDB_PATH"] == str(dbt_module.PROD_DATABASE)


def test_ci_removes_worktree_when_run_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    run_command = stub_ci(monkeypatch, tmp_path)
    run_command.side_effect = lambda command, *_: (
        (_ for _ in ()).throw(subprocess.CalledProcessError(1, command))
        if command[1:2] == ["run"]
        else None
    )

    with pytest.raises(subprocess.CalledProcessError):
        dbt_ci.fn("abc1234")

    assert run_command.call_args_list[-1].args[0][:3] == ["git", "worktree", "remove"]


@pytest.mark.parametrize("ref", ["--upload-pack=x", "main", ""])
def test_ci_rejects_refs_that_are_not_commit_shas(
    monkeypatch: pytest.MonkeyPatch, tmp_path, ref: str
) -> None:
    run_command = stub_ci(monkeypatch, tmp_path)

    with pytest.raises(ValueError):
        dbt_ci.fn(ref)

    run_command.assert_not_called()


def test_deployments_keep_existing_names_and_schedules() -> None:
    deployments = {d.name: d for d in build_deployments(120, "America/Sao_Paulo")}

    assert set(deployments) == {
        "gtfs-poll",
        "hourly-parquet-export",
        "daily-retention",
        "dbt-prod",
        "dbt-ci",
    }
    assert not deployments["dbt-ci"].schedules

    poll = deployments["gtfs-poll"]
    assert poll.schedules[0].schedule.interval == timedelta(seconds=120)
    assert poll.concurrency_limit == 1
    assert poll.concurrency_options.collision_strategy == ConcurrencyLimitStrategy.CANCEL_NEW

    for name, cron, zone in [
        ("hourly-parquet-export", "5 * * * *", "America/Sao_Paulo"),
        ("daily-retention", "30 1 * * *", "America/Sao_Paulo"),
        ("dbt-prod", "0 4 * * *", "America/Sao_Paulo"),
    ]:
        schedule = deployments[name].schedules[0].schedule
        assert (schedule.cron, schedule.timezone) == (cron, zone)
