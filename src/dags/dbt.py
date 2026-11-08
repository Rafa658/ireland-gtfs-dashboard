import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.concurrency.sync import concurrency

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "target" / "manifest.json"
PROD_ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "prod"
PROD_DATABASE = PROJECT_ROOT / "gtfs.duckdb"
# Prod and CI runs never overlap: DuckDB cannot attach a file another process is writing.
DBT_LIMIT_NAME = "gtfs-dbt"
GIT_SHA = re.compile(r"[0-9a-f]{7,40}")


def _run_command(
    command: list[str], cwd: Path = PROJECT_ROOT, env: dict[str, str] | None = None
) -> None:
    logger = get_run_logger()
    with subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        if process.stdout is None:
            raise RuntimeError(f"Unable to capture output of {command[0]}")
        for line in process.stdout:
            logger.info("%s", line.rstrip())
        returncode = process.wait()
    if returncode:
        raise subprocess.CalledProcessError(returncode, command)


@task(name="git-pull")
def git_pull() -> None:
    _run_command(["git", "fetch", "origin"])
    _run_command(["git", "pull", "origin", "main"])


@task(name="dbt-run")
def dbt_run() -> None:
    with concurrency(DBT_LIMIT_NAME, strict=True):
        _run_command(["poetry", "run", "dbt", "run", "-t", "prod"])


@task(name="publish-manifest")
def publish_manifest() -> Path:
    PROD_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    destination = Path(shutil.copy2(MANIFEST_PATH, PROD_ARTIFACTS_DIR))
    get_run_logger().info("Copied %s to %s", MANIFEST_PATH, destination)
    return destination


@flow(name="dbt-prod")
def dbt_prod() -> None:
    git_pull()
    dbt_run()
    publish_manifest()


@flow(name="dbt-ci")
def dbt_ci(ref: str) -> None:
    """Run the models changed at `ref` into a throwaway stg database, deferring to prod."""
    if not GIT_SHA.fullmatch(ref):
        raise ValueError(f"ref must be a commit SHA, got {ref!r}")
    if not (PROD_ARTIFACTS_DIR / "manifest.json").exists():
        raise FileNotFoundError(f"No prod manifest in {PROD_ARTIFACTS_DIR}; run dbt-prod first")

    # The flow runs inside the project's virtualenv, so dbt lives next to python.
    bin_dir = Path(sys.executable).parent
    env = {**os.environ, "PROD_DUCKDB_PATH": str(PROD_DATABASE)}

    _run_command(["git", "fetch", "origin"])
    with tempfile.TemporaryDirectory(prefix="dbt-ci-") as tmp:
        workdir = Path(tmp) / "project"
        _run_command(["git", "worktree", "add", "--detach", str(workdir), ref])
        try:
            with concurrency(DBT_LIMIT_NAME, strict=True):
                _run_command(
                    [
                        str(bin_dir / "dbt"),
                        "run",
                        "--target",
                        "stg",
                        "--select",
                        "state:modified+",
                        "--defer",
                        "--state",
                        str(PROD_ARTIFACTS_DIR),
                    ],
                    workdir,
                    env,
                )
        finally:
            # Also deletes the worktree's gtfs_stg.duckdb, so every run starts from scratch.
            _run_command(["git", "worktree", "remove", "--force", str(workdir)])
