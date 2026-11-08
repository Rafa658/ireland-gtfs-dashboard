import shutil
import subprocess
from pathlib import Path

from prefect import flow, get_run_logger, task

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "target" / "manifest.json"
PROD_ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "prod"


def _run_command(command: list[str]) -> None:
    logger = get_run_logger()
    with subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
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
    _run_command(["poetry", "run", "dbt", "run", "-t", "prod"])


@task(name="publish-manifest")
def publish_manifest() -> Path:
    PROD_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    destination = Path(shutil.copy2(MANIFEST_PATH, PROD_ARTIFACTS_DIR))
    get_run_logger().info("Copied %s to %s", MANIFEST_PATH, destination)
    return destination


@flow(name="dbt-daily-run")
def dbt_daily_run() -> None:
    git_pull()
    dbt_run()
    publish_manifest()
