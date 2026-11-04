import subprocess
from pathlib import Path

from prefect import flow, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@flow(name="dbt-daily-run")
def dbt_daily_run() -> None:
    logger = get_run_logger()
    command = ["poetry", "run", "dbt", "run", "-t", "prod"]
    with subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        if process.stdout is None:
            raise RuntimeError("Unable to capture dbt output")
        for line in process.stdout:
            logger.info("%s", line.rstrip())
        returncode = process.wait()
    if returncode:
        raise subprocess.CalledProcessError(returncode, command)
