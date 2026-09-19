import json

from orchestration.config import OrchestrationConfig
from orchestration.service import initialize_archive_state
from orchestration.state import PrefectCursorStore
from orchestration.storage import PostgresArchiveRepository


def initialize() -> dict[str, str | bool]:
    config = OrchestrationConfig.from_env()
    result = initialize_archive_state(
        PostgresArchiveRepository(config),
        PrefectCursorStore(),
        config.archive_initial_start,
    )
    return {
        "cursor": result.cursor.isoformat(),
        "retention_enabled": result.retention_enabled,
    }


def main() -> None:
    print(json.dumps(initialize(), separators=(",", ":")))


if __name__ == "__main__":
    main()
