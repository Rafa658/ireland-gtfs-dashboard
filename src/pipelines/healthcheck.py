import os
import sys
import urllib.error
import urllib.request


def run() -> None:
    """Exit 0 while the Prefect API is serving, including when it demands credentials."""
    port = os.getenv("PREFECT_SERVER_API_PORT", "4200")
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=5)
    except urllib.error.HTTPError as error:
        sys.exit(0 if error.code in {401, 403} else 1)
    except OSError:
        sys.exit(1)


if __name__ == "__main__":
    run()
