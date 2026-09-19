from dataclasses import dataclass
from typing import Any

import requests

API_ENDPOINT = "https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json"


class ResponseValidationError(ValueError):
    """Raised when the upstream response does not match the required contract."""


class ApiRedirectError(RuntimeError):
    """Raised when the fixed upstream endpoint attempts to redirect."""


@dataclass(frozen=True, slots=True)
class ApiPayload:
    header: Any
    entity: Any
    entity_present: bool


class ApiClient:
    def __init__(
        self,
        api_key: str,
        timeout_seconds: int,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()

    def fetch(self) -> ApiPayload:
        response = self._session.get(
            API_ENDPOINT,
            headers={"Accept": "application/json", "x-api-key": self._api_key},
            timeout=self._timeout_seconds,
            allow_redirects=False,
        )
        if 300 <= response.status_code < 400:
            raise ApiRedirectError("API redirects are not allowed")
        response.raise_for_status()

        try:
            body = response.json()
        except requests.exceptions.JSONDecodeError as error:
            raise ResponseValidationError("API response must contain valid JSON") from error

        if not isinstance(body, dict):
            raise ResponseValidationError("API response must be a JSON object")
        if "header" not in body:
            raise ResponseValidationError("API response must contain a header key")

        return ApiPayload(
            header=body["header"],
            entity=body.get("entity"),
            entity_present="entity" in body,
        )
