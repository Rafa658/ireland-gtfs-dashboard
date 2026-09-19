from unittest.mock import Mock

import pytest

from poller.api_client import (
    API_ENDPOINT,
    ApiClient,
    ApiRedirectError,
    ResponseValidationError,
)


def test_fetches_with_api_key_and_returns_present_entity() -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"header": {"version": "2"}, "entity": [{"id": "1"}]}
    response.raise_for_status.return_value = None
    session = Mock()
    session.get.return_value = response

    payload = ApiClient("secret-key", timeout_seconds=30, session=session).fetch()

    session.get.assert_called_once_with(
        API_ENDPOINT,
        headers={"Accept": "application/json", "x-api-key": "secret-key"},
        timeout=30,
        allow_redirects=False,
    )
    assert payload.header == {"version": "2"}
    assert payload.entity == [{"id": "1"}]
    assert payload.entity_present is True


def test_distinguishes_an_omitted_entity_from_explicit_json_null() -> None:
    missing_response = Mock()
    missing_response.status_code = 200
    missing_response.json.return_value = {"header": {}}
    missing_response.raise_for_status.return_value = None
    null_response = Mock()
    null_response.status_code = 200
    null_response.json.return_value = {"header": {}, "entity": None}
    null_response.raise_for_status.return_value = None
    session = Mock()
    session.get.side_effect = [missing_response, null_response]
    client = ApiClient("secret-key", timeout_seconds=30, session=session)

    missing_payload = client.fetch()
    null_payload = client.fetch()

    assert missing_payload.entity is None
    assert missing_payload.entity_present is False
    assert null_payload.entity is None
    assert null_payload.entity_present is True


@pytest.mark.parametrize("body", [[], {"entity": []}, "invalid"])
def test_rejects_invalid_response_contract(body: object) -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = body
    response.raise_for_status.return_value = None
    session = Mock()
    session.get.return_value = response

    with pytest.raises(ResponseValidationError):
        ApiClient("secret-key", timeout_seconds=30, session=session).fetch()


def test_rejects_redirects_without_forwarding_the_api_key() -> None:
    response = Mock()
    response.status_code = 302
    response.raise_for_status.return_value = None
    session = Mock()
    session.get.return_value = response

    with pytest.raises(ApiRedirectError):
        ApiClient("secret-key", timeout_seconds=30, session=session).fetch()

    session.get.assert_called_once_with(
        API_ENDPOINT,
        headers={"Accept": "application/json", "x-api-key": "secret-key"},
        timeout=30,
        allow_redirects=False,
    )
