"""Unit tests for the DRAFT simulator (SQL API v2) mode — QA-291.

Pure/offline: HTTP is monkeypatched, no simulator or Snowflake required.
Covers config gating, response parsing (incl. partitions), record SQL shaping,
and INFORMATION_SCHEMA-based discovery.
"""

from __future__ import annotations

from typing import Any

import pytest
from tap_snowflake import simulator as sim


class _FakeResponse:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.ok = 200 <= status < 300
        self.text = str(payload)

    def json(self) -> Any:
        return self._payload


# ── config gating ────────────────────────────────────────────────────────────
def test_config_disabled_when_no_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(sim.ENV_BASE_URL, raising=False)
    assert sim.load_simulator_config({}) is None


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(sim.ENV_BASE_URL, "https://snowflake-sim-01.example.com/")
    monkeypatch.setenv(sim.ENV_CLIENT_ID, "cid")
    monkeypatch.setenv(sim.ENV_CLIENT_SECRET, "csecret")
    cfg = sim.load_simulator_config({"database": "CUSTOMER_DB"})
    assert cfg is not None
    assert (
        cfg.base_url == "https://snowflake-sim-01.example.com"
    )  # trailing slash stripped
    assert cfg.client_id == "cid"
    assert cfg.database == "CUSTOMER_DB"


def test_config_missing_creds_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(sim.ENV_BASE_URL, "https://sim")
    monkeypatch.delenv(sim.ENV_CLIENT_ID, raising=False)
    monkeypatch.delenv(sim.ENV_CLIENT_ID_FALLBACK, raising=False)
    with pytest.raises(ValueError, match="client id/secret"):
        sim.load_simulator_config({})


def test_config_prefers_explicit_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(sim.ENV_BASE_URL, "https://env-host")
    cfg = sim.load_simulator_config(
        {
            "simulator_base_url": "https://config-host",
            "simulator_client_id": "c",
            "simulator_client_secret": "s",
        },
    )
    assert cfg is not None
    assert cfg.base_url == "https://config-host"


# ── type map ───────────────────────────────────────────────────────────────
def test_type_map() -> None:
    assert sim.snowflake_type_to_jsonschema("TIMESTAMP_NTZ")["format"] == "date-time"
    assert "number" in sim.snowflake_type_to_jsonschema("NUMBER(38,0)")["type"]
    # unknown types fall back to nullable string
    assert sim.snowflake_type_to_jsonschema("GEOGRAPHY")["type"] == ["string", "null"]


# ── statement execution + partitions ─────────────────────────────────────────
def _client() -> sim.SnowflakeSimulatorClient:
    return sim.SnowflakeSimulatorClient(
        sim.SimulatorConfig(
            base_url="https://sim",
            client_id="c",
            client_secret="s",
            database="CUSTOMER_DB",
        ),
    )


def test_execute_parses_columns_and_pages_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok", "expires_in": 600})
        posts.append(kwargs.get("json", {}))
        return _FakeResponse(
            {
                "statementHandle": "h1",
                "resultSetMetaData": {
                    "rowType": [{"name": "CONTACT_ID"}, {"name": "EMAIL"}],
                    "partitionInfo": [{"rowCount": 1}, {"rowCount": 1}],
                },
                "data": [["c-1", "a@x.com"]],
            },
        )

    def fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        assert kwargs["params"]["partition"] == 1
        return _FakeResponse({"data": [["c-2", "b@x.com"]]})

    monkeypatch.setattr(sim.requests, "post", fake_post)
    monkeypatch.setattr(sim.requests, "get", fake_get)

    cols, rows = _client().execute(
        "SELECT CONTACT_ID, EMAIL FROM CUSTOMER_DB.PUBLIC.CONTACTS",
    )
    assert cols == ["CONTACT_ID", "EMAIL"]
    assert rows == [["c-1", "a@x.com"], ["c-2", "b@x.com"]]  # inline + partition 1
    assert posts[0]["database"] == "CUSTOMER_DB"


def test_execute_dicts_zips_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse(
            {
                "resultSetMetaData": {"rowType": [{"name": "A"}, {"name": "B"}]},
                "data": [[1, 2], [3, 4]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    rows = list(_client().execute_dicts("SELECT A, B FROM T"))
    assert rows == [{"A": 1, "B": 2}, {"A": 3, "B": 4}]


def test_execute_raises_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse({"message": "boom"}, status=422)

    monkeypatch.setattr(sim.requests, "post", fake_post)
    with pytest.raises(RuntimeError, match="Simulator statement failed"):
        _client().execute("SELECT 1")


# ── discovery ────────────────────────────────────────────────────────────────
def test_build_catalog_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse(
            {
                "resultSetMetaData": {
                    "rowType": [
                        {"name": "TABLE_SCHEMA"},
                        {"name": "TABLE_NAME"},
                        {"name": "COLUMN_NAME"},
                        {"name": "DATA_TYPE"},
                    ],
                },
                "data": [
                    ["PUBLIC", "CONTACTS", "CONTACT_ID", "TEXT"],
                    ["PUBLIC", "CONTACTS", "EMAIL", "TEXT"],
                    ["PUBLIC", "EVENTS", "EVENT_ID", "TEXT"],
                    ["PUBLIC", "EVENTS", "EVENT_TIMESTAMP", "TIMESTAMP_NTZ"],
                ],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    entries = sim.build_catalog_entries(_client(), database="CUSTOMER_DB")

    by_id = {e["tap_stream_id"]: e for e in entries}
    assert set(by_id) == {"CUSTOMER_DB-PUBLIC-CONTACTS", "CUSTOMER_DB-PUBLIC-EVENTS"}
    events = by_id["CUSTOMER_DB-PUBLIC-EVENTS"]
    assert events["schema"]["properties"]["EVENT_TIMESTAMP"]["format"] == "date-time"
    assert events["table_name"] == "EVENTS"


def test_build_catalog_entries_requires_database() -> None:
    with pytest.raises(ValueError, match="requires `database`"):
        sim.build_catalog_entries(_client(), database=None)
