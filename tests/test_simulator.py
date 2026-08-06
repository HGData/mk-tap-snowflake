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


@pytest.fixture(autouse=True)
def _neutral_deploy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep `ENV` out of the picture unless a test sets it deliberately.

    `load_simulator_config` refuses simulator mode when ENV is production, so an
    inherited ENV in the runner's environment would otherwise flip these tests.
    """
    monkeypatch.delenv(sim.ENV_DEPLOY_ENV, raising=False)


# ── config gating ────────────────────────────────────────────────────────────
def test_config_disabled_when_no_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(sim.ENV_BASE_URL, raising=False)
    assert sim.load_simulator_config({}) is None


@pytest.mark.parametrize("env_value", ["prod", "PROD", " production "])
def test_config_refused_in_production(
    monkeypatch: pytest.MonkeyPatch,
    env_value: str,
) -> None:
    """A stray override must not point a production pull at the simulator."""
    monkeypatch.setenv(sim.ENV_BASE_URL, "https://snowflake-sim-01.example.com")
    monkeypatch.setenv(sim.ENV_CLIENT_ID, "cid")
    monkeypatch.setenv(sim.ENV_CLIENT_SECRET, "csecret")
    monkeypatch.setenv(sim.ENV_DEPLOY_ENV, env_value)
    assert sim.load_simulator_config({}) is None


def test_config_refused_in_production_even_via_explicit_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal is not env-var-only — explicit tap config cannot bypass it."""
    monkeypatch.setenv(sim.ENV_DEPLOY_ENV, "prod")
    assert (
        sim.load_simulator_config(
            {
                "simulator_base_url": "https://config-host",
                "simulator_client_id": "c",
                "simulator_client_secret": "s",
            },
        )
        is None
    )


def test_config_enabled_in_non_production(monkeypatch: pytest.MonkeyPatch) -> None:
    """dev/local must still activate — the guard is narrow, not a kill switch."""
    monkeypatch.setenv(sim.ENV_BASE_URL, "https://snowflake-sim-01.example.com")
    monkeypatch.setenv(sim.ENV_CLIENT_ID, "cid")
    monkeypatch.setenv(sim.ENV_CLIENT_SECRET, "csecret")
    monkeypatch.setenv(sim.ENV_DEPLOY_ENV, "dev")
    assert sim.load_simulator_config({}) is not None


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


def test_execute_raises_on_multi_partition_without_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A multi-partition result with no handle must fail, not short-read.

    Partition 0 is inline; 1..N need the statementHandle. Returning only
    partition 0 would look like "the source has less data than expected"
    instead of an error.
    """

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse(
            {
                # no statementHandle, but three partitions advertised
                "resultSetMetaData": {
                    "rowType": [{"name": "A"}],
                    "partitionInfo": [
                        {"rowCount": 1},
                        {"rowCount": 1},
                        {"rowCount": 1},
                    ],
                },
                "data": [["only-partition-0"]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    with pytest.raises(RuntimeError, match="no statementHandle to page them"):
        _client().execute("SELECT A FROM CUSTOMER_DB.PUBLIC.EVENTS")


def test_execute_single_partition_without_handle_is_fine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single-partition result needs no handle — it is inline. Must not raise."""

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse(
            {
                "resultSetMetaData": {
                    "rowType": [{"name": "A"}],
                    "partitionInfo": [{"rowCount": 1}],
                },
                "data": [["v"]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    cols, rows = _client().execute("SELECT A FROM CUSTOMER_DB.PUBLIC.EVENTS")
    assert cols == ["A"]
    assert rows == [["v"]]


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
    # Keys are normalized, matching the property names discovery publishes.
    assert rows == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_execute_dicts_keys_match_discovered_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record keys and catalog properties must agree, or records fail validation."""

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse(
            {
                "resultSetMetaData": {
                    "rowType": [{"name": "EVENT_ID"}, {"name": "EVENT_TIMESTAMP"}],
                },
                "data": [["e1", "2026-01-01T00:00:00"]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    row = next(iter(_client().execute_dicts("SELECT * FROM CUSTOMER_DB.PUBLIC.EVENTS")))
    assert set(row) == {"event_id", "event_timestamp"}


# ── identifier normalization ─────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PUBLIC", "public"),
        ("EVENT_ID", "event_id"),
        ("CUSTOMER_DB", "customer_db"),
        ("already_lower", "already_lower"),
        # Mixed case means the identifier was created quoted, so Snowflake
        # preserves it and so must we — lower-casing it would break the lookup.
        ("MixedCase", "MixedCase"),
        # The rule is NOT "lower-case if upper-case": an all-upper name is only
        # lower-cased when the lower-cased form would not require quoting. A
        # dash is not a legal bare identifier character and `SELECT` is
        # reserved, so both stay as-is on the driver path — and must here.
        ("HAS-DASH", "HAS-DASH"),
        ("SELECT", "SELECT"),
        ("", ""),
    ],
)
def test_normalize_identifier(raw: str, expected: str) -> None:
    assert sim.normalize_identifier(raw) == expected


def test_execute_raises_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse({"message": "boom"}, status=422)

    monkeypatch.setattr(sim.requests, "post", fake_post)
    with pytest.raises(RuntimeError, match="Simulator statement failed"):
        _client().execute("SELECT 1")


# ── async lifecycle (202 RUNNING → poll → SUCCEEDED) ─────────────────────────
def test_execute_polls_until_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 202 POST must be polled until SUCCEEDED, then results read (not 0 rows)."""

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        # Accepted but still executing — body carries no results.
        return _FakeResponse({"statementHandle": "h9", "status": "RUNNING"}, status=202)

    calls = {"n": 0}

    def fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        assert url.endswith("/api/v2/statements/h9")
        calls["n"] += 1
        if calls["n"] < 3:  # RUNNING twice, then SUCCEEDED
            return _FakeResponse({"status": "RUNNING"}, status=202)
        return _FakeResponse(
            {
                "status": "SUCCEEDED",
                "statementHandle": "h9",
                "resultSetMetaData": {
                    "rowType": [{"name": "A"}],
                    "partitionInfo": [{"rowCount": 2}],
                },
                "data": [[1], [2]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    monkeypatch.setattr(sim.requests, "get", fake_get)
    monkeypatch.setattr(sim.time, "sleep", lambda _s: None)

    cols, rows = _client().execute("SELECT A FROM T")
    assert cols == ["A"]
    assert rows == [[1], [2]]
    assert calls["n"] == 3


def test_execute_poll_terminal_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A terminal status (e.g. 422 ABORTED/FAILED) during polling must raise."""

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/oauth/token-request"):
            return _FakeResponse({"access_token": "tok"})
        return _FakeResponse({"statementHandle": "h9", "status": "RUNNING"}, status=202)

    def fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse({"message": "cancelled"}, status=422)

    monkeypatch.setattr(sim.requests, "post", fake_post)
    monkeypatch.setattr(sim.requests, "get", fake_get)
    monkeypatch.setattr(sim.time, "sleep", lambda _s: None)

    with pytest.raises(RuntimeError, match="failed while polling"):
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
    # `{schema}-{table}` — no database prefix, case-normalized — matching what
    # singer-sdk's SQLConnector.discover_catalog_entry emits on the driver path
    # (snowflake-sqlalchemy lower-cases unquoted identifiers before the SDK sees
    # them). Getting this wrong is silent: mk-airflow builds select rules and
    # stream_maps as `{schema.lower()}-{table.lower()}`, so an upper-cased id
    # matches nothing, every stream is deselected, and the pull writes zero rows
    # while the DAG still reports success.
    assert set(by_id) == {"public-contacts", "public-events"}
    events = by_id["public-events"]
    assert events["schema"]["properties"]["event_timestamp"]["format"] == "date-time"
    assert events["table_name"] == "events"
    # The database is still carried in metadata even though it is not in the id.
    root = next(m for m in events["metadata"] if m["breadcrumb"] == [])
    assert root["metadata"]["database-name"] == "customer_db"
    assert root["metadata"]["schema-name"] == "public"
    # Column breadcrumbs must use the same normalized names as `properties`,
    # or the SDK's selection logic cannot resolve them.
    crumbs = {tuple(m["breadcrumb"]) for m in events["metadata"] if m["breadcrumb"]}
    assert crumbs == {("properties", "event_id"), ("properties", "event_timestamp")}


def test_build_catalog_entries_preserves_quoted_mixed_case_in_the_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mixed-case identifier was created quoted — normalizing it would break it.

    CATALOG-LEVEL ONLY. Preserving the name here is necessary but not sufficient:
    `_get_records_via_simulator` interpolates identifiers unquoted, so a read of
    this stream would still emit bare `eventId`. See the LIMITATION note on that
    method (QA-291 follow-up (e)). Unreachable via the simulator, which serves
    only legal upper-case identifiers — this test pins the discovery half of the
    contract, and deliberately does not claim the read half works.
    """

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
                "data": [["Analytics", "webEvents", "eventId", "TEXT"]],
            },
        )

    monkeypatch.setattr(sim.requests, "post", fake_post)
    entries = sim.build_catalog_entries(_client(), database="CUSTOMER_DB")
    assert [e["tap_stream_id"] for e in entries] == ["Analytics-webEvents"]
    assert set(entries[0]["schema"]["properties"]) == {"eventId"}


def test_build_catalog_entries_requires_database() -> None:
    with pytest.raises(ValueError, match="requires `database`"):
        sim.build_catalog_entries(_client(), database=None)
