"""Simulator (SQL API v2) execution mode for tap-snowflake.

DRAFT — QA-291. This is a **dev/test-only escape hatch**. It lets the tap read
from the RGIP Snowflake connector simulator (`rgip-connector-simulators`,
`simulators/snowflake`) instead of a live Snowflake account, so the MDI
connector pull can run in CI with no real warehouse.

Why this exists at all: the simulator implements the Snowflake **SQL API v2**
REST interface (`POST /api/v2/statements`, `POST /oauth/token-request`). The
production tap talks to Snowflake over the **driver** (`snowflake-connector-python`
via SQLAlchemy), which speaks a different, binary wire protocol the simulator
does not implement. So when — and only when — a simulator base URL is provided,
we bypass SQLAlchemy for discovery + record reads and issue the equivalent SQL
over the SQL API v2 instead.

Activation is fail-closed and mirrors the QA-257 Salesforce override: absent
`SIMULATOR_TAP_SNOWFLAKE_BASE_URL` (env or `simulator_base_url` config), this
module is a no-op and the tap behaves exactly as today (driver path). The
`mdi_simulator_overrides` MWAA variable injects these env vars onto the pull
task's ECS container in dev only; production never sets them.

See docs/simulator-rest-mode.md for the design and open questions.
"""

from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

# Env var names. These are the keys the tests-repo override entry writes into
# mdi_simulator_overrides[<tenant>].snowflake, and what mk-airflow copies onto
# the pull task's containerOverrides. `BASE_URL` presence is the activation flag.
ENV_BASE_URL = "SIMULATOR_TAP_SNOWFLAKE_BASE_URL"
ENV_CLIENT_ID = "SIMULATOR_TAP_SNOWFLAKE_CLIENT_ID"
ENV_CLIENT_SECRET = "SIMULATOR_TAP_SNOWFLAKE_CLIENT_SECRET"  # noqa: S105 — env var name, not a secret

# Fallbacks — reuse the ordinary tap creds if sim-specific ones are not set.
ENV_CLIENT_ID_FALLBACK = "TAP_SNOWFLAKE_CLIENT_ID"
ENV_CLIENT_SECRET_FALLBACK = "TAP_SNOWFLAKE_CLIENT_SECRET"  # noqa: S105 — env var name, not a secret

_HTTP_TIMEOUT_S = 60
_STATEMENT_TIMEOUT_S = 300
_POLL_INTERVAL_S = 1.0


@dataclass(frozen=True)
class SimulatorConfig:
    """Resolved simulator connection settings."""

    base_url: str
    client_id: str
    client_secret: str
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None


def _first(*values: str | None) -> str | None:
    for v in values:
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def load_simulator_config(config: dict[str, Any] | None) -> SimulatorConfig | None:
    """Build the simulator config from tap config + env, or None if disabled.

    Precedence: explicit tap config (`simulator_*`) first, then env. Returns None
    when no base URL is set anywhere — that is the fail-closed "run against real
    Snowflake as usual" path.
    """
    config = config or {}

    base_url = _first(
        config.get("simulator_base_url"),
        os.environ.get(ENV_BASE_URL),
    )
    if not base_url:
        return None

    client_id = _first(
        config.get("simulator_client_id"),
        os.environ.get(ENV_CLIENT_ID),
        os.environ.get(ENV_CLIENT_ID_FALLBACK),
    )
    client_secret = _first(
        config.get("simulator_client_secret"),
        os.environ.get(ENV_CLIENT_SECRET),
        os.environ.get(ENV_CLIENT_SECRET_FALLBACK),
    )
    if not client_id or not client_secret:
        msg = (
            f"{ENV_BASE_URL} is set (simulator mode) but client id/secret are "
            f"missing. Set {ENV_CLIENT_ID}/{ENV_CLIENT_SECRET} (or the "
            "TAP_SNOWFLAKE_CLIENT_ID/SECRET fallbacks)."
        )
        raise ValueError(msg)

    return SimulatorConfig(
        base_url=base_url.rstrip("/"),
        client_id=client_id,
        client_secret=client_secret,
        database=_first(config.get("database")),
        schema=_first(config.get("schema")),
        warehouse=_first(config.get("warehouse")),
        role=_first(config.get("role")),
    )


class SnowflakeSimulatorClient:
    """Minimal Snowflake SQL API v2 client aimed at the RGIP simulator.

    Only the read path the tap needs: OAuth client_credentials token, submit a
    statement, and page any partitioned result set. Not a general Snowflake SQL
    API client.
    """

    def __init__(self, config: SimulatorConfig) -> None:
        """Store config; the token is minted lazily on first use."""
        self._config = config
        self._token: str | None = None

    # ── Auth ────────────────────────────────────────────────────────────────
    def _access_token(self) -> str:
        """Mint (and cache) an OAuth2 access token via /oauth/token-request."""
        if self._token:
            return self._token

        creds = f"{self._config.client_id}:{self._config.client_secret}"
        basic = base64.b64encode(creds.encode()).decode()
        url = f"{self._config.base_url}/oauth/token-request"
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=_HTTP_TIMEOUT_S,
        )
        if not resp.ok:
            msg = (
                f"Simulator token endpoint {url} returned "
                f"{resp.status_code}: {resp.text}"
            )
            raise RuntimeError(msg)
        token = resp.json().get("access_token")
        if not token:
            msg = "Simulator token response did not include an access_token."
            raise RuntimeError(msg)
        self._token = token
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token()}",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── Statements ──────────────────────────────────────────────────────────
    def execute(self, statement: str) -> tuple[list[str], list[list[Any]]]:
        """Run a SQL statement and return (column_names, rows).

        Handles the SQL API v2 async lifecycle: a statement that does not finish
        within the synchronous window returns 202 RUNNING (with a
        statementStatusUrl) — poll GET /api/v2/statements/{handle} until it
        SUCCEEDED before reading. Then the partitioned result set: partition 0
        arrives inline in the (final) response; any further partitions are fetched
        with GET /api/v2/statements/{handle}?partition=N.
        """
        url = f"{self._config.base_url}/api/v2/statements"
        body: dict[str, Any] = {
            "statement": statement,
            "timeout": _STATEMENT_TIMEOUT_S,
        }
        # Session context so unqualified identifiers resolve like the driver would.
        if self._config.database:
            body["database"] = self._config.database
        if self._config.schema:
            body["schema"] = self._config.schema
        if self._config.warehouse:
            body["warehouse"] = self._config.warehouse
        if self._config.role:
            body["role"] = self._config.role

        resp = requests.post(
            url,
            headers=self._headers(),
            json=body,
            timeout=_HTTP_TIMEOUT_S,
        )
        if not resp.ok:
            msg = (
                f"Simulator statement failed ({resp.status_code}) "
                f"for {statement!r}: {resp.text}"
            )
            raise RuntimeError(msg)
        payload = resp.json()

        # Async lifecycle: 202 (or status RUNNING) means the statement is still
        # executing and the body carries no results yet — poll until SUCCEEDED.
        # (The driver path handles this natively; the REST path must poll, else a
        # slow/async statement silently yields zero rows.)
        still_running = (
            resp.status_code == HTTPStatus.ACCEPTED
            or payload.get("status") == "RUNNING"
        )
        if still_running:
            handle = payload.get("statementHandle")
            if not handle:
                msg = (
                    "Simulator returned RUNNING without a statementHandle to poll "
                    f"for {statement!r}: {resp.text}"
                )
                raise RuntimeError(msg)
            payload = self._poll_until_succeeded(handle)

        meta = payload.get("resultSetMetaData", {})
        columns = [c["name"] for c in meta.get("rowType", [])]
        rows: list[list[Any]] = list(payload.get("data", []) or [])

        handle = payload.get("statementHandle")
        partitions = meta.get("partitionInfo", []) or []
        for idx in range(1, len(partitions)):
            rows.extend(self._fetch_partition(handle, idx))

        return columns, rows

    def _poll_until_succeeded(self, handle: str) -> dict[str, Any]:
        """Poll GET /api/v2/statements/{handle} until the statement completes.

        Returns the SUCCEEDED response body (metadata + partition 0 data). Raises
        on a terminal non-success status or once _STATEMENT_TIMEOUT_S elapses.
        While RUNNING the SQL API returns 202; on completion, 200 with results.
        """
        url = f"{self._config.base_url}/api/v2/statements/{handle}"
        deadline = time.monotonic() + _STATEMENT_TIMEOUT_S
        while True:
            resp = requests.get(url, headers=self._headers(), timeout=_HTTP_TIMEOUT_S)
            if resp.status_code == HTTPStatus.OK:
                return resp.json()
            if resp.status_code == HTTPStatus.ACCEPTED:
                if time.monotonic() >= deadline:
                    msg = (
                        f"Simulator statement {handle} still RUNNING after "
                        f"{_STATEMENT_TIMEOUT_S}s — giving up."
                    )
                    raise RuntimeError(msg)
                time.sleep(_POLL_INTERVAL_S)
                continue
            # Any other status (e.g. 422 ABORTED/FAILED) is terminal.
            msg = (
                f"Simulator statement {handle} failed while polling "
                f"({resp.status_code}): {resp.text}"
            )
            raise RuntimeError(msg)

    def _fetch_partition(self, handle: str | None, partition: int) -> list[list[Any]]:
        if not handle:
            return []
        url = f"{self._config.base_url}/api/v2/statements/{handle}"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"partition": partition},
            timeout=_HTTP_TIMEOUT_S,
        )
        if not resp.ok:
            msg = (
                f"Simulator partition fetch failed ({resp.status_code}) "
                f"handle={handle} partition={partition}: {resp.text}"
            )
            raise RuntimeError(msg)
        return list(resp.json().get("data", []) or [])

    def execute_dicts(self, statement: str) -> Iterator[dict[str, Any]]:
        """Run a statement and yield one dict per row keyed by column name."""
        columns, rows = self.execute(statement)
        for row in rows:
            yield dict(zip(columns, row, strict=False))


# ─────────────────────────────────────────────────────────────────────────────
# Discovery over SQL API v2
#
# DRAFT / needs-confirmation (see docs): reproduces enough of the SDK's
# SQLAlchemy-reflection discovery to build a Singer catalog from the simulator.
# The catalog dict shape, stream-id format ({db}-{schema}-{table}), and the
# Snowflake→JSON type map below must match what production driver-based
# discovery emits, or downstream `select` rules / stream_maps / dbt keys drift.
# ─────────────────────────────────────────────────────────────────────────────

# Coarse Snowflake data_type → JSON schema map. Intentionally minimal; extend to
# match the real discovery output once the sim's INFORMATION_SCHEMA is confirmed.
_TYPE_MAP: dict[str, dict[str, Any]] = {
    "TEXT": {"type": ["string", "null"]},
    "VARCHAR": {"type": ["string", "null"]},
    "STRING": {"type": ["string", "null"]},
    "CHAR": {"type": ["string", "null"]},
    "BOOLEAN": {"type": ["boolean", "null"]},
    "NUMBER": {"type": ["number", "null"]},
    "FLOAT": {"type": ["number", "null"]},
    "DATE": {"type": ["string", "null"], "format": "date"},
    "TIMESTAMP_NTZ": {"type": ["string", "null"], "format": "date-time"},
    "TIMESTAMP_LTZ": {"type": ["string", "null"], "format": "date-time"},
    "TIMESTAMP_TZ": {"type": ["string", "null"], "format": "date-time"},
    "VARIANT": {"type": ["object", "string", "null"]},
    "OBJECT": {"type": ["object", "null"]},
    "ARRAY": {"type": ["array", "null"]},
}


def snowflake_type_to_jsonschema(data_type: str) -> dict[str, Any]:
    """Map a Snowflake INFORMATION_SCHEMA data_type to a JSON schema fragment."""
    key = (data_type or "").upper().split("(")[0].strip()
    return _TYPE_MAP.get(key, {"type": ["string", "null"]})


def build_catalog_entries(
    client: SnowflakeSimulatorClient,
    database: str | None,
    tables: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Discover catalog entries from the simulator via INFORMATION_SCHEMA.

    Mirrors SnowflakeConnector.discover_catalog_entries but sourced over REST.
    """
    if not database:
        msg = "Simulator discovery requires `database` (TAP_SNOWFLAKE_DATABASE)."
        raise ValueError(msg)

    wanted = {t.lower() for t in (tables or [])}
    sql = (
        "SELECT table_schema, table_name, column_name, data_type "
        f"FROM {database}.information_schema.columns "
        "WHERE table_schema <> 'INFORMATION_SCHEMA' "
        "ORDER BY table_schema, table_name, ordinal_position"
    )
    columns, rows = client.execute(sql)
    idx = {name.upper(): i for i, name in enumerate(columns)}

    grouped: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for row in rows:
        schema = row[idx["TABLE_SCHEMA"]]
        table = row[idx["TABLE_NAME"]]
        col = row[idx["COLUMN_NAME"]]
        dtype = row[idx["DATA_TYPE"]]
        if wanted and f"{schema}.{table}".lower() not in wanted:
            continue
        grouped.setdefault((schema, table), []).append((col, dtype))

    entries: list[dict[str, Any]] = []
    for (schema, table), cols in grouped.items():
        stream_id = f"{database}-{schema}-{table}"
        properties = {col: snowflake_type_to_jsonschema(dtype) for col, dtype in cols}
        column_metadata = [
            {
                "breadcrumb": ["properties", col],
                "metadata": {"inclusion": "available", "selected-by-default": True},
            }
            for col, _ in cols
        ]
        entries.append(
            {
                "tap_stream_id": stream_id,
                "table_name": table,
                "stream": stream_id,
                "schema": {
                    "type": "object",
                    "properties": properties,
                },
                "key_properties": [],
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "inclusion": "available",
                            "selected-by-default": False,
                            "schema-name": schema,
                            "database-name": database,
                            "table-key-properties": [],
                        },
                    },
                    *column_metadata,
                ],
            },
        )
    return entries
