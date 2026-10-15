"""Per-partition bookmarks and the future-bookmark bound, run against SQLite.

Offline: the connector's table lookup and query execution are pointed at an
in-memory SQLite table, and the tap runs its normal sync, so state carries from
one run to the next as it does in production. The rows reproduce one shared
event table loaded once a day, where each kind of event stops at a different
time of day.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from typing import Any

import pytest
import sqlalchemy
from singer_sdk.exceptions import ConfigValidationError
from sqlalchemy.pool import StaticPool
from tap_snowflake.client import SnowflakeConnector
from tap_snowflake.tap import TapSnowflake

STREAM = "public-events"

CATALOG = {
    "streams": [
        {
            "tap_stream_id": STREAM,
            "stream": STREAM,
            "table_name": "events",
            "replication_method": "INCREMENTAL",
            "replication_key": "updated_at",
            "key_properties": ["id"],
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": ["string"]},
                    "event_name": {"type": ["string", "null"]},
                    "updated_at": {"type": ["string"], "format": "date-time"},
                },
            },
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "selected": True,
                        "replication-method": "INCREMENTAL",
                        "replication-key": "updated_at",
                        "schema-name": "public",
                        "table-key-properties": ["id"],
                    },
                },
                {"breadcrumb": ["properties", "id"], "metadata": {"selected": True}},
                {
                    "breadcrumb": ["properties", "event_name"],
                    "metadata": {"selected": True},
                },
                {
                    "breadcrumb": ["properties", "updated_at"],
                    "metadata": {"selected": True},
                },
            ],
        },
    ],
}

BASE_CONFIG = {"user": "test", "account": "test", "password": "test"}

# The first daily load: published sites run to 05:05 on the 10th, design sessions
# stop at midnight. The load's newest row is the published site.
FIRST_LOAD = [
    ("s1", "site_published", "2026-09-09T12:00:00.000000+00:00"),
    ("s2", "site_published", "2026-09-10T05:05:00.000000+00:00"),
    ("d1", "designer_session", "2026-09-09T18:00:00.000000+00:00"),
    ("d2", "designer_session", "2026-09-09T23:59:00.000000+00:00"),
]

# The next daily load: the night of the 10th's sessions arrive now, stamped before
# the first load's newest row.
SECOND_LOAD = [
    ("d3", "designer_session", "2026-09-10T03:00:00.000000+00:00"),
    ("d4", "designer_session", "2026-09-10T23:00:00.000000+00:00"),
    ("s3", "site_published", "2026-09-11T05:00:00.000000+00:00"),
]


class _Result:
    """What the tap reads from a CursorResult, fetched before the connection closes."""

    def __init__(self, rows: list[sqlalchemy.Row]) -> None:
        self._rows = rows

    def __iter__(self) -> Any:
        return iter(self._rows)

    def mappings(self) -> list[Any]:
        return [row._mapping for row in self._rows]


@pytest.fixture
def warehouse(monkeypatch: pytest.MonkeyPatch) -> sqlalchemy.Engine:
    """An in-memory events table the tap reads instead of Snowflake."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    metadata = sqlalchemy.MetaData()
    events = sqlalchemy.Table(
        "events",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.String, primary_key=True),
        sqlalchemy.Column("event_name", sqlalchemy.String),
        sqlalchemy.Column("updated_at", sqlalchemy.String),
    )
    metadata.create_all(engine)

    def get_table(
        _self: SnowflakeConnector,
        full_table_name: Any,
        column_names: Any = None,
    ) -> sqlalchemy.Table:
        if column_names is None:
            return events
        wanted = set(column_names)
        return sqlalchemy.Table(
            "events",
            sqlalchemy.MetaData(),
            *[sqlalchemy.Column(c.name, c.type) for c in events.c if c.name in wanted],
        )

    def execute(_self: SnowflakeConnector, query: Any) -> _Result:
        with engine.connect() as conn:
            return _Result(list(conn.execute(query)))

    monkeypatch.setattr(SnowflakeConnector, "get_table", get_table)
    monkeypatch.setattr(SnowflakeConnector, "execute", execute)
    return engine


def _load(engine: sqlalchemy.Engine, rows: list[tuple[str, str | None, str]]) -> None:
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO events (id, event_name, updated_at) VALUES (:i, :n, :u)",
            ),
            [{"i": i, "n": n, "u": u} for i, n, u in rows],
        )


def _sync(config: dict, state: dict | None) -> tuple[list[str], dict]:
    """Run one sync; return the ids emitted and the final state."""
    tap = TapSnowflake(config=config, catalog=CATALOG, state=state or {})
    out = io.StringIO()
    with redirect_stdout(out):
        tap.sync_all()
    ids: list[str] = []
    final_state: dict = {}
    for line in out.getvalue().splitlines():
        message = json.loads(line)
        if message["type"] == "RECORD":
            ids.append(message["record"]["id"])
        elif message["type"] == "STATE":
            final_state = message["value"]
    return ids, final_state


def _partition_bookmarks(state: dict) -> dict[Any, str]:
    stream_state = state["bookmarks"][STREAM]
    return {
        p["context"]["event_name"]: p["replication_key_value"]
        for p in stream_state["partitions"]
    }


def test_one_bookmark_skips_the_kind_that_lands_late(
    warehouse: sqlalchemy.Engine,
) -> None:
    """Without partition_columns the late night of sessions is never read."""
    _load(warehouse, FIRST_LOAD)
    _, state = _sync(BASE_CONFIG, None)
    assert state["bookmarks"][STREAM]["replication_key_value"].startswith(
        "2026-09-10T05:05",
    )

    _load(warehouse, SECOND_LOAD)
    ids, _ = _sync(BASE_CONFIG, state)

    assert "d3" not in ids  # stamped 03:00, behind the 05:05 bookmark
    assert {"d4", "s3"} <= set(ids)


def test_one_bookmark_per_event_name_reads_the_late_kind(
    warehouse: sqlalchemy.Engine,
) -> None:
    config = {**BASE_CONFIG, "partition_columns": {STREAM: "event_name"}}
    _load(warehouse, FIRST_LOAD)
    ids, state = _sync(config, None)
    assert sorted(ids) == ["d1", "d2", "s1", "s2"]
    bookmarks = _partition_bookmarks(state)
    assert bookmarks["designer_session"].startswith("2026-09-09T23:59")
    assert bookmarks["site_published"].startswith("2026-09-10T05:05")

    _load(warehouse, SECOND_LOAD)
    ids, state = _sync(config, state)

    # Each kind re-reads only its own boundary row, then its new rows.
    assert sorted(ids) == ["d2", "d3", "d4", "s2", "s3"]
    assert _partition_bookmarks(state)["designer_session"].startswith(
        "2026-09-10T23:00",
    )


def test_partitions_start_from_the_existing_table_bookmark(
    warehouse: sqlalchemy.Engine,
) -> None:
    """Turning partitioning on re-reads nothing behind the table's bookmark."""
    _load(warehouse, FIRST_LOAD)
    _, state = _sync(BASE_CONFIG, None)  # one bookmark, at 05:05 on the 10th

    _load(warehouse, SECOND_LOAD)
    config = {**BASE_CONFIG, "partition_columns": {STREAM: "event_name"}}
    ids, state = _sync(config, state)

    # d3 was already behind the table bookmark when partitioning was turned on;
    # recovering it is a bookmark rewind, not a side effect of the switch.
    assert sorted(ids) == ["d4", "s2", "s3"]
    stream_state = state["bookmarks"][STREAM]
    assert stream_state["replication_key_value"].startswith("2026-09-10T05:05")
    assert _partition_bookmarks(state)["designer_session"].startswith(
        "2026-09-10T23:00",
    )


def test_a_partition_rewind_rereads_only_that_kind(
    warehouse: sqlalchemy.Engine,
) -> None:
    config = {**BASE_CONFIG, "partition_columns": {STREAM: "event_name"}}
    _load(warehouse, FIRST_LOAD + SECOND_LOAD)
    _, state = _sync(config, None)

    for partition in state["bookmarks"][STREAM]["partitions"]:
        if partition["context"]["event_name"] == "designer_session":
            partition["replication_key_value"] = "2026-09-01T00:00:00.000000+00:00"
    ids, _ = _sync(config, state)

    assert sorted(ids) == ["d1", "d2", "d3", "d4", "s3"]


def test_rows_without_an_event_name_keep_their_own_bookmark(
    warehouse: sqlalchemy.Engine,
) -> None:
    config = {**BASE_CONFIG, "partition_columns": {STREAM: "event_name"}}
    _load(warehouse, [*FIRST_LOAD, ("n1", None, "2026-09-09T01:00:00.000000+00:00")])
    ids, state = _sync(config, None)

    assert "n1" in ids
    assert _partition_bookmarks(state)[None].startswith("2026-09-09T01:00")


def test_a_future_stamp_does_not_carry_the_bookmark(
    warehouse: sqlalchemy.Engine,
) -> None:
    future = ("f1", "publish_count_30d_changed", "2099-11-01T00:00:00.000000+00:00")
    _load(warehouse, [*FIRST_LOAD, future])

    ids, state = _sync({**BASE_CONFIG, "max_future_bookmark_seconds": 3600}, None)
    assert "f1" not in ids
    assert state["bookmarks"][STREAM]["replication_key_value"].startswith(
        "2026-09-10T05:05",
    )

    # Unbounded, the one future row takes the shared bookmark to 2099.
    ids, state = _sync(BASE_CONFIG, None)
    assert "f1" in ids
    assert state["bookmarks"][STREAM]["replication_key_value"].startswith("2099")


def test_a_partition_column_the_stream_does_not_select_is_rejected(
    warehouse: sqlalchemy.Engine,
) -> None:
    _load(warehouse, FIRST_LOAD)
    config = {**BASE_CONFIG, "partition_columns": {STREAM: "event_type"}}
    with pytest.raises(ConfigValidationError, match="event_type"):
        _sync(config, None)


def test_other_streams_keep_one_bookmark(warehouse: sqlalchemy.Engine) -> None:
    _load(warehouse, FIRST_LOAD)
    config = {**BASE_CONFIG, "partition_columns": {"public-contacts": "email"}}
    _, state = _sync(config, None)

    assert "partitions" not in state["bookmarks"][STREAM]
