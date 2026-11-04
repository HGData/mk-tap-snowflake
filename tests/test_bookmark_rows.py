"""Reading strictly after the bookmark once its rows are all read, run against SQLite.

Offline: the connector's table lookup and query execution are pointed at an
in-memory SQLite table, and the tap runs its normal sync, so state carries from
one run to the next as it does in production. Each load stamps all of its rows
with one shared value, the way a daily batch writes its run time into every row.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy
from sqlalchemy.pool import StaticPool
from tap_snowflake.client import BOOKMARK_ROWS, SnowflakeConnector, SnowflakeStream
from tap_snowflake.tap import TapSnowflake

if TYPE_CHECKING:
    from collections.abc import Iterator

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
                    "updated_at": {"type": ["string", "null"], "format": "date-time"},
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
                    "breadcrumb": ["properties", "updated_at"],
                    "metadata": {"selected": True},
                },
            ],
        },
    ],
}

CONFIG = {"user": "test", "account": "test", "password": "test"}

FIRST_STAMP = "2026-11-01T06:29:53.463000+00:00"
SECOND_STAMP = "2026-11-02T06:28:16.476000+00:00"
FIRST_LOAD = [("a1", FIRST_STAMP), ("a2", FIRST_STAMP), ("a3", FIRST_STAMP)]
SECOND_LOAD = [("b1", SECOND_STAMP), ("b2", SECOND_STAMP)]


class _Result:
    """What the tap reads from a CursorResult, fetched before the connection closes.

    `stop_after` drops the connection after that many records, the way a run can
    die part-way through a load.
    """

    def __init__(self, rows: list[sqlalchemy.Row], stop_after: int | None) -> None:
        self._rows = rows
        self._stop_after = stop_after

    def __iter__(self) -> Iterator[sqlalchemy.Row]:
        return iter(self._rows)

    def mappings(self) -> Iterator[Any]:
        for index, row in enumerate(self._rows):
            if index == self._stop_after:
                msg = "connection lost"
                raise ConnectionError(msg)
            yield row._mapping


class _Warehouse:
    """The in-memory events table, and a switch to cut a read off part-way."""

    def __init__(self, engine: sqlalchemy.Engine) -> None:
        self.engine = engine
        self.stop_after: int | None = None


@pytest.fixture
def warehouse(monkeypatch: pytest.MonkeyPatch) -> _Warehouse:
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
        sqlalchemy.Column("updated_at", sqlalchemy.String),
    )
    metadata.create_all(engine)
    state = _Warehouse(engine)

    def get_table(
        _self: SnowflakeConnector,
        full_table_name: Any,
        column_names: Any = None,
    ) -> sqlalchemy.Table:
        return events

    def execute(_self: SnowflakeConnector, query: Any) -> _Result:
        with engine.connect() as conn:
            return _Result(list(conn.execute(query)), state.stop_after)

    monkeypatch.setattr(SnowflakeConnector, "get_table", get_table)
    monkeypatch.setattr(SnowflakeConnector, "execute", execute)
    return state


def _load(warehouse: _Warehouse, rows: list[tuple[str, str | None]]) -> None:
    with warehouse.engine.begin() as conn:
        conn.execute(
            sqlalchemy.text("INSERT INTO events (id, updated_at) VALUES (:i, :u)"),
            [{"i": i, "u": u} for i, u in rows],
        )


def _read_messages(output: str) -> tuple[list[str], dict]:
    ids: list[str] = []
    last_state: dict = {}
    for line in output.splitlines():
        message = json.loads(line)
        if message["type"] == "RECORD":
            ids.append(message["record"]["id"])
        elif message["type"] == "STATE":
            last_state = message["value"]
    return ids, last_state


def _sync(state: dict | None) -> tuple[list[str], dict]:
    """Run one sync; return the ids emitted and the final state."""
    tap = TapSnowflake(config=CONFIG, catalog=CATALOG, state=state or {})
    out = io.StringIO()
    with redirect_stdout(out):
        tap.sync_all()
    return _read_messages(out.getvalue())


def _sync_until_cut_off(state: dict) -> tuple[list[str], dict]:
    """Run a sync the warehouse cuts off; return the ids emitted and the last state."""
    tap = TapSnowflake(config=CONFIG, catalog=CATALOG, state=state)
    out = io.StringIO()
    with redirect_stdout(out), pytest.raises(ConnectionError):
        tap.sync_all()
    return _read_messages(out.getvalue())


def _stream_state(state: dict) -> dict:
    return state["bookmarks"][STREAM]


def test_a_load_under_one_stamp_is_read_once(warehouse: _Warehouse) -> None:
    _load(warehouse, FIRST_LOAD)
    ids, state = _sync(None)
    assert sorted(ids) == ["a1", "a2", "a3"]
    assert _stream_state(state)[BOOKMARK_ROWS] == {
        "replication_key_value": FIRST_STAMP,
        "rows": 3,
    }

    ids, state = _sync(state)
    assert ids == []  # before the change, every run read the whole load again

    _load(warehouse, SECOND_LOAD)
    ids, state = _sync(state)
    assert sorted(ids) == ["b1", "b2"]
    assert _stream_state(state)["replication_key_value"] == SECOND_STAMP


def test_rows_added_at_the_bookmark_later_are_read(warehouse: _Warehouse) -> None:
    """A load committed in pieces under one stamp, or a date-only stamp."""
    _load(warehouse, FIRST_LOAD)
    _, state = _sync(None)

    _load(warehouse, [("a4", FIRST_STAMP), ("a5", FIRST_STAMP)])
    ids, state = _sync(state)
    assert {"a4", "a5"} <= set(ids)
    assert _stream_state(state)[BOOKMARK_ROWS] == {
        "replication_key_value": FIRST_STAMP,
        "rows": 5,
    }

    ids, _ = _sync(state)
    assert ids == []


def test_a_run_cut_off_inside_a_load_reads_that_load_again(
    warehouse: _Warehouse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SnowflakeStream, "STATE_MSG_FREQUENCY", 1)
    _load(warehouse, FIRST_LOAD)
    _, state = _sync(None)

    _load(warehouse, [*SECOND_LOAD, ("b3", SECOND_STAMP), ("b4", SECOND_STAMP)])
    warehouse.stop_after = 2
    ids, cut_off_state = _sync_until_cut_off(state)
    assert len(ids) == 2
    # The last state the run wrote already sits on the new load's stamp.
    assert _stream_state(cut_off_state)["replication_key_value"] == SECOND_STAMP

    warehouse.stop_after = None
    ids, _ = _sync(cut_off_state)
    assert sorted(ids) == ["b1", "b2", "b3", "b4"]


def test_a_bookmark_moved_back_by_hand_reads_from_the_new_value(
    warehouse: _Warehouse,
) -> None:
    _load(warehouse, [*FIRST_LOAD, *SECOND_LOAD])
    _, state = _sync(None)
    assert _stream_state(state)["replication_key_value"] == SECOND_STAMP

    _stream_state(state)["replication_key_value"] = FIRST_STAMP
    ids, _ = _sync(state)
    assert sorted(ids) == ["a1", "a2", "a3", "b1", "b2"]


def test_state_without_a_row_count_reads_the_bookmark_once_more(
    warehouse: _Warehouse,
) -> None:
    """The first run after this change starts from state the old code wrote."""
    _load(warehouse, FIRST_LOAD)
    old_state = {
        "bookmarks": {
            STREAM: {
                "replication_key": "updated_at",
                "replication_key_value": FIRST_STAMP,
            },
        },
    }
    ids, state = _sync(old_state)
    assert sorted(ids) == ["a1", "a2", "a3"]

    ids, _ = _sync(state)
    assert ids == []


def test_a_date_only_stamp_is_read_at_or_after(warehouse: _Warehouse) -> None:
    """A row changed in place under a date-only stamp leaves the count the same."""
    midnight = "2026-11-02T00:00:00.000000+00:00"
    _load(warehouse, [("c1", midnight), ("c2", midnight)])
    _, state = _sync(None)

    ids, _ = _sync(state)
    assert sorted(ids) == ["c1", "c2"]


def test_rows_without_a_stamp_are_not_counted(warehouse: _Warehouse) -> None:
    _load(warehouse, [("n1", None), *FIRST_LOAD])
    ids, state = _sync(None)
    assert sorted(ids) == ["a1", "a2", "a3", "n1"]
    assert _stream_state(state)[BOOKMARK_ROWS] == {
        "replication_key_value": FIRST_STAMP,
        "rows": 3,
    }


def test_a_row_record_without_a_count_reads_at_or_after(
    warehouse: _Warehouse,
) -> None:
    """A hand-edited record that lost its row count counts as no record."""
    _load(warehouse, FIRST_LOAD)
    _, state = _sync(None)
    del _stream_state(state)[BOOKMARK_ROWS]["rows"]

    ids, _ = _sync(state)
    assert sorted(ids) == ["a1", "a2", "a3"]


class _DatetimeRow:
    """A result row whose `updated_at` is a `datetime`, as Snowflake returns it."""

    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = mapping

    def __getitem__(self, index: int) -> Any:
        return list(self._mapping.values())[index]


@pytest.mark.parametrize(
    ("stamp", "kind"),
    [(FIRST_STAMP, "naive"), ("2026-10-30T05:14:47.572000-07:00", "aware")],
)
def test_datetime_values_record_the_sdk_bookmark(
    warehouse: _Warehouse,
    monkeypatch: pytest.MonkeyPatch,
    stamp: str,
    kind: str,
) -> None:
    """Snowflake returns datetimes; the record must match the SDK's bookmark."""
    read = SnowflakeConnector.execute

    def execute(self: SnowflakeConnector, query: Any) -> _Result:
        result = read(self, query)
        rows = []
        for row in result._rows:
            mapping = dict(row._mapping)
            if isinstance(mapping.get("updated_at"), str):
                value = datetime.fromisoformat(mapping["updated_at"])
                naive = value.replace(tzinfo=None)
                mapping["updated_at"] = naive if kind == "naive" else value
            rows.append(_DatetimeRow(mapping))
        return _Result(rows, result._stop_after)

    monkeypatch.setattr(SnowflakeConnector, "execute", execute)
    _load(warehouse, [("a1", stamp), ("a2", stamp)])
    ids, state = _sync(None)
    assert sorted(ids) == ["a1", "a2"]
    stream_state = _stream_state(state)
    recorded = stream_state[BOOKMARK_ROWS]["replication_key_value"]
    assert recorded == stream_state["replication_key_value"]

    ids, _ = _sync(state)
    assert ids == []
