"""SQL client handling.

This includes SnowflakeStream and SnowflakeConnector.
"""

from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from enum import Enum, auto
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.engine.reflection
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from singer_sdk import metrics
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.sql import SQLConnector, SQLStream
from singer_sdk.sql.connector import SQLToJSONSchema
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from snowflake.sqlalchemy import URL
from sqlalchemy.sql import text

from tap_snowflake.simulator import (
    SnowflakeSimulatorClient,
    build_catalog_entries,
    load_simulator_config,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from singer_sdk.helpers import types
    from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
    from singer_sdk.sql.connector import FullyQualifiedName
    from sqlalchemy.engine import Connection, CursorResult
    from sqlalchemy.sql import Executable
    from sqlalchemy.sql.elements import TextClause


class SnowflakeAuthMethod(Enum):
    """Supported methods to authenticate to snowflake."""

    BROWSER = 1
    PASSWORD = 2
    KEY_PAIR = 3


class ProfileStats(Enum):
    """Profile Statistics Enum."""

    TABLE_SIZE_IN_MB = auto()
    TABLE_ROW_COUNT = auto()
    COLUMN_MIN_VALUE = auto()
    COLUMN_MAX_VALUE = auto()
    COLUMN_NULL_VALUES = auto()
    COLUMN_NONNULL_VALUES = auto()


@dataclass
class ColumnProfile:
    """Column Profile."""

    min_value: int | None
    max_value: int | None
    null_values: int | None
    nonnull_values: int | None


@dataclass
class TableProfile:
    """Table Profile."""

    column_profiles: dict[str, ColumnProfile]
    row_count: int | None
    size_in_mb: int | None


class SnowflakeToJSONSchema(SQLToJSONSchema):
    """Snowflake to JSON schema.

    We can't take advantage of this instance to map the type of VARIANT, ARRAY, and
    OBJECT columns because snowflake-sqlalchemy returns JSON strings:

    https://github.com/snowflakedb/snowflake-sqlalchemy/blob/31062877ae013e0fda3194142055b9aea58acdfc/README.md#variant-array-and-object-support
    """


class SnowflakeConnector(SQLConnector):
    """Connects to the Snowflake SQL source."""

    sql_to_jsonschema_converter = SnowflakeToJSONSchema

    @cached_property
    def simulator_client(self) -> SnowflakeSimulatorClient | None:
        """A SQL API v2 client when simulator mode is enabled, else None.

        DRAFT (QA-291). Fail-closed: returns None unless a simulator base URL is
        configured (env `SIMULATOR_TAP_SNOWFLAKE_BASE_URL` or `simulator_base_url`
        config), in which case the driver path is bypassed for discovery + reads.
        """
        config = load_simulator_config(dict(self.config))
        if config is None:
            return None
        self.logger.info(
            "tap-snowflake: SIMULATOR MODE enabled — reading from SQL API v2 sim at %s "
            "(driver path bypassed). This must never be set in production.",
            config.base_url,
        )
        return SnowflakeSimulatorClient(config)

    def get_private_key(self) -> bytes:
        """Get private key from the right location."""
        try:
            encoded_passphrase = self.config["private_key_passphrase"].encode()
        except KeyError:
            encoded_passphrase = None

        if "private_key_path" in self.config:
            with Path(self.config["private_key_path"]).open("rb") as key:
                key_content = key.read()
        else:
            key_content = self.config["private_key"].encode()

        p_key = serialization.load_pem_private_key(
            key_content,
            password=encoded_passphrase,
            backend=default_backend(),
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @cached_property
    def auth_method(self) -> SnowflakeAuthMethod:
        """Validate & return the authentication method based on config."""
        if self.config.get("use_browser_authentication"):
            return SnowflakeAuthMethod.BROWSER

        valid_auth_methods = {"private_key", "private_key_path", "password"}
        config_auth_methods = [x for x in self.config if x in valid_auth_methods]
        if len(config_auth_methods) != 1:
            msg = (
                "Neither password nor private key was provided for "
                "authentication. For password-less browser authentication via SSO, "
                "set use_browser_authentication config option to True."
            )
            raise ConfigValidationError(msg)
        if config_auth_methods[0] in ["private_key", "private_key_path"]:
            return SnowflakeAuthMethod.KEY_PAIR
        return SnowflakeAuthMethod.PASSWORD

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        params = {
            "account": config["account"],
            "user": config["user"],
        }

        if self.auth_method == SnowflakeAuthMethod.BROWSER:
            params["authenticator"] = "externalbrowser"
        elif self.auth_method == SnowflakeAuthMethod.PASSWORD:
            params["password"] = config["password"]

        for option in ["database", "schema", "warehouse", "role"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)

    @contextlib.contextmanager
    def _redirect_stdout_to_stderr(self) -> Any:
        """Temporarily redirect stdout to stderr.

        This is used to prevent snowflake-connector-python's browser authentication
        messages from polluting stdout, which must only contain Singer messages.
        """
        old_stdout = sys.stdout
        sys.stdout = sys.stderr
        try:
            yield
        finally:
            sys.stdout = old_stdout

    def create_engine(self) -> sqlalchemy.engine.Engine:
        """Create SQLAlchemy engine instance.

        Returns:
            A SQLAlchemy engine.
        """
        connect_args: dict[str, Any] = {
            "client_request_mfa_token": True,
            "client_store_temporary_credential": True,
        }
        if self.auth_method == SnowflakeAuthMethod.KEY_PAIR:
            connect_args["private_key"] = self.get_private_key()

        engine = sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            pool_timeout=10,
            connect_args=connect_args,
        )

        if self.auth_method == SnowflakeAuthMethod.BROWSER:
            # Patch the connect method to redirect stdout during browser auth
            original_connect = engine.connect

            def wrapped_connect(*args: Any, **kwargs: Any) -> Connection:
                with self._redirect_stdout_to_stderr():
                    return original_connect(*args, **kwargs)

            engine.connect = wrapped_connect  # type: ignore[method-assign]

        return engine

    # overridden to filter out the information_schema from catalog discovery
    def discover_catalog_entries(
        self,
        *,
        exclude_schemas: Sequence[str] = (),
        reflect_indices: bool = True,
    ) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Args:
            exclude_schemas: A list of schema names to exclude from discovery.
            reflect_indices: Whether to reflect indices to detect potential primary
                keys.

        Returns:
            The discovered catalog entries as a list.
        """
        if self.simulator_client is not None:
            # Simulator mode: reflect over the SQL API v2 instead of the driver.
            return build_catalog_entries(
                self.simulator_client,
                self.config.get("database"),
                self.config.get("tables"),
            )

        result: list[dict] = []
        tables = [t.lower() for t in self.config.get("tables", [])]
        engine = self.create_engine()
        inspected = sqlalchemy.inspect(engine)
        table_schema_names: list[str] = list(
            ()  # No tables specified
            if not tables
            else {x.split(".")[0] for x in tables}  # Get the schema from each table
        )
        if not table_schema_names:
            table_schema_names = [
                self._dialect.identifier_preparer.quote(schema_name)
                for schema_name in self.get_schema_names(engine, inspected)
                if schema_name.lower() != "information_schema"
            ]

        object_kinds = (
            (sqlalchemy.engine.reflection.ObjectKind.TABLE, False),
            (sqlalchemy.engine.reflection.ObjectKind.ANY_VIEW, True),
        )
        for schema_name in table_schema_names:
            # Iterate through each table and view of relevant schemas
            if schema_name in exclude_schemas:
                continue

            primary_keys = inspected.get_multi_pk_constraint(schema=schema_name)

            if reflect_indices:
                indices = inspected.get_multi_indexes(schema=schema_name)
            else:
                indices = {}

            for object_kind, is_view in object_kinds:
                columns = inspected.get_multi_columns(
                    schema=schema_name,
                    kind=object_kind,
                )

                result.extend(
                    self.discover_catalog_entry(
                        engine,
                        inspected,
                        schema_name,
                        table,
                        is_view,
                        reflected_columns=columns[schema, table],
                        reflected_pk=primary_keys.get((schema, table)),
                        reflected_indices=indices.get((schema, table), []),
                    ).to_dict()
                    for schema, table in columns
                    if not tables or (f"{schema_name}.{table}" in tables)
                )

        return result

    def get_table_profile(
        self,
        full_table_name: str,
        stats: set[ProfileStats],
        profile_columns: list[str] | None = None,
        where_clause: str | None = None,
    ) -> TableProfile:
        """Scan source system for a set of profile stats.

        This is a proposed implementation to eventually be included in the SDK.

        This generic implementation should be compatible with most/all SQL providers, as
        it only requires min(), max(), and count() support, which are all part of ANSI
        SQL.

        Developers may override this for performance improvements on their native SQL
        implementation.

        Any stats not requested but available 'for free' while collecting other stats
        may be included in the returned profile. For example, the base implementation
        cannot calculate COLUMN_NULL_VALUES without also calculating TABLE_ROW_COUNT and
        COLUMN_NONNULL_VALUES. The additional stats therefore would be returned along
        with the requested stats. The consumer should ignore or disregard any stats not
        needed.

        Note: Gathering stats can take a long time. The implementation should attempt
        to combine stats gathering into fewer tables scans where possible and only
        spend time pulling in requested stats.

        Args:
            full_table_name: Fully qualified table name.
            stats: Set of profile statistics to collect.
            profile_columns: List of column names to profile.
            where_clause: Optional WHERE clause to filter the table (e.g., "col >= value").

        Returns:
            A TableProfile object. Stats may be left null if not requested, not
            available, or not implemented. Callers should check each field for null
            values and handle null as 'not available'.
        """
        profile_columns = profile_columns or []
        expressions: list[str] = []
        if (
            ProfileStats.TABLE_ROW_COUNT in stats
            or ProfileStats.COLUMN_NULL_VALUES in stats
        ):
            expressions.append("count(1) as row_count")
        if ProfileStats.TABLE_SIZE_IN_MB in stats:
            self.logger.debug(
                "TABLE_SIZE_IN_MB stats not implemented for this provider.",
            )
        for col in profile_columns or []:
            if ProfileStats.COLUMN_MIN_VALUE in stats:
                expressions.append(f"min({col}) as min__{col}")
            if ProfileStats.COLUMN_MAX_VALUE in stats:
                expressions.append(f"max({col}) as max__{col}")
            if (
                ProfileStats.COLUMN_NONNULL_VALUES in stats
                or ProfileStats.COLUMN_NULL_VALUES in stats
            ):
                expressions.append(f"count({col}) as nonnull__{col}")
            if ProfileStats.COLUMN_NULL_VALUES in stats:
                expressions.append(f"count(1) - count({col}) as null__{col}")
        
        query = f"SELECT {', '.join(expressions)} FROM {full_table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result_dict = (
            self.execute(
                text(query),
            )
            .one()
            ._asdict()
        )
        return TableProfile(
            row_count=result_dict.get("row_count", None),
            size_in_mb=result_dict.get("size_in_mb", None),
            column_profiles={
                col: ColumnProfile(
                    min_value=result_dict.get(f"min__{col}", None),
                    max_value=result_dict.get(f"max__{col}", None),
                    null_values=result_dict.get(f"null__{col}", None),
                    nonnull_values=result_dict.get(f"nonnull__{col}", None),
                )
                for col in profile_columns
            },
        )

    def execute(self, query: Executable) -> CursorResult:
        """Execute the sqlalchemy query and return the cursor result."""
        with self._connect() as conn:
            return conn.execute(query)


class SnowflakeStream(SQLStream):
    """Stream class for Snowflake streams."""

    connector_class = SnowflakeConnector

    @property
    def is_sorted(self) -> bool:
        """Is sorted."""
        return bool(self.replication_key)

    def _sync_batches(
        self,
        batch_config: BatchConfig,
        context: types.Context | None = None,
    ) -> None:
        """Sync batches, emitting BATCH messages.

        This is a proposed replacement for the SDK internal SQLStream._sync_baches.
        Per: https://github.com/meltano/sdk/issues/976

        This version preserves existing bookmarks, collects the max replication value
        only from records that were actually synced (filtered by bookmark if it exists),
        and increments the stream state with this value after the sync operation completes.

        Args:
            batch_config: The batch configuration.
            context: Stream partition or context dictionary.
        """
        # Capture existing bookmark BEFORE _write_starting_replication_value
        # Read directly from state like the SDK does, to avoid dependency on
        # STARTING_MARKER which is only set after _write_starting_replication_value
        # This ensures we preserve the bookmark even if
        # _write_starting_replication_value tries to reset it
        existing_bookmark = None
        if self.replication_key:
            state = self.get_context_state(context)
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and self.replication_key == state.get(
                "replication_key",
            ):
                # Keep the bookmark as-is from state
                # (already JSON-compatible string/number)
                existing_bookmark = replication_key_value

        self._write_starting_replication_value(context)

        # Collect the max value for the replication column.
        # If bookmark exists, only collect max from records >= bookmark
        # (incremental sync). If no bookmark, collect max from entire table
        # (first sync / full table).
        max_replication_key_value = None
        if self.replication_key:
            if existing_bookmark:
                # Incremental sync: only get max from records that will be synced
                # Format the bookmark value for SQL WHERE clause.
                # State stores values as JSON-compatible strings, so we need to
                # format them properly for SQL (handle strings, timestamps, numbers)
                if self.is_timestamp_replication_key:
                    # For timestamps, wrap in quotes (already a string from state)
                    bookmark_str = f"'{existing_bookmark}'"
                elif isinstance(existing_bookmark, str):
                    # For string values, escape single quotes and wrap in quotes
                    escaped = existing_bookmark.replace(chr(39), chr(39) + chr(39))
                    bookmark_str = f"'{escaped}'"
                else:
                    # For numeric values, use as-is
                    bookmark_str = str(existing_bookmark)

                where_clause = f"{self.replication_key} >= {bookmark_str}"
                table_profile: TableProfile = (
                    self.connector.get_table_profile(  # type: ignore[attr-defined]
                        full_table_name=self.fully_qualified_name,
                        stats={ProfileStats.COLUMN_MAX_VALUE},
                        profile_columns=[self.replication_key],
                        where_clause=where_clause,
                    )
                )
            else:
                # First sync or full table: get max from entire table
                table_profile = self.connector.get_table_profile(  # type: ignore[attr-defined]
                    full_table_name=self.fully_qualified_name,
                    stats={ProfileStats.COLUMN_MAX_VALUE},
                    profile_columns=[self.replication_key],
                )

            max_replication_key_value = table_profile.column_profiles[
                self.replication_key
            ].max_value

        # Sync batches
        with metrics.batch_counter(self.name, context=context) as counter:
            for encoding, manifest in self.get_batches(batch_config, context):
                counter.increment()
                self._write_batch_message(encoding=encoding, manifest=manifest)
                self._write_state_message()

        # Increment and emit the final STATE message after sync has completed.
        # Only update state if we have a max value (records were synced).
        if max_replication_key_value:
            self._increment_stream_state(
                latest_record={
                    self.replication_key: max_replication_key_value,  # type: ignore[dict-item]
                },
                context=context,
            )
        self._write_state_message()

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: types.Context | None = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Get batches of Records from Snowflake.

        Currently this returns records batches unloaded via an internal user stage.
        In future this can be updated to include new methods for unloading via
        external stages.

        For more details on batch unloading data from Snowflake,
        see the Snowflake docs:
        https://docs.snowflake.com/en/user-guide-data-unload.html
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)
        yield from self.get_batches_from_internal_user_stage(batch_config, context)

    def _get_full_table_copy_statement(
        self,
        sync_id: str,
        prefix: str,
        objects: list[str],
        table_name: FullyQualifiedName,
    ) -> tuple[TextClause, dict]:
        """Get FULL_TABLE copy statement and key bindings."""
        statement = [f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "]
        if self.replication_key:
            statement.append(
                f"(select object_construct({', '.join(objects)}) from {table_name} "
                f"order by {self.replication_key}) ",
            )
        else:
            statement.append(
                f"(select object_construct({', '.join(objects)}) from {table_name}) ",
            )
        statement.append(
            "file_format = (type='JSON', compression='GZIP') overwrite = TRUE",
        )
        return (
            text("".join(statement)),
            {},
        )

    def _get_incremental_copy_statement(
        self,
        sync_id: str,
        prefix: str,
        objects: list[str],
        table_name: FullyQualifiedName,
        replication_key_value,
    ) -> tuple[TextClause, dict]:
        """Get INCREMENTAL copy statement and key bindings."""
        return (
            text(
                f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "
                f"(select object_construct({', '.join(objects)}) "
                f"from {table_name} "
                f"where {self.replication_key} >= :replication_key_value "
                f"order by {self.replication_key}) "
                "file_format = (type='JSON', compression='GZIP') overwrite = TRUE",
            ),
            {"replication_key_value": replication_key_value},
        )

    def _get_copy_statement(
        self,
        sync_id: str,
        prefix: str,
        context: types.Context | None = None,
    ) -> tuple[TextClause, dict]:
        """Construct copy statement.

        Takes into account stream property selection and incremental keys.
        """
        selected_schema = self.get_selected_schema()
        objects = [f"'{col}', {col}" for col in selected_schema["properties"]]

        if self.replication_method == REPLICATION_FULL_TABLE:
            return self._get_full_table_copy_statement(
                sync_id=sync_id,
                prefix=prefix,
                objects=objects,
                table_name=self.fully_qualified_name,
            )

        if self.replication_method == REPLICATION_INCREMENTAL:
            replication_key_value = (
                self.get_starting_timestamp(context=context)
                if self.is_timestamp_replication_key
                else self.get_starting_replication_key_value(context=context)
            )
            if replication_key_value:
                return self._get_incremental_copy_statement(
                    sync_id=sync_id,
                    prefix=prefix,
                    objects=objects,
                    table_name=self.fully_qualified_name,
                    replication_key_value=replication_key_value,
                )
            return self._get_full_table_copy_statement(
                sync_id=sync_id,
                prefix=prefix,
                objects=objects,
                table_name=self.fully_qualified_name,
            )
        raise NotImplementedError(
            "Only 'FULL_TABLE' and 'INCREMENTAL' replication strategies "
            "are supported by this tap.",
        )

    def get_batches_from_internal_user_stage(
        self,
        batch_config: BatchConfig,
        context: types.Context | None = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Unload Snowflake table to User Internal Stage, and download files to local storage.

        This method uses the Internal stage type, into the Snowflake-managed stage created
        for each user.

        More details on how this works can be found in the Snowflake docs:
        https://docs.snowflake.com/en/user-guide/data-unload-snowflake.html#unloading-data-to-your-user-stage
        """  # noqa: E501
        root = batch_config.storage.root
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""
        # prepare object_construct statement
        files = []
        try:
            # unload table into user internal stage
            copy_statement, kwargs = self._get_copy_statement(
                sync_id=sync_id,
                prefix=prefix,
                context=context,
            )
            self.connector.execute(copy_statement, **kwargs).all()  # type: ignore[attr-defined]
            # list available files
            results = self.connector.execute(  # type: ignore[attr-defined]
                text(f"list '@~/tap-snowflake/{sync_id}/'"),
            ).all()
            # download available files
            local_path = f"{root.replace('file://', '')}/{sync_id}"
            Path(local_path).mkdir(parents=True, exist_ok=True)
            for result in results:
                stage_path = result[0]
                file_name = Path(stage_path).name
                self.connector.execute(  # type: ignore[attr-defined]
                    text(f"get '@~/{stage_path}' '{root}/{sync_id}'"),
                )
                files.append(f"{root}/{sync_id}/{file_name}")
        finally:
            # remove staged files
            self.connector.execute(text(f"remove '@~/tap-snowflake/{sync_id}/'"))  # type: ignore[attr-defined]
        yield (batch_config.encoding, files)

    # Get records from stream
    # Overridden to use native objects under `if start_val:`
    def _get_records_via_simulator(
        self,
        context: types.Context | None,
    ) -> Iterable[dict[str, Any]]:
        """DRAFT (QA-291): read records over the SQL API v2 simulator.

        Builds the same SELECT the SQLAlchemy record path would (selected
        columns, incremental replication-key filter + ordering) and runs it
        over REST. Record mode only — the internal-stage batch path
        (`get_batches_from_internal_user_stage`) is not simulatable over SQL
        API v2 and must stay disabled (no `batch_config`) in simulator runs.
        """
        client = self.connector.simulator_client  # type: ignore[attr-defined]
        assert client is not None  # noqa: S101 — guarded by caller

        selected = list(self.get_selected_schema()["properties"].keys())
        col_list = ", ".join(selected) if selected else "*"
        sql = f"SELECT {col_list} FROM {self.fully_qualified_name}"

        clause = ""
        if self.replication_key:
            start_val = self.get_starting_replication_key_value(context)
            if start_val is not None:
                # DRAFT: inline literal (escaped). Switch to SQL API v2 bindings
                # once the sim's binding support is confirmed. See docs.
                literal = str(start_val).replace("'", "''")
                clause += f" WHERE {self.replication_key} >= '{literal}'"
            clause += f" ORDER BY {self.replication_key}"

        if self.ABORT_AT_RECORD_COUNT is not None:
            clause += f" LIMIT {self.ABORT_AT_RECORD_COUNT}"

        yield from client.execute_dicts(f"{sql}{clause}")

    def get_records(self, context: types.Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        if self.connector.simulator_client is not None:  # type: ignore[attr-defined]
            yield from self._get_records_via_simulator(context)
            return

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            query = query.limit(self.ABORT_AT_RECORD_COUNT)

        for record in self.connector.execute(query).mappings():  # type: ignore[attr-defined]
            yield dict(record)
