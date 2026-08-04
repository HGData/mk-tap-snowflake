# tap-snowflake simulator (SQL API v2) mode — DRAFT (QA-291)

**Status:** draft for review — not merged, not wired into CI yet.
**Author:** David Gustafson (with Claude) · **For review by:** Sheetal Karande, Jatin
**Related:** QA-291 (Snowflake simulator connector automation), QA-312 (mapping +
model, done), QA-257 (Salesforce simulator override — the pattern this mirrors),
`rgip-connector-simulators` PR #64 (the simulator).

---

## Why this exists

QA-291 wants the Snowflake MDI connector pull to run in CI against the
**simulator** (`rgip-connector-simulators/simulators/snowflake`) instead of a
live Snowflake account ("simulator only — cost-prohibitive").

The blocker we hit: the simulator implements the Snowflake **SQL API v2** REST
interface (`POST /api/v2/statements`, `POST /oauth/token-request`) — validated
solid in PR #64. But this tap (`meltanolabs-tap-snowflake`) connects over the
**driver** (`snowflake-connector-python` via SQLAlchemy), which speaks a
different binary wire protocol the simulator does not implement. Pointing the
driver at the sim can't work, and the tap exposes no host override.

So: the sim serves the right data over REST, but nothing consumes it via the
real tap. This draft is the missing link — a **dev-only REST execution mode** in
the tap, gated the same way as the Salesforce override (QA-257).

This closes the *pull* half of QA-291. The *downstream* half (mapping publish →
canonical tables → model → Hermes) is already automated and green in QA-312, and
is connector-agnostic — it chains off the canonical data this pull lands.

## What this draft does

When (and only when) a simulator base URL is configured, the tap bypasses
SQLAlchemy for **discovery** and **record reads** and issues the equivalent SQL
over the SQL API v2. Absent that config it is a **no-op** — the driver path runs
exactly as today (fail-closed).

Files:

| File | Change |
|------|--------|
| `tap_snowflake/simulator.py` | **New.** Config resolution (env + config), `SnowflakeSimulatorClient` (OAuth token → `POST /statements` → partition paging), and `build_catalog_entries` (discovery via `INFORMATION_SCHEMA.COLUMNS`). |
| `tap_snowflake/client.py` | `SnowflakeConnector.simulator_client` cached property; sim branch in `discover_catalog_entries`; `SnowflakeStream.get_records` sim branch + `_get_records_via_simulator`. |
| `tap_snowflake/tap.py` | `simulator_base_url` / `simulator_client_id` / `simulator_client_secret` config properties (all optional, dev-only). |
| `tests/test_simulator.py` | **New.** Offline unit tests (HTTP monkeypatched) — config gating, statement/partition parsing, record dict shaping, discovery. |
| `pyproject.toml` | `per-file-ignores` for `tests/**` (the existing suite uses the SDK's generated test class; hand-written unit tests need the exemption under `select = ["ALL"]`). |

### Activation / gating

- Env (what the CI override sets): `SIMULATOR_TAP_SNOWFLAKE_BASE_URL` (activation
  flag), `SIMULATOR_TAP_SNOWFLAKE_CLIENT_ID`, `SIMULATOR_TAP_SNOWFLAKE_CLIENT_SECRET`
  (falls back to `TAP_SNOWFLAKE_CLIENT_ID/SECRET`).
- Or tap config: `simulator_base_url` / `simulator_client_id` / `simulator_client_secret`.
- These reach the pull task via the `mdi_simulator_overrides` MWAA variable
  (QA-257 mechanism) — dev only; production never sets them.

### Validated so far

- `ruff check` + `ruff format` clean; 10 offline unit tests pass
  (`pytest tests/test_simulator.py`). No live sim or Snowflake required.
- **Not** yet run end-to-end against the real simulator or through Meltano.

---

## Open questions / needs confirmation

Ordered roughly by how much they could change the approach.

1. **Record vs batch mode.** This draft implements *record mode* (`get_records`
   → `SELECT`). The tap also has a batch path (`get_batches_from_internal_user_stage`:
   `COPY INTO @~` + `LIST` + driver `GET`), which is **not** expressible over SQL
   API v2. The `pull_snowflake` MDI task has no `batch_config`/`--batch`, so the
   SDK should default to record mode — **please confirm** no `batch_config` is set
   in the deployed meltano.yml for `tap-snowflake`. If batch mode is on, we force
   record mode for sim runs (small extraction-transport fidelity gap) or rethink.

2. **Discovery: runtime vs catalog.** Does the MDI run do runtime discovery, or
   pass a stored `--catalog`? If a catalog is supplied, `build_catalog_entries` is
   moot and we can drop it. If runtime, then:
   - **Stream-id format** — RESOLVED. It is `{schema}-{table}`, no database
     prefix. The driver path goes through singer-sdk's
     `SQLConnector.discover_catalog_entry`, which builds
     `f"{schema_name}-{table_name}"` (`singer_sdk/sql/connector.py`), and this
     repo's own fixtures agree (`tests/catalog.json`: `tpch_sf1-customer`).
     An earlier draft of this file used `{database}-{schema}-{table}`; that was
     wrong and would have silently broken `select` rules / `stream_maps` / dbt
     keys in simulator mode. The database is still emitted as `database-name`
     stream metadata.
   - **Type map** (`snowflake_type_to_jsonschema`) is coarse — confirm it matches
     the real discovery output for the columns in play.
   - **Key/replication metadata** — I emit empty `key_properties`; confirm what
     production sets.

3. **Exact sim schema + column selection.** `_get_records_via_simulator` does
   `SELECT <selected columns> FROM <db.schema.table>`. Need to confirm the sim's
   `CONTACTS`/`EVENTS` column names + case match what the tap will request (from
   PR #64 your rework used `CONTACT_ID, EMAIL, EVENT_ID, EVENT_NAME, EVENT_TIMESTAMP…`).

4. **Replication key filter.** Incremental uses `WHERE <rk> >= '<literal>'` with
   the value inlined (escaped) + `ORDER BY <rk>`. `rk` is `EVENT_TIMESTAMP` per
   your rework. Confirm: (a) the timestamp literal format the sim's `/statements`
   accepts, and (b) whether we should use SQL API v2 **bindings** instead of an
   inlined literal (cleaner; depends on the sim supporting bindings).

5. **Auth mode.** I use OAuth2 `client_credentials` against `/oauth/token-request`
   (the sim supports it). The sim also accepts key-pair JWT. OAuth is simpler for
   CI — confirm that's acceptable vs reusing the tap's keypair.

6. **SQL API v2 response shape.** The client reads `resultSetMetaData.rowType[].name`,
   `data`, `resultSetMetaData.partitionInfo[]`, `statementHandle`, and pages
   partitions via `GET /statements/{handle}?partition=N`. You validated the
   partition-fetch round trip in PR #64 — confirm these field names match the
   sim's actual response. Also: does the sim ever return **202/async** for these
   queries, or always 200 sync? (This draft assumes sync.)

7. **`requests` dependency.** The client uses `requests` (available transitively
   via singer-sdk / snowflake-connector-python). Before merge it should be a
   declared dependency in `pyproject.toml` + `poetry.lock`.

8. **Networking / TLS.** The dev pull ECS task must reach the snowflake-sim ALB
   over HTTPS with a valid cert (same egress the SF/HubSpot sims got —
   `salesforce-sim-01.hip.staging…`). Confirm a `snowflake-sim-01` host exists and
   is reachable from the pull task + the GHA runner.

---

## Still to do (outside this repo) — the rest of QA-291

- **`rgip-connector-simulators`** — add `snowflake` to the CI build matrix so the
  image builds/pushes to ECR. *(Done in draft branch `feat/QA-291-snowflake-sim-ci`:
  `ci.yml` + `build-branch-image.yml`.)*
- **`rgip-connector-tests`** — add `snowflake` to the MDI override
  (`lib/mwaa/simulator.ts` `buildSnowflakeOverrideEntry` writing the
  `SIMULATOR_TAP_SNOWFLAKE_*` keys; `set-`/`clear-simulator-override.ts`
  `SUPPORTED_CONNECTORS`); add `test-snowflake.yml` (clone of `test-salesforce.yml`,
  `SOURCE_SYSTEM: snowflake`). The connector-agnostic airflow/s3/glue/redshift
  scripts are reused via `SOURCE_SYSTEM`. Retire the mk-pull-style `scripts/snowflake/*`
  scaffolding (legacy path).
- **`mk-airflow`** — no code change; `_apply_simulator_overrides` is env-var-keyed.
- **Rollout** — cut a `mk-tap-snowflake` release and bump the pull ECS image
  (same shape as the Outreach sim PRs, DVO-3107).

## Validation strategy

A real Snowflake QA account exists (creds in Confluence, "Credentials for
Connectors") and produced the known-good tenant **426170** output QA-312 ran on.
So we can diff a sim-REST-mode pull's S3 / canonical output against a real-account
run — a strong correctness check before trusting the sim path.
