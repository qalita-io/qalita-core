# Polars streaming analytics — design

**Date**: 2026-08-02
**Branch**: `feat/polars-streaming` (core, packs, cli, platform)
**Goal**: a worker VM with **16 GB RAM** analyzes a **100+ GiB** source without OOM.

## Problem

A 100 GiB source on a 16 GB worker dies three times today, and the first death is not
even in Python.

**1. SQL ingestion.** All 14 SQL source classes in `qalita_core/data_source_opener.py`
call `pd.read_sql(sql, engine, chunksize=N)`. `chunksize` chunks *client-side only*:
psycopg2/pymysql/pymssql buffer the entire result set in the driver before pandas
yields row 1. `grep -c 'stream_results\|yield_per\|iter_batches'` over the 3021-line
file returns **0**. The Polars escape hatch `_load_database_polars` (L311-335) is dead
code three times over: `pl.read_database(sql, str(engine.url))` raises
`ValueError: string URI is invalid here` on polars ≥1.37, `str(engine.url)` returns a
password-masked URL (`postgresql://user:***@h:5432/db`), and neither `connectorx` nor
`adbc` is a dependency. The failure is logged at WARNING and falls back to pandas, so
100% of DB loads take the OOM path silently.

**2. Pack loaders.** 24 `pd.read_parquet` call sites across 14 packs. An identical
~30-line `_load_parquet_if_path()` preamble is copy-pasted into every `main.py`; it
re-materializes in pandas every chunk the loader just wrote to disk, and holds them all
alive in a list. Three of the four heavy packs OOM here before computing a single
metric. The streaming substrate that would fix this **already exists and is used by
nobody**: `Pack.scan_data()` returns a `pl.LazyFrame`, `Pack.get_row_count()` uses
`collect(engine="streaming")`, and all of `polars_io.py` is exported — grep across
`packs/` finds zero imports.

**3. Aggregators.** `DuplicateAggregator.combo_to_count` is a Python dict with one
entry per distinct key combination, filled by an `iter_rows()` loop. On a primary key
that is one tuple per row. `pii_scanner` keeps a Python `set` of row indices.
`fhir_compliance` runs `for idx in range(row_count)` with `.iloc[idx]` per field.

### Aggravating factors, independent of data size

**Live correctness bugs.** `paths[0]` silently analyzes only the first 100k-row chunk
(`data_drift`, `referential_integrity`); `zip(table_or_query, loaded)` silently
discards chunks 2..N (`pii_scanner`, `fhir_compliance`, `great_expectations`) or
relabels each chunk as its own dataset (`soda`). Chunking is on by default at 100k
rows, so **every source above 100k rows is affected in production today**.

**Failure is invisible.** `run_pack` (cli) computes the subprocess return code and
never uses it — success is decided by regex-matching stderr. A pack killed by the OOM
killer is reported to the platform as *succeeded*.

**Nothing can detect a regression.** No RSS assertion, no fixture above ~100k rows, no
benchmark or memory harness anywhere in core, packs, cli or platform.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | ydata-profiling (pandas-only, non-streamable) | **Replace** with a native Polars streaming profiler in core; drop the dependency from `profiling_pack` and `schema_scanner_pack` |
| 2 | pyod/KNN outlier detection (needs a dense matrix) | **Replace** with two-pass streaming IQR/z-score; `normality_score` changes definition |
| 3 | Great Expectations / Soda (pandas-only engines) | **DuckDB view over the parquet parts**; also repairs `great_expectations_pack`, broken today (imports the V2 API removed in GX 1.0) |
| 4 | Scope | Everything: pack correctness bugs, memory harness, backend SQL + ingestion, CLI data path |
| 5 | Exact vs approximate | **Approximate by default** (HyperLogLog for distinct, t-digest for quantiles) with an `exact` flag in `pack_conf`; approximate metrics carry `approximate=true`. Count, nulls, min/max, mean, std, sums stay exact |
| 6 | pandas | **Optional extra** `qalita-core[pandas]`; core is Polars-native; the global `pd.DataFrame.to_parquet` monkeypatch is deleted |
| 7 | Metric continuity | **Assumed one-time break** + major version bump per affected pack, documented in CHANGELOG/README; emit old *and* new keys where cheap (e.g. data_drift emits `psi` and `ks_binned`) |
| 8 | Row-level failure examples | **Included and on by default**, bounded; never for `pii_scanner` |

## Architecture

Five layers. Data goes to disk as Parquet once, and is never fully in RAM again.

### Layer 1 — Ingestion (`core/qalita_core/data_source_opener.py`)

- **SQL**: `engine.connect().execution_options(stream_results=True, yield_per=N)` +
  `pl.read_database(..., iter_batches=True)` → a `pyarrow.parquet.ParquetWriter` with a
  **pinned Arrow schema** derived once from cursor metadata. Per-chunk schema inference
  is what makes `pl.scan_parquet(paths)` raise `SchemaError` on exactly the large
  datasets this migration targets (`_part_1` types a column `Int64`, `_part_7` types it
  `String`).
- **Files**: `scan_csv` / `scan_ndjson` → `sink_parquet` **always** — the current 1 GB
  `STREAMING_THRESHOLD_BYTES` gate is removed. Excel streams through `openpyxl`
  read-only into Arrow RecordBatches. Whole-document JSON is sniffed and either treated
  as NDJSON or streamed with `ijson`.
- **Remote**: `storage_options` are propagated. Remote Parquet pass-through is the one
  zero-copy path in the layer and is currently broken on private buckets because the
  options are dropped.
- **Contract change**: `get_data()` returns `Dict[object_name, List[path]]` instead of a
  flat list. This is what structurally kills the `zip(tables, chunks)` mispairing bug —
  the mapping can no longer be guessed wrong because it is no longer guessed.
- **Disk guard**: a pre-flight `shutil.disk_usage` check against an estimate; a 100 GiB
  source stages to 20-60 GiB on a volume that today has no quota, no free-space check
  and no cleanup.

### Layer 2 — `qalita_core.analytics`

The new public API. Seven primitives plus three `Pack` methods, and nothing else.

```python
Pack.scan(trigger, table=None) -> pl.LazyFrame   # the only door to data
Pack.tables(trigger) -> list[str]
Pack.schema(trigger, table=None) -> dict[str, pl.DataType]   # parquet footers only

agg(lf, {name: expr}) -> dict[str, Any]          # ONE streaming pass, N expressions
sample(lf, n=100_000, method='reservoir') -> pl.DataFrame
approx_n_unique(lf, cols, exact=False) -> dict[str, int]
quantiles(lf, cols, qs, exact=False) -> dict[str, dict[float, float]]
top_k(lf, by, k=50, over=None, other=False) -> pl.DataFrame
failures(lf, predicate, limit=1000) -> tuple[int, pl.DataFrame]
sink(lf, path, max_rows_per_file=None) -> list[str]
```

Three invariants, **enforced rather than documented**:

1. Every collect goes through `engine="streaming"` and **raises** on failure. There is
   no non-streaming fallback anywhere. A streaming failure at 100 GiB is exactly the
   case where the in-memory engine cannot succeed — the six `except Exception:
   .collect()` fallbacks in `aggregation.py` today are OOMs disguised as resilience.
2. Every function that returns **rows** is bounded by construction. You cannot ask this
   API for an unbounded row set.
3. Packs never see a path, a pandas DataFrame, or a chunk — only LazyFrames and small
   results.

`agg` is the workhorse and exists to force a batching discipline. A naive lazy port
that loops `for column in columns:` re-scans 100 GiB **once per column**; that is the
trap all six validation packs would fall into. One `agg` call per dataset produces one
row of a few hundred scalars.

**Deliberately excluded** (YAGNI / anti-patterns): no `to_pandas` helper (a pack that
needs pandas calls `.to_pandas()` on the bounded result of `sample()` and owns the
consequence); no free-form `collect_streaming` (a free-form collect is how unbounded
results sneak back in); no `stream_to_parquet_chunks` (its current implementation
re-executes the whole plan per slice — N chunks means N full scans); no
`estimate_memory_usage` / `should_use_streaming` (there is no non-streaming mode left
to choose between); no `scan_csv`/`scan_excel`/`scan_json` (ingestion is the loader's
job; packs only ever see Parquet).

This replaces the current `polars_io` public surface — 13 exported symbols that zero
packs import, five of which are fake-lazy (`scan_excel`, `scan_json`,
`read_database_streaming` all read fully then call `.lazy()`).

### Layer 3 — Packs

Delete the eager preamble everywhere (~180 lines), one `agg` per dataset. Per decision:
native Polars profiler, streaming IQR/z-score outliers, DuckDB-backed GX and Soda,
bounded failure-row examples on by default except `pii_scanner`.

### Layer 4 — CLI / worker

Honour the subprocess return code (negative → signal → "killed, likely OOM"), pass
`TMPDIR` / `POLARS_MAX_THREADS` to pack runs, pre-flight disk check, clean up job
directories, and migrate `data_preview` + `action_executor` to Polars lazy with
`pl.SQLContext`, dropping `pandasql` (which copies the whole frame into an in-memory
SQLite).

### Layer 5 — Platform backend

Lift the 10 MB JSON ceiling with streaming parse and bulk insert — today the handler
does one `INSERT` + one `COMMIT` **per array element**, so 50k metrics means 50k
commits in one request. Push `monthly_scores` / `scores` / `hierarchy` / schema search
aggregations into SQL instead of Python loops over 10 000 ORM rows.

### Layer 0 — Harness

Large synthetic Parquet fixture generator, tests under an RSS ceiling, and a static
check that bans `pd.read_parquet` and bare `.collect()` in core and packs. Without it
nothing can prove this work does not regress.

## Testing

- **Semantic**: existing core suite (372 tests) stays green; per-pack unit tests for
  each ported computation, asserting the same values on small fixtures.
- **Memory**: parametrized fixtures well above the RAM ceiling of the test process,
  run under `ulimit -v`; the assertion is "completes", not "is fast".
- **Static**: the streaming lint runs in CI for core and packs and gates publish.

## Out of scope

- `omop_cdm_pack` throughput work (batching ~2535 checks, the two quadratic checks):
  the pack lives only on `feat/omop-cdm-pack` (PR #66, pending legal review), so it is
  absent from the `main`-based branch. To be done after #66 merges.
- `accepted_values_pack`: an empty directory, absent from `main`. Not created here.
- Push-down scanning of cloud sources in place (Athena `UNLOAD`, BigQuery
  `EXPORT DATA`). The stage-locally contract is kept; only the dropped
  `storage_options` on remote Parquet is repaired.
