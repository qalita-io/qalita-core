# QALITA Core

<p align="center">
  <img width="250px" height="auto" src="https://app.platform.qalita.io/logo.svg" style="max-width:250px;"/>
</p>

QALITA Core is a lightweight helper library used by QALITA packs to load data from multiple sources, materialize them to Parquet in deterministic chunks, and share common utilities (sanitization and aggregation helpers).

## Key features

- Unified data access via a simple `DataSource` abstraction and factory
- File, database, and object storage loaders with streaming to Parquet
- Deterministic, size-bounded Parquet chunking with stable filenames
- Safe Parquet writing for pandas DataFrames (automatic sanitization)
- Shared aggregators for completeness, outliers, duplicates, and timeliness
- Minimal pack runtime with JSON config loading and simple asset persistence

## Supported sources

- Files: CSV (`.csv`), Excel (`.xlsx`), JSON, Parquet (pass-through)
- Databases: PostgreSQL, MySQL, Oracle, MS SQL Server, SQLite
- Object storage: Amazon S3, Google Cloud Storage, Azure Blob (via `abfs`), HDFS

Notes:
- Folder, MongoDB classes exist as placeholders; MongoDB is not yet implemented.
- SQLite is supported through the generic `DatabaseSource` when selected via `type: "sqlite"`.

## Installation

Prerequisites: Python 3.10–3.12 and uv.

Install dependencies and set your environment:

```bash
pip install uv
uv sync
```

Open a uv shell when developing:

```bash
uv shell
```

## Quickstart

### Use within a Pack

`Pack` loads four JSON files by default (overridable) and provides `load_data()` for `source` or `target` triggers.

```python
from qalita_core.pack import Pack

pack = Pack(configs={
    "pack_conf": "./pack_conf.json",
    "source_conf": "./source_conf.json",
    "target_conf": "./target_conf.json",
    "agent_file": "~/.qalita/.worker",
})

# Ensure chunking/output are set (can be in pack_conf["job"] too)
pack.pack_config.setdefault("job", {})
pack.pack_config["job"]["parquet_output_dir"] = "./parquet"
pack.pack_config["job"]["chunk_rows"] = 100_000

# Load source
source_paths = pack.load_data("source")
# Load target (optional)
target_paths = pack.load_data("target")

# Persist custom metrics/recommendations/schemas to JSON files
pack.metrics.data.append({"key": "score", "value": "0.95", "scope": {"perimeter": "dataset", "value": "my_dataset"}})
pack.metrics.save()       # writes metrics.json
pack.recommendations.save()  # writes recommendations.json
pack.schemas.save()          # writes schemas.json
```

## Parquet chunking and filenames

- CSV/JSON/Excel are streamed with `chunksize` into multiple parquet files.
- Databases are read with chunked SQL via SQLAlchemy/`pandas.read_sql`.
- Filenames use a stable pattern: `<source>_<object>_part_<k>.parquet` where:
  - `<source>` is a slug of the source type (e.g. `file`, `sqlite`, `postgresql`).
  - `<object>` is a slug of the table name, query label, or file stem.
  - Example: `file_testdata_part_1.parquet`, `sqlite_items_part_3.parquet`, `sqlite_query_part_2.parquet`.

Configure output and size via `pack_config`:

- `parquet_output_dir` (default: `./parquet`)
- `chunk_rows` (default: `100000`)
- Optional `job.source.skiprows` applied to CSV/Excel

## Safe Parquet writing for pandas

On import, QALITA Core installs a small monkeypatch so `DataFrame.to_parquet`:

- Ensures column names are strings
- Decodes bytes to UTF‑8 strings when present
- Normalizes mixed-type object columns and categoricals
- Defaults to `engine="pyarrow"`

You can also call the sanitizer explicitly:

```python
from qalita_core import sanitize_dataframe_for_parquet
clean_df = sanitize_dataframe_for_parquet(df)
```

## Aggregation helpers (for packs)

Helpers centralize common result/metric aggregation logic:

```python
from qalita_core import (
    detect_chunked_from_items,
    normalize_and_dedupe_recommendations,
    CompletenessAggregator,
    OutlierAggregator,
    DuplicateAggregator,
    TimelinessAggregator,
)
```

- `CompletenessAggregator`: column/dataset completeness and schema extraction
- `OutlierAggregator`: per-column and dataset outlier/normality metrics
- `DuplicateAggregator`: duplicate counts and dataset-level score using key columns
- `TimelinessAggregator`: dates/years coverage and recency scoring

## Figures — `figures.json`

`FiguresAsset` (exposed as `pack.figures`) is the sibling of `pack.metrics`: metrics
carry the numbers, figures carry the aggregates that explain them. A pack declares
an **intention**, never a chart type — the platform picks the rendering.

```python
from qalita_core.pack import Pack

with Pack() as pack:
    pack.figures.declare_measure(
        "p_missing", unit="ratio", direction="lower_is_better", target=0.05
    )
    pack.figures.add(
        "missing_by_column",
        intent="breakdown",          # what contributes to the number
        of="p_cells_missing",        # the metric this figure explains
        frame=df,
        dims=["column"],
        measures=["p_missing"],
        scope={"perimeter": "dataset", "value": name},
    )
    pack.figures.save()
```

### API

```python
declare_measure(key, *, unit=None, direction=None, target=None,
                warn=None, label=None, description=None)
```
Declares the semantics of a measure — referenceable both by a figure's `measures`/`of`
and by a `metrics.json` key.

```python
add(key, *, intent, frame, dims, measures, scope, of=None, title=None,
    max_rows=5000)
```
Adds one figure. `intent` is one of `breakdown`, `composition`, `distribution`,
`trend`, `comparison`, `matrix`, `flow`. `frame` must be an **aggregate**: pandas
or polars `DataFrame`, or a list of dicts — at least one dimension, at least one
measure, exactly one row per dimension tuple. `dims` items are a bare name
(`"column"`, nominal) or a `(name, type)` tuple (`type` one of `nominal`,
`ordinal`, `temporal`). Rows beyond `max_rows` are dropped and the figure is
flagged `truncated: true` rather than raising.

`add()` raises on an empty `dims`, an empty `measures`, or a duplicate dimension
tuple in `frame` (a strong signal the frame was never aggregated). It does
**not** catch every non-aggregate: a frame keyed by a unique identifier —
`dims=["patient_id"], measures=["age"]` — has no duplicate tuple by construction
and clears every guard, shipping one row per patient (up to `max_rows`, truncated
above that). Nothing but the caller enforces "aggregate only, never raw rows" in
that case — worth remembering on regulated data.

`save()` additionally verifies that every measure named by a figure's `measures`
or `of` was declared via `declare_measure`; if not, it raises rather than writing
a figure that can never be linked to its metric. This means an undeclared measure
fails the whole pack run, not just that figure.

```python
add_raw(key, *, option, scope, title=None)
```
Escape hatch: a raw ECharts `option` dict, bypassing intent/self-service
rendering and excluded from reporting/drill-down. Use only when no `intent`
fits.

```python
top_n(frame, by, n, other=False, label="Autres", dim=None)
```
Keeps the `n` largest rows by `by`, dropping the tail. `other=True` folds the
tail into one row labeled `label` instead — valid **only** for an additive
measure (a count): summing ratios into that row produces a wrong number, which
is why `other` defaults to `False`. `dim` names the column that receives
`label` on the folded row; it is inferred only when exactly one column other
than `by` exists, and `top_n` raises rather than guess when there is more than
one candidate (or none).

Note: folding an integer dimension column produces different element types
across backends — `["1", "2", "Autres"]` under polars vs. `[1, 2, "Autres"]`
under pandas, because the label forces a common supertype. Both are valid JSON;
this is intentional and covered by tests, not a bug to work around.

### Chunking and `figures.add`

Data above `chunk_rows` (default 100 000) is processed in chunks (see above). A
pack that calls `figures.add` once per chunk without aggregating across chunks
first will emit one row per rule *execution*, not per dimension value — producing
duplicate dimension tuples that `add()` rejects. Either accumulate into a dict
keyed by the dimension across the whole loop and call `add()` once at the end, or
concatenate chunks into a single frame before computing aggregates. Both are
legitimate; pick one per pack.

## Development

- Tests: `uv run pytest`
- Formatting: `uv run black .`
- Linting: `uv run flake8` and `uv run pylint <module>`
- Editable install while debugging:

```bash
uv sync
uv pip install -e .
```

## Documentation

Additional material can be found in the online documentation: `https://doc.qalita.io/`.
