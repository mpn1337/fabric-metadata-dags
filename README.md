# fabric-metadata-dags

A metadata-driven DAG generator for orchestrating notebooks in **Microsoft Fabric** using `mssparkutils.notebook.runMultiple`.

Define pipelines declaratively in YAML files and automatically generate a Jupyter notebook (`.ipynb`) that executes the pipeline DAG — no manual notebook editing required.

---

## How it works

```
metadata/sales_pipeline.yaml
         │
         ▼
  generate-pipeline          ← CLI
         │
         ├── load_pipeline   ← parse YAML
         ├── validate schema ← catch unknown keys early
         ├── resolve config  ← 3-tier inheritance
         ├── validate DAG    ← duplicates / missing deps / cycles
         ├── build_dag       ← Fabric-format dict
         └── generate_notebook
                  │
                  ▼
      output/sales_pipeline.ipynb
```

The generated notebook contains three cells ready to run in Fabric:

```python
# Cell 1
from notebookutils import mssparkutils

# Cell 2
DAG = {
    "activities": [...],
    "timeoutInSeconds": 43200,
    "concurrency": 50,
}

# Cell 3
mssparkutils.notebook.runMultiple(
    DAG,
    {"displayDAGViaGraphviz": False},
)
```

---

## Installation

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/your-org/meta-driven-dags
cd meta-driven-dags
uv sync
```

---

## Usage

### Generate a single pipeline

```bash
uv run generate-pipeline metadata/sales_pipeline.yaml
```

### Generate all pipelines in `metadata/`

```bash
uv run generate-pipeline
```

### Custom options

```bash
uv run generate-pipeline metadata/sales_pipeline.yaml \
    --display-dag \
    --output-dir generated/ \
    --verbose
```

| Flag | Default | Description |
|---|---|---|
| `YAML_PATH` | *(all in `metadata/`)* | Path to a specific pipeline YAML file |
| `--metadata-dir DIR` | `metadata/` | Directory scanned when no file is given |
| `--display-dag` | `False` | Set `displayDAGViaGraphviz: True` in the notebook |
| `--output-dir DIR` | `output/` | Directory where `.ipynb` files are written |
| `--verbose` | `False` | Enable DEBUG-level logging |

---

## Pipeline YAML reference

```yaml
pipeline: sales_pipeline          # used as the output notebook filename

settings:
  concurrency: 50                 # max notebooks running in parallel
  timeoutInSeconds: 43200         # max runtime for the entire DAG (12 h)
  refreshInterval: 3              # status poll interval in seconds

defaults:                         # pipeline-level defaults (override framework defaults)
  retry: 2
  retryIntervalInSeconds: 30
  timeoutPerCellInSeconds: 90

activities:

  - name: ingest_sales            # unique activity name
    path: /notebooks/ingestion/ingest_sales   # notebook path in Fabric
    args:                         # notebook parameters
      load_type: full
      year: 2025

  - name: transform_sales
    path: /notebooks/transform/transform_sales
    dependencies:
      - ingest_sales              # runs after ingest_sales completes

  - name: aggregate_sales
    path: /notebooks/transform/aggregate_sales
    dependencies:
      - transform_sales
    retry: 5                      # activity-level override
    timeoutPerCellInSeconds: 200
```

### Supported activity keys

| Key | Required | Inheritable | Description |
|---|---|---|---|
| `name` | ✅ | — | Unique activity name |
| `path` | ✅ | — | Notebook path in Fabric |
| `args` | | — | Dict of notebook parameters |
| `dependencies` | | — | List of upstream activity names |
| `workspace` | | — | Target Fabric workspace (defaults to current) |
| `retry` | | ✅ | Max retry attempts |
| `retryIntervalInSeconds` | | ✅ | Delay between retries |
| `timeoutPerCellInSeconds` | | ✅ | Per-cell timeout |

### Supported settings keys

| Key | Description |
|---|---|
| `concurrency` | Max notebooks running in parallel |
| `timeoutInSeconds` | Max runtime for the entire DAG |
| `refreshInterval` | Status poll interval in seconds |

---

## Configuration inheritance

Inheritable fields (`retry`, `retryIntervalInSeconds`, `timeoutPerCellInSeconds`) are resolved in this priority order:

```
activity setting  →  pipeline defaults  →  framework defaults
```

Framework defaults:

| Field | Default |
|---|---|
| `retry` | `0` |
| `retryIntervalInSeconds` | `0` |
| `timeoutPerCellInSeconds` | `90` |

---

## Validation

The CLI validates every pipeline before generating a notebook:

1. **Schema** — unknown keys in any YAML section raise an error immediately, listing every problem at once.
2. **Duplicate names** — every activity must have a unique name.
3. **Missing dependencies** — all referenced dependency names must exist.
4. **Circular dependencies** — the dependency graph must be acyclic (DFS cycle detection with path reporting).

Example error output:

```
ERROR    Schema validation failed: Pipeline schema validation failed:
  • Unknown top-level pipeline key(s): "activitiez"
  • Unknown activity key(s) in "ingest_sales": "colour"

ERROR    DAG validation failed: Circular dependency detected: a → b → a
```

---

## Project structure

```
meta-driven-dags/
├── src/fabric_metadata_dags/
│   ├── schema.py       ← canonical valid-key frozensets
│   ├── loader.py       ← YAML → raw dict
│   ├── config.py       ← framework defaults + 3-tier resolution
│   ├── validator.py    ← schema + DAG structural validation
│   ├── builder.py      ← resolved metadata → Fabric DAG dict
│   ├── generator.py    ← DAG dict → .ipynb via nbformat
│   └── cli.py          ← argparse CLI entry point
├── metadata/           ← pipeline YAML definitions
│   ├── sales_pipeline.yaml
│   └── marketing_pipeline.yaml
├── output/             ← generated notebooks (git-ignored)
├── tests/
│   ├── test_config.py
│   ├── test_validator.py
│   ├── test_schema_validation.py
│   ├── test_builder.py
│   ├── test_loader.py
│   └── test_generator.py
└── pyproject.toml
```

---

## Running tests

```bash
uv run pytest -v
```
