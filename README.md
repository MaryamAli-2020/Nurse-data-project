# Nurse Data Project

Research-grade data foundation and experiment framework for nurse rostering, centered on:

- `INRC-II` benchmark and test XML datasets
- `SchedulingBenchmarks` static benchmark cases
- a shared canonical SQLite schema
- reproducible parser, validation, feature generation, and experiment runners

This repository is intentionally source-only.
Raw datasets, generated reports, SQLite artifacts, and experiment outputs are excluded from version control.

## What This Repo Contains

- `src/inrc2_data`
  Core Python package for discovery, parsing, validation, schema generation, feature bundles, and experiments.
- `src/inrc2_data/experiments`
  Shared evaluation logic plus runnable static and multistage experiment runners.
- `scripts/build_foundation.py`
  Builds the canonical database, validation report, schema docs, and feature bundles.
- `scripts/run_static_experiment.py`
  Runs one static experiment on either a `SchedulingBenchmarks` case or an INRC-II weekly projection.
- `scripts/run_multistage_experiment.py`
  Runs one multistage INRC-II rollout week by week with history updates.
- `scripts/run_methodology_suite.py`
  Runs the implemented methodology variants for one target instance.

## Supported Problem Families

### 1. INRC-II

- Static weekly projections:
  - one scenario
  - one initial history
  - one reusable week
- Full multistage instances:
  - one scenario
  - one initial history
  - an ordered sequence of weeks

### 2. SchedulingBenchmarks

- Static-only benchmark family
- One full-horizon case per instance
- Parsed from `.ros` as canonical input
- `.txt` is used for traceability and parity checks

## Expected Local Folder Layout

Place the datasets in the project root before building the foundation:

```text
Nurse-data-project/
  INRC-II_datasets _xml/
  INRC-II_test datasets _xml/
  SchedulingBenchmarks/
  scripts/
  src/
```

The parser is robust to minor folder-name spacing inconsistencies around the INRC-II roots, but the datasets should stay at the project root.

## Python Requirements

- Python 3.10 or newer
- Standard-library-only runtime for the current data and experiment layer

No third-party package install is required for the current codebase.

## Quick Start

### 1. Build the data foundation

From the repository root:

```powershell
python scripts/build_foundation.py
```

This generates:

- `data/processed/inrc2_foundation.sqlite` or a rebuild fallback path if the main DB is locked
- canonical reports under `reports/`
- solver-friendly bundles under `data/processed/feature_bundles/`

### 2. Run one static experiment

SchedulingBenchmarks example:

```powershell
python scripts/run_static_experiment.py --algorithm greedy --case-code Instance1
```

INRC-II weekly projection example:

```powershell
python scripts/run_static_experiment.py --algorithm ga --instance-code "n005w4|H0|W0" --local-search-iterations 8
```

### 3. Run one multistage experiment

```powershell
python scripts/run_multistage_experiment.py --instance-code "n005w4|H0|WD_1-2-3-3" --algorithm greedy
```

### 4. Run the current methodology suite

Static suite on a SchedulingBenchmarks case:

```powershell
python scripts/run_methodology_suite.py --mode static --case-code Instance1
```

Multistage suite on an INRC-II instance:

```powershell
python scripts/run_methodology_suite.py --mode multistage --instance-code "n005w4|H0|WD_1-2-3-3"
```

## Implemented Methodology

The currently implemented workflow is:

1. Discover and parse all available benchmark families
2. Materialize a canonical SQLite data foundation
3. Validate referential integrity and structural consistency
4. Build static and multistage solver bundles
5. Run:
   - constructive baseline
   - GA baseline
   - memetic-style GA with local search
6. Persist solver runs, assignments, and constraint results back into SQLite

## Current Solver Scope

Implemented now:

- shared static evaluation engine
- `greedy` constructive baseline
- `ga` population-based solver
- memetic-style variant through GA plus local search
- multistage INRC-II weekly rollout with rolling history updates

Planned next:

- exact small-instance baseline with MIP or CP-SAT
- repair operator and exact subproblem repair
- stronger memetic neighborhoods
- hyper-heuristic operator control
- RL-guided operator selection

## Main Source Modules

- `src/inrc2_data/discovery.py`
  dataset-family-aware file discovery
- `src/inrc2_data/parse.py`
  canonical parsing into normalized tables
- `src/inrc2_data/scheduling_benchmarks.py`
  SchedulingBenchmarks-specific parsing and parity logic
- `src/inrc2_data/database.py`
  SQLite schema creation, inserts, and explainability views
- `src/inrc2_data/validation.py`
  structural and referential integrity checks
- `src/inrc2_data/features.py`
  bundle generation for static and multistage solving
- `src/inrc2_data/experiments/evaluation.py`
  shared hard/soft scoring logic
- `src/inrc2_data/experiments/solvers.py`
  constructive, GA, and memetic-style solvers
- `src/inrc2_data/experiments/runner.py`
  end-to-end static and multistage experiment runners

## Outputs Teammates Should Expect

After running the build and experiments locally, teammates will see generated artifacts such as:

- SQLite database
- validation and schema reports
- feature bundle JSON files
- experiment result JSON files

These are intentionally ignored by git and should be regenerated locally.

## Collaboration Notes

- Keep raw datasets out of git.
- Keep generated `reports/` and `data/processed/` out of git.
- Commit only source changes, script changes, and documentation updates.
- If the canonical SQLite file is open in another app, the build pipeline may write to a fallback rebuild filename instead of failing.

## Recommended Team Workflow

1. Pull the latest repo
2. Place the benchmark datasets in the project root
3. Run `python scripts/build_foundation.py`
4. Run the relevant experiment script
5. Inspect the generated outputs locally

## Repository Goal

This repo is meant to be the technical backbone for a publishable nurse rostering project:

- explainable data architecture
- shared validation and evaluation
- clean separation between raw data, canonical schema, and solver features
- direct support for both static and multistage research experiments
