# 08 · CI/CD for Spark Jobs

!!! note "Not executed against a live CI system in this environment"
    Config and code below are hand-traced against documented tooling
    behavior, not run in a live CI pipeline here.

Treating a PySpark job like any other piece of software — tested,
versioned, deployed through a pipeline rather than hand-copied to a
cluster — is what separates a script from production infrastructure.
This module covers testing PySpark logic without a real cluster,
packaging jobs for deployment, and a CI/CD pipeline shape that catches
regressions before they reach production.

## Unit testing PySpark logic with a local SparkSession

```python
# test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from transformations import clean_orders   # the function under test

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[2]")              # local mode -- no real cluster needed for logic tests
        .config("spark.sql.shuffle.partitions", "2")   # small, fast, deterministic for tests
        .getOrCreate()
    )

def test_clean_orders_drops_negative_amounts(spark):
    input_df = spark.createDataFrame(
        [(1, 101, 50.0), (2, 102, -10.0)], ["order_id", "customer_id", "amount"]
    )
    result = clean_orders(input_df)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == 1

def test_clean_orders_preserves_schema(spark):
    input_df = spark.createDataFrame(
        [(1, 101, 50.0)], ["order_id", "customer_id", "amount"]
    )
    result = clean_orders(input_df)
    assert set(result.columns) == {"order_id", "customer_id", "amount"}
```

```python
# transformations.py — the module under test, kept separate from any
# spark-submit entrypoint so it can be imported and unit tested directly
from pyspark.sql.functions import col

def clean_orders(df):
    return df.filter(col("amount") >= 0)
```

Structuring code this way — pure, testable transformation functions in
one module, a thin `main.py`/`job.py` entrypoint that wires them together
with `spark-submit` argument parsing — is the single highest-leverage
change for making a Spark codebase testable. A function that takes and
returns DataFrames is trivially unit-testable with a local
`SparkSession`; a monolithic script with all logic inline under
`if __name__ == "__main__":` is not.

## Testing DataFrame equality

```python
def assert_df_equal(actual_df, expected_df):
    actual_sorted = actual_df.orderBy(*actual_df.columns).collect()
    expected_sorted = expected_df.orderBy(*expected_df.columns).collect()
    assert actual_sorted == expected_sorted, f"\nActual:   {actual_sorted}\nExpected: {expected_sorted}"

def test_clean_orders_exact_output(spark):
    input_df = spark.createDataFrame([(1, 101, 50.0), (2, 102, -10.0)], ["order_id", "customer_id", "amount"])
    expected = spark.createDataFrame([(1, 101, 50.0)], ["order_id", "customer_id", "amount"])
    assert_df_equal(clean_orders(input_df), expected)
```

Sorting both sides before comparing `.collect()` output avoids test
flakiness from Spark's non-deterministic row ordering across partitions —
a common source of tests that pass locally and fail intermittently in CI.

## Testing UDFs in isolation, without Spark at all

```python
# Pure Python logic extracted from a UDF can be tested with plain pytest,
# no SparkSession needed — much faster for the actual business logic:
def parse_age_logic(age_str):
    try:
        return int(age_str)
    except (ValueError, TypeError):
        return None

def test_parse_age_logic():
    assert parse_age_logic("25") == 25
    assert parse_age_logic("not a number") is None
    assert parse_age_logic(None) is None
```

## Packaging: a wheel, not a loose script

```
project/
├── setup.py
├── src/
│   └── click_pipeline/
│       ├── __init__.py
│       ├── transformations.py
│       └── job.py
└── tests/
    └── test_transformations.py
```

```python
# setup.py
from setuptools import setup, find_packages

setup(
    name="click-pipeline",
    version="1.4.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[],   # PySpark itself is provided by the cluster, not bundled
)
```

```bash
python setup.py bdist_wheel
# dist/click_pipeline-1.4.0-py3-none-any.whl

spark-submit \
  --py-files dist/click_pipeline-1.4.0-py3-none-any.whl \
  src/click_pipeline/job.py --run-date 2024-01-15
```

Versioning the wheel (`1.4.0`) and deploying it explicitly, rather than
overwriting a loose `.py` file in place on a shared path, means you can
always answer "which exact code ran last Tuesday's job" and roll back to
a specific prior version if a release introduces a regression.

## A CI pipeline shape (GitHub Actions example)

```yaml
# .github/workflows/ci.yml
name: CI
on: [pull_request, push]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: "17"
          distribution: "temurin"
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install pyspark==3.5.0 pytest
      - run: pytest tests/ -v

  build-and-package:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: python setup.py bdist_wheel
      - uses: actions/upload-artifact@v4
        with:
          name: click-pipeline-wheel
          path: dist/*.whl

  deploy:
    needs: build-and-package
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: click-pipeline-wheel
      - run: |
          aws s3 cp *.whl s3://prod-spark-artifacts/click-pipeline/
          # trigger the orchestrator (Airflow, module 2) to pick up the
          # new artifact version on its next scheduled run, or bump a
          # config value the DAG reads for which version to submit.
```

The `test` job gating `build-and-package` gating `deploy` means a broken
unit test blocks the artifact from ever reaching production storage —
the same principle as any other software CI/CD, applied to Spark code
specifically because Spark logic is testable without a cluster once
structured as pure functions.

## Linting and static checks worth adding

```bash
# Catch obvious bugs before tests even run:
flake8 src/
mypy src/ --ignore-missing-imports   # PySpark's type stubs are partial but still useful
black --check src/
```

## Exercise

1. Take the `quality_gate` function from module 7 and write two pytest
   tests for it: one that should pass, one that should raise, using a
   local `SparkSession` fixture.
2. Extend the CI workflow above with a step that runs
   `mkdocs build --strict` (as this very site does) as a gate before
   `deploy` — explain why doc-build failures deserve to block a release
   the same way test failures do.
3. Explain why `install_requires` in `setup.py` deliberately excludes
   `pyspark` itself, and what would go wrong if a job's wheel bundled a
   different PySpark version than the cluster's own Spark installation.
