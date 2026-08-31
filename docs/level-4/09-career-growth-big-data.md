# 09 · Career Growth Big Data

Having finished Levels 1–4, you can read and write real PySpark, reason
about execution plans, tune shuffle-heavy and skewed jobs, run
streaming pipelines, and build production-grade lakehouse tables with
schema evolution and quality gates. This module is less code and more a
map of where that skill set leads, and what to build next to keep
growing.

## Where this skill set is used

Big-data engineering with Spark shows up across a few recognizable role
shapes, and it's worth knowing which one you're aiming at because the
"next skill to learn" differs by target:

- **Data Engineer** — owns ingestion, transformation, and warehouse/lake
  pipelines. Deepens into orchestration (Airflow, Dagster), data
  modeling, and the specific lakehouse platform your org uses (Delta
  Lake / Databricks, Iceberg, Hudi).
- **Analytics Engineer** — sits between data engineering and BI, focused
  on transforming raw data into clean, well-modeled tables for analysts.
  Often pairs Spark/SQL skills with dbt, and cares more about data
  modeling correctness than cluster tuning.
- **ML/Platform Engineer (data side)** — builds feature pipelines and
  training-data generation at scale, where Spark is the bridge between
  raw data and an ML training job. Deepens into feature stores, MLflow,
  and the handoff between Spark and training frameworks.
- **Big Data / Platform Infrastructure Engineer** — owns the cluster
  itself: capacity planning, multi-tenancy (Level 4 module 6), cost
  management (Level 4 module 3), and CI/CD for the platform, not just
  individual jobs.

## Skills that compound well from here

```python
# A rough "next skills" map, grouped by what you already have:

next_skills = {
    "You know PySpark tuning (Level 3)": [
        "Learn Spark's internals more deeply: Catalyst optimizer source,
         Tungsten execution engine — read the Spark source for the
         physical operators you've been reading in .explain() output.",
        "Scala — many Spark internals, and some UDF performance-critical
         paths, are easier to reason about once you can read Scala Spark
         code, even if you keep writing PySpark day to day.",
    ],
    "You know lakehouse patterns (Level 4)": [
        "Apache Iceberg and Apache Hudi — Delta Lake's two major
         alternatives; understanding all three's tradeoffs (especially
         around concurrent write conflict handling) is a strong signal
         in senior data-engineering interviews.",
        "Data modeling: dimensional modeling (star/snowflake schemas),
         slowly changing dimensions — these predate Spark by decades but
         remain exactly what a lakehouse table's shape should follow.",
    ],
    "You know orchestration and CI/CD (Level 4)": [
        "Infrastructure as code (Terraform) for the cluster/orchestrator
         itself, not just the jobs running on it.",
        "Observability beyond the Spark UI: structured logging, metrics
         export (Prometheus/Grafana) from Spark listeners, and
         data-lineage tooling (OpenLineage) for tracing a bad value back
         to its source across a whole pipeline graph.",
    ],
}
for area, items in next_skills.items():
    print(f"\n{area}:")
    for item in items:
        print(f"  - {item}")
```

## Building a portfolio project that actually demonstrates this level

A toy CSV-to-Parquet script (Level 1's capstone) demonstrates you can
write PySpark. A portfolio project that demonstrates *this* level should
show the judgment from Levels 3–4, not just more DataFrame calls:

- A pipeline with a **deliberately introduced and then diagnosed**
  performance problem — commit the slow version, then commit the fix
  with a written explanation referencing `.explain()` output and Spark UI
  screenshots, the way Level 3's capstone modeled.
- A **lakehouse table with schema evolution handled explicitly** — not
  just `mergeSchema=true` everywhere, but a documented decision for at
  least one rename/type-change scenario (Level 4 module 5).
- **Data quality gates with a quarantine table**, not just
  `.filter()`-and-forget (Level 4 module 7).
- A **CI pipeline** that actually runs `pytest` against your
  transformation functions on every push (Level 4 module 8) — a GitHub
  repo with a green CI badge is a stronger signal than the same code
  without one.

## Interview preparation: what actually gets asked

At the level this path targets, interviews tend to probe judgment more
than syntax recall:

```python
interview_question_shapes = [
    "Walk me through what happens when you call .join() and .collect() "
    "on two DataFrames — trace it from logical plan through physical "
    "execution.",                                                  # Level 3 module 1

    "A job is slow. What do you check first, second, third?",       # Level 3 modules 2,3,8

    "How would you design a nightly pipeline that's safe to retry "
    "after a partial failure?",                                     # Level 4 modules 2, 1

    "When would you choose Delta/Iceberg/Hudi over plain Parquet, "
    "and what does each actually give you?",                        # Level 4 module 1

    "How do you handle a column that got renamed upstream without "
    "breaking historical queries?",                                 # Level 4 module 5
]
for q in interview_question_shapes:
    print("-", q)
```

Being able to answer these by tracing through a concrete example (as this
whole path has tried to model, module by module) reads as far more senior
than reciting definitions.

## Certifications worth knowing about (optional, not required)

The Databricks Certified Associate/Professional Data Engineer
certifications are the most recognized in the Spark ecosystem
specifically, and their exam guides double as a useful checklist of
"things a working Spark engineer is expected to know" even if you never
sit the exam — worth skimming the public exam guide as a gap-check
against this path's coverage.

## Staying current

Spark itself keeps evolving — AQE (Level 3 module 6) was new in 3.x,
`pandas_udf` performance keeps improving with Arrow updates, and
lakehouse format interoperability (Delta/Iceberg/Hudi converging on
shared catalogs) is an actively moving area as of this writing. Following
the Apache Spark release notes and the engineering blogs of major
Spark-heavy companies (Databricks, Netflix, Uber, LinkedIn) is a
low-effort way to keep the mental model current after finishing this
path.

## Exercise

1. Pick one of the four role shapes above that most matches your goals,
   and write a 3-item, 90-day learning plan using the "next skills" map
   as a starting point.
2. Take one real (or plausible) slow job from your own experience and
   write it up the way Level 3's capstone modeled: symptom, diagnosis via
   `.explain()`/UI, fix, verification — as a portfolio artifact.
3. Pick one of the five interview question shapes above and write a full
   answer for it, tracing through a concrete example the way this path's
   modules have done throughout.
