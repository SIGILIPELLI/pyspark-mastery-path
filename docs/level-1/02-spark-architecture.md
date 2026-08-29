# 02 · Spark Architecture (Driver, Executors, Cluster Manager)

## The three moving parts

Every Spark application, however small, involves the same three actors:

1. **Driver.** The process running your `main()` code (your PySpark script).
   The driver builds the execution plan (what transformations to run, in what
   order), asks the cluster manager for resources, and coordinates the whole
   job. It also collects final results when you call an action like
   `.collect()` or `.count()`. There is exactly **one** driver per
   application.
2. **Executors.** Worker processes that actually run tasks and hold data
   partitions in memory or on local disk. Each executor runs many tasks
   (one per CPU core it's been allocated, typically) over the lifetime of the
   application. Executors report status back to the driver and exchange data
   with each other directly when a shuffle happens (more on shuffles in
   Level 2).
3. **Cluster manager.** The system that allocates physical resources (CPU,
   memory) across the cluster for the driver and executors to run on.
   Options include Spark's own **Standalone** manager, **YARN**, **Kubernetes**,
   or — for local development, which is what this entire level uses — Spark's
   **local mode**, where the driver and all "executors" are just threads
   inside a single JVM process on your own machine.

```text
                    ┌─────────────────────┐
                    │   Cluster Manager    │
                    │ (Standalone/YARN/K8s)│
                    └──────────┬───────────┘
                               │ allocates resources
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
           ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Executor │  │ Executor │  │ Executor │
           │ (tasks,  │  │ (tasks,  │  │ (tasks,  │
           │  cache)  │  │  cache)  │  │  cache)  │
           └────▲─────┘  └────▲─────┘  └────▲─────┘
                │             │             │
                └─────────────┼─────────────┘
                       coordinates tasks,
                       collects results
                               │
                        ┌──────┴──────┐
                        │   Driver    │  ← runs your PySpark script
                        │ (SparkSession)│
                        └─────────────┘
```

## Jobs, stages, and tasks

When your driver calls an **action** (an operation that forces computation
and produces a result — `.count()`, `.collect()`, `.write.parquet(...)`, etc.),
Spark breaks the work into a hierarchy:

- **Job**: triggered by one action. A single script can trigger many jobs
  (one per action called).
- **Stage**: a job is split into stages at points where data must be
  reshuffled across the network (e.g. before a `groupBy` or a join that
  isn't broadcast). Stages run sequentially; a later stage typically depends
  on the output of an earlier one.
- **Task**: the smallest unit of work — one stage running on one partition.
  If a stage's input has 200 partitions, that stage runs as 200 tasks, and
  those tasks are distributed across whatever executors are available,
  running in parallel up to the number of available cores.

This matters for building intuition: **operations that don't require moving
data between partitions (like `filter` or `select`) are cheap and stay
within a stage; operations that need data grouped or matched across
partitions (like `groupBy`, `join`, `orderBy`) trigger a shuffle and a new
stage.** You'll see this directly with `.explain()` in Level 3, but the
vocabulary starts here.

## Lazy evaluation: why nothing runs until you ask

PySpark code you write does *not* execute line by line the way a normal
Python script does. Instead:

- **Transformations** (`select`, `filter`, `withColumn`, `groupBy`, `join`,
  ...) are **lazy** — calling them just adds a step to a logical plan. No
  data is read or processed yet.
- **Actions** (`.count()`, `.collect()`, `.show()`, `.write...`) are
  **eager** — calling one triggers Spark to optimize the accumulated plan
  and actually execute it across the cluster.

```python
# NOT EXECUTED against a live cluster in this environment — traced through
# by hand to show what happens at each line, which is the point of this module.

df = spark.read.csv("orders.csv", header=True, inferSchema=True)  # lazy: no read yet
filtered = df.filter(df.country == "US")                          # lazy: just records the step
selected = filtered.select("id", "amount")                        # lazy: still nothing has run

selected.show()   # ACTION: only now does Spark actually read orders.csv,
                   # apply the filter, apply the select, and print rows
```

This is a deliberate design choice: by waiting until an action is called,
Spark's **Catalyst optimizer** can look at the *entire* chain of
transformations at once and rewrite it for efficiency — for example,
pushing the `country == "US"` filter down so it's applied while reading the
file (skipping irrelevant data early) rather than reading everything first
and filtering afterward. You get this optimization for free just by writing
DataFrame code naturally.

## Worked example: tracing a plan by hand

Given this snippet (again, reasoned through by hand — not run against a live
cluster here, since this module is purely conceptual):

```python
df = spark.read.parquet("events.parquet")
result = (
    df.filter(df.event_type == "click")
      .select("user_id", "page")
      .groupBy("page")
      .count()
)
result.show()
```

Tracing it against the concepts above:

1. Lines building `df` and `result` are all **transformations** — nothing
   executes yet, Spark just accumulates a logical plan.
2. `.show()` is the **action** that triggers a **job**.
3. The job splits into (at least) two **stages**: Stage 1 reads the Parquet
   partitions, applies `filter` and `select` (no shuffle needed — each
   partition can do this independently), and Stage 2 performs the `groupBy`
   + `count`, which requires a **shuffle** (rows with the same `page` value
   need to end up together, possibly on a different executor than where they
   started).
4. Each stage runs as multiple **tasks** — one per partition of `events.parquet`.
5. The driver collects the small, aggregated result (one row per distinct
   `page`) and prints it via `.show()`.

## Exercise

Using the same reasoning as the worked example, trace through this snippet
by hand (write your answers in a notes file — no cluster needed):

```python
df = spark.read.json("logs.json")
active_users = (
    df.filter(df.status == "active")
      .select("user_id", "country")
      .distinct()
)
by_country = active_users.groupBy("country").count()
by_country.write.csv("output/active_by_country")
```

1. List every transformation and identify the single action.
2. How many jobs does this script trigger? (Hint: count the actions.)
3. Which operation(s) likely require a shuffle, and why?
4. If `logs.json` is split into 50 partitions, how many tasks would you
   expect in the stage that performs the initial `filter` + `select`?

Answers: (1) `read.json`, `filter`, `select`, `distinct`, `groupBy` are
transformations; `write.csv` is the action. (2) One job — there's exactly one
action. (3) `distinct` and `groupBy` both require rows to be compared/grouped
across partitions, so both trigger shuffles. (4) 50 tasks — one per input
partition, since `filter`/`select` don't change partition count on their own.
