# 06 · Multi Tenant Cluster Strategy

!!! note "Not executed against a live cluster in this environment"
    Config and behavior below are hand-traced against documented Spark
    resource-management behavior, not run against a live cluster here.

Once more than one team or job shares a Spark cluster, "it works on my
job" stops being the only requirement — you need jobs to coexist without
one starving another, predictable resource allocation, and a way to
reason about who gets what when demand exceeds capacity. This module
covers the mechanisms: dynamic allocation, scheduler pools, and
resource-isolation patterns.

## Static allocation: the naive multi-tenant failure mode

```python
# Without dynamic allocation, a job requests a fixed executor count up
# front and holds it for the job's entire lifetime — even during idle
# periods (e.g., waiting on a slow upstream read).
static_conf = {
    "spark.executor.instances": "50",   # held for the whole job, whether busy or not
}
```

On a shared cluster, several jobs each statically claiming 50 executors
"just in case" exhausts cluster capacity fast, and idle-but-still-holding
jobs block genuinely active ones from getting resources — the core
multi-tenancy problem.

## Dynamic allocation: scale executors with actual demand

```python
dynamic_conf = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "50",
    "spark.dynamicAllocation.initialExecutors": "5",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",     # release an idle executor after 60s
    "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",  # request more if tasks queue for 1s
    "spark.shuffle.service.enabled": "true",   # REQUIRED: lets released executors' shuffle
                                                 # data still be served by a separate service
}
```

`spark.shuffle.service.enabled` is the easy-to-miss prerequisite: without
an external shuffle service, releasing an executor mid-job would also
delete any shuffle files it was still serving to other tasks, breaking
correctness — dynamic allocation depends on decoupling shuffle-file
serving from the executor process's own lifetime.

## Fair scheduler pools: sharing one SparkContext across jobs

Within a single long-lived application (e.g., a shared notebook server or
a job server pattern), the **FAIR** scheduler with pools lets multiple
concurrently-submitted jobs share cluster resources round-robin instead
of the default FIFO queue where job 2 waits entirely for job 1 to finish.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("multi-tenant-app")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.allocation.file", "/opt/spark/conf/fairscheduler.xml")
    .getOrCreate()
)
```

```xml
<!-- fairscheduler.xml -->
<allocations>
  <pool name="high_priority">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>4</minShare>
  </pool>
  <pool name="default">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>0</minShare>
  </pool>
</allocations>
```

```python
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "high_priority")
important_job_result = spark.range(0, 10_000_000).groupBy((spark.range(0,1).id)).count()

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "default")
background_job_result = spark.range(0, 100_000_000).count()
```

`weight=3` means the `high_priority` pool gets roughly 3x the resource
share of `default` when both have pending work, and `minShare=4`
guarantees it at least 4 cores even under contention — this is how you
express "interactive dashboard queries should never wait behind a
nightly batch backfill" within one shared cluster.

## Resource isolation across tenants: separate clusters vs. one shared cluster

```python
# Approach A: separate ephemeral clusters per team/workload — strongest
# isolation, simplest reasoning about "did tenant X starve tenant Y", but
# loses the cost efficiency of a shared resource pool during off-peak hours.

# Approach B: one shared cluster with dynamic allocation + fair scheduling +
# YARN/Kubernetes resource queues -- better utilization, but requires
# active governance (quotas, monitoring) to prevent one tenant's runaway
# job from degrading everyone else.
```

Most organizations land on a hybrid: separate clusters for genuinely
different SLA tiers (e.g., a customer-facing real-time pipeline never
shares a cluster with ad-hoc analyst notebooks), and shared clusters with
scheduler pools + quotas within a tier.

## YARN/Kubernetes queue-level quotas

```python
# When running on YARN, queue assignment (separate from Spark's internal
# fair scheduler pools) caps a whole application's resource ceiling at
# the cluster-manager level -- this is the outermost isolation boundary,
# enforced even if a job's own Spark config tries to request more.
yarn_submit_conf = {
    "spark.yarn.queue": "team_analytics",   # maps to a YARN capacity-scheduler queue
}

# On Kubernetes, the equivalent is a ResourceQuota / LimitRange on the
# namespace the Spark driver/executor pods run in -- enforced by the
# Kubernetes scheduler, independent of Spark's own settings entirely.
```

## Preventing one job from starving others: task/executor limits

```python
# Cap how much of the shared cluster ANY single job can consume, even
# under dynamic allocation, as a blunt but effective fairness backstop:
per_job_cap_conf = {
    "spark.dynamicAllocation.maxExecutors": "20",   # even if cluster has 200 available
    "spark.task.cpus": "1",                          # don't let one task hog multiple cores silently
}
```

## Monitoring multi-tenant contention

```python
# Symptoms to watch across tenants, using the tools from module 8:
# - A job's "scheduler delay" (time between task creation and start)
#   climbing during hours when other tenants are also busy -> genuine
#   resource contention, not a bug in that specific job.
# - dynamicAllocation.maxExecutors being hit consistently for one tenant
#   -> either raise their ceiling or investigate why they need that much.
# - A FAIR pool's actual observed share diverging from its configured
#   weight over time -> possible scheduler misconfiguration or one job
#   holding resources longer than its fair share via long-running tasks
#   that don't yield.
```

## Worked example: designing pools for three workload types

```python
# Workload profile:
# - "realtime_dashboards": interactive, short queries, needs LOW latency
# - "nightly_etl": long batch jobs, latency-insensitive, needs throughput
# - "adhoc_analyst": unpredictable, should never starve the other two

fairscheduler_design = {
    "realtime_dashboards": {"weight": 5, "minShare": 8},
    "nightly_etl":         {"weight": 2, "minShare": 0},
    "adhoc_analyst":       {"weight": 1, "minShare": 0},
}
for pool, cfg in fairscheduler_design.items():
    print(f"{pool}: weight={cfg['weight']}, minShare={cfg['minShare']}")
```

`realtime_dashboards` gets both a high weight (wins contention) and a
guaranteed minimum share (never fully starved even if the other two pools
are saturated) — `nightly_etl` and `adhoc_analyst` compete for the
remainder proportional to their weights, with ETL prioritized 2:1 over
ad-hoc work since it has firmer completion-time expectations.

## Exercise

1. Explain, specifically, why `spark.shuffle.service.enabled=true` is a
   hard prerequisite for `dynamicAllocation.enabled=true` in terms of
   what would break without it.
2. Design a `fairscheduler.xml` for a cluster with a "critical_alerts"
   pool that must always win contention against everything else, and a
   "batch" pool for everything else — justify your `weight`/`minShare`
   choices.
3. A team's job consistently hits `maxExecutors` and runs slow during
   business hours only. Name two different fixes (one config-level, one
   architectural) and the tradeoff between them.
