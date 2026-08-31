# 03 · Cost Performance Tradeoffs

!!! note "Not executed against a live cluster in this environment"
    Figures below are illustrative reasoning about cost/performance
    tradeoffs, not measurements from a live cluster run here.

Every performance win from Level 3 costs something — cluster size, engineer
time, or operational complexity. In production, "make it faster" isn't
free, and the right answer is usually "faster than what, and is it worth
what it costs." This module is about reasoning explicitly about that
tradeoff rather than optimizing blindly.

## The basic cost model for a cloud Spark job

```python
# A rough cost model: cost = (cluster hourly rate) x (job duration in hours)
# for on-demand cluster time, plus storage costs that are usually much smaller
# for a batch job's transient compute.

def job_cost(num_executors, executor_hourly_rate, duration_hours, driver_hourly_rate=0.0):
    return (num_executors * executor_hourly_rate + driver_hourly_rate) * duration_hours

# 20 executors at $0.40/hr each, running for 45 minutes:
cost_before = job_cost(20, 0.40, 45 / 60)
print(round(cost_before, 2))   # 6.00

# After tuning (module 5's changes): same cluster, 12 minutes:
cost_after = job_cost(20, 0.40, 12 / 60)
print(round(cost_after, 2))    # 1.60
```

Reducing wall-clock time on a transient (auto-scaling, pay-per-use)
cluster directly reduces cost — this is the easy, uncontroversial case:
performance tuning that shortens a job *always* pays for itself if the
cluster is billed by the hour and shuts down after the job.

## The less obvious case: scaling out vs. scaling up

```python
# Scaling OUT (more executors, same size each): often near-linear speedup
# for embarrassingly parallel or well-partitioned work, cost roughly flat
# per unit of work done since (executors x rate x time) ~ constant if
# time drops proportionally to added executors.
scale_out_cost = job_cost(num_executors=40, executor_hourly_rate=0.40, duration_hours=22.5 / 60)
print(round(scale_out_cost, 2))   # 6.00 -- same total cost, half the wall-clock time

# Scaling UP (fewer, bigger executors): can reduce shuffle volume (fewer
# network hops for a given partition count) but bigger executors often
# cost more than linearly per unit of memory/CPU on cloud instance pricing.
scale_up_cost = job_cost(num_executors=10, executor_hourly_rate=1.10, duration_hours=30 / 60)
print(round(scale_up_cost, 2))   # 5.50 -- cheaper AND faster here, but instance-pricing-dependent
```

Scaling out is usually "free" in cost terms if your job parallelizes well
(same total cost, less wall-clock latency) — the real question to ask
before adding executors is whether the job is actually
parallelism-bound or whether it's bound by a straggler/skew problem
(module 3) that more executors won't fix at all.

## When more tuning effort isn't worth it

```python
# Engineer time has a cost too. If a senior engineer costs $150k/yr
# (~$75/hr loaded), and a job runs once a day:
engineer_hourly = 75
tuning_effort_hours = 6
tuning_cost = engineer_hourly * tuning_effort_hours
print(tuning_cost)   # 450

# Savings from the tuning: 15 fewer minutes/day on a 20-executor job
daily_savings = job_cost(20, 0.40, 15 / 60)
annual_savings = daily_savings * 365
print(round(annual_savings, 2))   # 730.0

payback_days = tuning_cost / daily_savings
print(round(payback_days, 1))   # 22.5 days
```

A tuning effort with a ~3-week payback on a daily job is usually easy to
justify. The same 6 hours of tuning effort on a job that runs *once a
month* would have a payback measured in years — not obviously worth
prioritizing over other work. Always compute (or at least estimate) this
payback before spending days chasing the last 10% of performance.

## Spot/preemptible instances: cheap compute with a durability cost

```python
on_demand_rate = 0.40
spot_rate = 0.12          # ~70% discount, typical order of magnitude

# Spot instances can be reclaimed mid-job. Spark handles executor loss via
# lineage recomputation (module 9) automatically, but a job using spot
# executors needs SOME tolerance for retries/recomputation time, and the
# driver itself should almost always stay on-demand -- losing the driver
# kills the whole application, not just one executor's work.
mixed_fleet_cost = job_cost(num_executors=18, executor_hourly_rate=spot_rate, duration_hours=1,
                             driver_hourly_rate=on_demand_rate)
on_demand_fleet_cost = job_cost(num_executors=18, executor_hourly_rate=on_demand_rate, duration_hours=1,
                                  driver_hourly_rate=on_demand_rate)
print(round(mixed_fleet_cost, 2), round(on_demand_fleet_cost, 2))   # 2.56 vs 7.6
```

Checkpointing long-lineage stages (module 9) becomes more valuable, not
less, on a spot fleet — a reclaimed executor triggers lineage recompute,
and a checkpoint bounds how much work that recompute actually costs.

## Storage format cost implications

```python
# Cheaper to store, more expensive to query repeatedly:
gzip_storage_cost_per_tb_month = 20    # illustrative
snappy_storage_cost_per_tb_month = 32  # illustrative, larger files

# But gzip is NOT splittable and costs more CPU-hours to decompress on
# every read -- for a table queried dozens of times a day, the extra
# compute cost from gzip's decompression overhead usually swamps the
# storage savings within days.
```

The module 5 recommendation (Snappy or zstd, not gzip, for Spark inputs)
is itself a cost decision, not just a performance one — cheaper storage
that costs more compute on every read is a bad trade for any
frequently-queried table.

## Worked example: deciding whether to fix a known skew problem

A job runs nightly, costs $6/night unoptimized (40 min on 20 executors at
$0.40/hr) due to a known skew issue (module 3). Fixing it (salting, ~1 day
of engineering) would cut runtime to 12 minutes.

```python
current_annual_cost = job_cost(20, 0.40, 40 / 60) * 365
fixed_annual_cost = job_cost(20, 0.40, 12 / 60) * 365
engineering_cost = 75 * 8   # one engineer-day

annual_savings = current_annual_cost - fixed_annual_cost
payback_days = engineering_cost / (annual_savings / 365)

print(round(current_annual_cost, 2), round(fixed_annual_cost, 2))   # 2190.0, 657.0
print(round(annual_savings, 2), round(payback_days, 1))              # 1533.0, 142.9
```

A ~143-day payback is still generally worth doing for a job that will keep
running indefinitely, but it's a noticeably weaker case than the earlier
22-day example — the decision now depends on what else is competing for
that engineer's day, which a pure performance-tuning mindset wouldn't
surface at all.

## Exercise

1. A job costs $10/run on a 30-executor on-demand cluster and runs
   hourly. Estimate the annual cost, then estimate the payback period for
   a 2-engineer-day tuning effort that would cut runtime by 50%.
2. Explain, in cost terms, why moving the *driver* to a spot instance is a
   worse idea than moving executors to spot, even though both nominally
   save the same per-hour rate.
3. A table is queried 200 times/day by a BI tool and rewritten once/day
   by a batch job. Argue for gzip vs. snappy/zstd compression using the
   cost reasoning from this module, not just "gzip compresses better."
