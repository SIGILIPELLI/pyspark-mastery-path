---
title: "Final Quiz"
description: "Test what you have learned across all four levels of PySpark Mastery Path with a 19-question final quiz."
---

# Final Quiz

19 questions drawn from all four levels of this course. You need **70% (14 out of 19)** to pass —
you can retake it as many times as you like. Your best result is saved in this browser.

<div id="mp-quiz-root"></div>

<script type="application/json" id="mp-quiz-data">
[
  {
    "question": "What does this print?\n```python\ndf = spark.createDataFrame([(1, \"a\"), (1, \"a\"), (2, \"b\")], [\"id\", \"v\"])\nprint(df.count(), df.distinct().count(), df.dropDuplicates([\"id\"]).count())\n```\n",
    "options": [
      "3 2 3",
      "3 3 2",
      "3 2 2",
      "2 2 2"
    ],
    "correctIndex": 2,
    "explanation": "distinct vs dropDuplicates. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "What does this print?\n```python\ndf = spark.createDataFrame([(\"a\", 1), (\"a\", None), (\"b\", 2)], [\"k\", \"v\"])\nprint(df.agg(F.count(\"*\"), F.count(\"v\"), F.sum(\"v\")).collect()[0])\n```\n",
    "options": [
      "Row(count(1)=3, count(v)=2, sum(v)=3)",
      "Error",
      "Row(count(1)=2, count(v)=2, sum(v)=3)",
      "Row(count(1)=3, count(v)=3, sum(v)=3)"
    ],
    "correctIndex": 0,
    "explanation": "count(col) skips nulls. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "What does this print?\n```python\ndf = spark.createDataFrame([(\"x\", 5), (\"x\", 7), (\"y\", 1)], [\"k\", \"v\"])\nw = Window.partitionBy(\"k\").orderBy(F.desc(\"v\"))\nprint([tuple(r) for r in df.withColumn(\"rn\", F.row_number().over(w)).filter(\"rn = 1\").orderBy(\"k\").collect()])\n```\n",
    "options": [
      "Error",
      "[('x', 7, 1), ('y', 1, 1)]",
      "[('x', 5, 1), ('y', 1, 1)]",
      "[('x', 7, 1)]"
    ],
    "correctIndex": 1,
    "explanation": "Top-1 per group with row_number. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "What does this print?\n```python\na = spark.createDataFrame([(1, \"x\"), (2, \"y\"), (3, \"z\")], [\"id\", \"v\"])\nb = spark.createDataFrame([(1,), (3,)], [\"id\"])\nprint(a.join(b, \"id\", \"left_semi\").count(), a.join(b, \"id\", \"left_anti\").collect())\n```\n",
    "options": [
      "2 [Row(id=2, v='y')]",
      "2 [Row(id=1, v='x')]",
      "Error",
      "3 [Row(id=2, v='y')]"
    ],
    "correctIndex": 0,
    "explanation": "Semi and anti joins filter without adding columns. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "What does this print?\n```python\ndf = spark.createDataFrame([(\"2026-03-05\",), (\"bad\",)], [\"d\"])\nprint([r.m for r in df.select(F.month(F.to_date(\"d\")).alias(\"m\")).collect()])\n```\n",
    "options": [
      "[3]",
      "Error",
      "[3, None]",
      "[3, 0]"
    ],
    "correctIndex": 2,
    "explanation": "Unparseable dates become null. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "What does this print?\n```python\ndf = spark.range(10)\nprint(df.repartition(5).rdd.getNumPartitions(), df.coalesce(1).rdd.getNumPartitions())\n```\n",
    "options": [
      "Error",
      "5 1",
      "5 5",
      "1 5"
    ],
    "correctIndex": 1,
    "explanation": "repartition vs coalesce. Verified by running it on PySpark 3.5 (local mode)."
  },
  {
    "question": "A groupBy job has one task taking 20 minutes while the rest finish in seconds. What's the likely cause?",
    "options": [
      "Network",
      "Disk",
      "Python",
      "Data skew on a hot key"
    ],
    "correctIndex": 3,
    "explanation": "Check the stage's task distribution."
  },
  {
    "question": "When should you broadcast a table?",
    "options": [
      "Both",
      "Never",
      "Always",
      "When it is small enough to fit in executor memory, avoiding a shuffle of the large side"
    ],
    "correctIndex": 3,
    "explanation": "Tune the threshold."
  },
  {
    "question": "Why define a schema instead of inferSchema in production?",
    "options": [
      "Style",
      "Speed only",
      "Law",
      "It avoids an extra pass and prevents surprise types"
    ],
    "correctIndex": 3,
    "explanation": "Stable contracts."
  },
  {
    "question": "What makes a write idempotent?",
    "options": [
      "Append",
      "Sort",
      "Cache",
      "Overwriting a partition or using MERGE so a retry doesn't duplicate rows"
    ],
    "correctIndex": 3,
    "explanation": "Safe retries."
  },
  {
    "question": "What causes many tiny output files?",
    "options": [
      "Cache",
      "Too many partitions at write time; coalesce/repartition first",
      "Skew",
      "Few"
    ],
    "correctIndex": 1,
    "explanation": "Small file problem."
  },
  {
    "question": "A job OOMs on a join. First checks?",
    "options": [
      "More code",
      "Restart",
      "Cache",
      "Broadcast size, partition counts and skew in the Spark UI"
    ],
    "correctIndex": 3,
    "explanation": "Diagnose before scaling."
  },
  {
    "question": "What does AQE do?",
    "options": [
      "Cache",
      "Uses runtime statistics to coalesce partitions, change join strategy and split skewed partitions",
      "Schema",
      "Retry"
    ],
    "correctIndex": 1,
    "explanation": "Adaptive."
  },
  {
    "question": "What does a watermark do in streaming?",
    "options": [
      "Caches",
      "Bounds how late data may arrive so old state can be dropped",
      "Marks",
      "Keys"
    ],
    "correctIndex": 1,
    "explanation": "Event time."
  },
  {
    "question": "Why make transformations pure functions of DataFrames?",
    "options": [
      "Law",
      "Cost",
      "They're easy to unit test on small local data",
      "Speed"
    ],
    "correctIndex": 2,
    "explanation": "Testability."
  },
  {
    "question": "Where do failing records go in a robust pipeline?",
    "options": [
      "Dropped",
      "A quarantine table with the reason, plus an alert",
      "Ignored",
      "Kept"
    ],
    "correctIndex": 1,
    "explanation": "Data quality."
  },
  {
    "question": "What does Delta Lake add to a data lake?",
    "options": [
      "Speed only",
      "ACID transactions, schema enforcement and time travel",
      "SQL",
      "Cache"
    ],
    "correctIndex": 1,
    "explanation": "Lakehouse."
  },
  {
    "question": "What is the right way to compare two tuning attempts?",
    "options": [
      "Run both on the same data and compare runtime, shuffle and spill metrics",
      "Once",
      "Feel",
      "Guess"
    ],
    "correctIndex": 0,
    "explanation": "Evidence."
  },
  {
    "question": "What does dynamic partition overwrite protect?",
    "options": [
      "Only the partitions in the new data are replaced",
      "All data",
      "Cache",
      "Schema"
    ],
    "correctIndex": 0,
    "explanation": "Incremental loads."
  }
]
</script>

## Coding challenges

Try these on your own computer, then compare your output. Nothing you write is sent anywhere.

### CSV to Parquet ETL with quarantine and reconciliation

Read the raw CSV with an explicit all-string schema, cast amount and date, quarantine rows that fail with a reason, de-duplicate by order_id, reconcile counts, report monthly revenue and the largest order per country, then write country-partitioned Parquet and read it back.

**Starter code**

```python
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
import os, tempfile

# 1. Raw CSV with a few bad rows.
raw = """order_id,customer,country,amount,order_date
1,alice,US,120.50,2026-01-05
2,bob,IN,75.00,2026-01-06
2,bob,IN,75.00,2026-01-06
3,carla,DE,abc,2026-01-07
4,dan,US,300.00,2026-02-01
5,erin,IN,,2026-02-02
6,fay,US,45.25,2026-02-03
7,gus,DE,210.00,not-a-date
8,hana,IN,99.99,2026-02-10
"""
work = tempfile.mkdtemp()
open(work + "/orders.csv", "w").write(raw)

schema = StructType([StructField(n, StringType()) for n in ["order_id", "customer", "country", "amount", "order_date"]])
df = spark.read.csv(work + "/orders.csv", header=True, schema=schema)

# 2. TODO: cast amount to decimal(10,2) and order_date to a date (yyyy-MM-dd); add a `reason` column
#    ("bad_amount" when the amount does not parse, else "bad_date" when the date does not parse, else null)
#    good = rows without a reason, de-duplicated by order_id; bad = rows with a reason.

# 3. TODO: print  input=<n> good=<n> quarantined=<n> duplicates_removed=<n>  and whether the counts reconcile,
#    then the sorted list of (order_id, reason) for the quarantined rows.

# 4. TODO: monthly orders and revenue per country (print "<country> <yyyy-MM> orders=<n> revenue=<amount>"),
#    then the largest order per country using a window (row_number) -> list of (country, customer, amount).

# 5. TODO: write partitioned Parquet (by country), print the partition folders and whether a read-back row count matches.
```

**Expected output**

```text
input=9 good=5 quarantined=3 duplicates_removed=1
reconciled: True
quarantine: [('3', 'bad_amount'), ('5', 'bad_amount'), ('7', 'bad_date')]
IN 2026-01 orders=1 revenue=75.00
IN 2026-02 orders=1 revenue=99.99
US 2026-01 orders=1 revenue=120.50
US 2026-02 orders=2 revenue=345.25
top order per country: [('IN', 'hana', '99.99'), ('US', 'dan', '300.00')]
partitions: ['country=IN', 'country=US']
round trip: True
```

<details><summary>Show solution and explanation</summary>

<pre><code>from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master(&quot;local[2]&quot;).config(&quot;spark.ui.enabled&quot;, &quot;false&quot;).config(&quot;spark.sql.shuffle.partitions&quot;, &quot;4&quot;).getOrCreate()
spark.sparkContext.setLogLevel(&quot;ERROR&quot;)
import os, tempfile

# 1. Raw CSV with a few bad rows.
raw = &quot;&quot;&quot;order_id,customer,country,amount,order_date
1,alice,US,120.50,2026-01-05
2,bob,IN,75.00,2026-01-06
2,bob,IN,75.00,2026-01-06
3,carla,DE,abc,2026-01-07
4,dan,US,300.00,2026-02-01
5,erin,IN,,2026-02-02
6,fay,US,45.25,2026-02-03
7,gus,DE,210.00,not-a-date
8,hana,IN,99.99,2026-02-10
&quot;&quot;&quot;
work = tempfile.mkdtemp()
open(work + &quot;/orders.csv&quot;, &quot;w&quot;).write(raw)

schema = StructType([StructField(n, StringType()) for n in [&quot;order_id&quot;, &quot;customer&quot;, &quot;country&quot;, &quot;amount&quot;, &quot;order_date&quot;]])
df = spark.read.csv(work + &quot;/orders.csv&quot;, header=True, schema=schema)

# 2. Parse, then split valid and quarantined rows (with a reason).
parsed = (df.withColumn(&quot;amount_d&quot;, F.col(&quot;amount&quot;).cast(&quot;decimal(10,2)&quot;))
            .withColumn(&quot;date_d&quot;, F.to_date(&quot;order_date&quot;, &quot;yyyy-MM-dd&quot;)))
reason = (F.when(F.col(&quot;amount_d&quot;).isNull(), &quot;bad_amount&quot;).when(F.col(&quot;date_d&quot;).isNull(), &quot;bad_date&quot;))
checked = parsed.withColumn(&quot;reason&quot;, reason)
good = checked.filter(F.col(&quot;reason&quot;).isNull()).dropDuplicates([&quot;order_id&quot;])
bad = checked.filter(F.col(&quot;reason&quot;).isNotNull())

# 3. Reconcile counts.
total_in, n_good, n_bad = df.count(), good.count(), bad.count()
dupes = checked.filter(F.col(&quot;reason&quot;).isNull()).count() - n_good
print(f&quot;input={total_in} good={n_good} quarantined={n_bad} duplicates_removed={dupes}&quot;)
print(&quot;reconciled:&quot;, total_in == n_good + n_bad + dupes)
print(&quot;quarantine:&quot;, sorted((r.order_id, r.reason) for r in bad.collect()))

# 4. Monthly revenue by country with the top customer per country (window function).
clean = good.select(&quot;order_id&quot;, &quot;customer&quot;, &quot;country&quot;, F.col(&quot;amount_d&quot;).alias(&quot;amount&quot;), F.date_format(&quot;date_d&quot;, &quot;yyyy-MM&quot;).alias(&quot;month&quot;))
rev = clean.groupBy(&quot;country&quot;, &quot;month&quot;).agg(F.count(&quot;*&quot;).alias(&quot;orders&quot;), F.sum(&quot;amount&quot;).alias(&quot;revenue&quot;)).orderBy(&quot;country&quot;, &quot;month&quot;)
for r in rev.collect(): print(f&quot;{r.country} {r.month} orders={r.orders} revenue={r.revenue}&quot;)
w = Window.partitionBy(&quot;country&quot;).orderBy(F.desc(&quot;amount&quot;))
top = clean.withColumn(&quot;rn&quot;, F.row_number().over(w)).filter(&quot;rn = 1&quot;).orderBy(&quot;country&quot;)
print(&quot;top order per country:&quot;, [(r.country, r.customer, str(r.amount)) for r in top.collect()])

# 5. Write partitioned Parquet and read it back.
out = work + &quot;/out&quot;
clean.write.partitionBy(&quot;country&quot;).mode(&quot;overwrite&quot;).parquet(out)
print(&quot;partitions:&quot;, sorted(p for p in os.listdir(out) if p.startswith(&quot;country=&quot;)))
print(&quot;round trip:&quot;, spark.read.parquet(out).count() == n_good)
</code></pre>

<p>Parsing into typed columns turns bad values into nulls so rows can be quarantined with a reason, and the counts must add up (input = good + quarantined + duplicates). Verified by running the solution on PySpark 3.5 in local mode.</p>
</details>

### Join strategy, skew salting and a quality gate

Show that a broadcast hint changes the physical join without changing the result, find the hottest key, prove that two-stage salted aggregation equals the direct aggregation, and write a small quality gate that stops the pipeline when checks fail.

**Starter code**

```python
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
fact = spark.createDataFrame([("hot", i) for i in range(60)] + [("k%d" % i, i) for i in range(10)], ["key", "v"])
dim = spark.createDataFrame([("hot", "H"), ("k0", "A"), ("k1", "B"), ("k2", "C")], ["key", "label"])

def plan_has(df, text):
    return text in df._jdf.queryExecution().executedPlan().toString()

# TODO 1: disable automatic broadcast (spark.sql.autoBroadcastJoinThreshold = -1), left-join fact with dim, and print
#         "auto broadcast off -> SortMergeJoin" (check the plan); then repeat with F.broadcast(dim) and print
#         "with broadcast hint -> BroadcastHashJoin"; print whether both give the same rows and the row count.
# TODO 2: find the hottest key and its share of the rows ("hottest key: <key> with <pct>% of rows").
# TODO 3: salt (v % 4), aggregate in two stages (sum and count per key+salt, then per key), compare with the direct
#         aggregation, and print the number of partial groups for the hot key and its total.
# TODO 4: a gate(df, name, cond) helper that counts violating rows; run four checks (v not null, v >= 0,
#         dimension matched, dim key unique), print PASS / FAIL(n) for each and finally STOP or CONTINUE.
```

**Expected output**

```text
auto broadcast off -> SortMergeJoin
with broadcast hint -> BroadcastHashJoin
same result: True | rows: 70
hottest key: hot with 86% of rows
salted == direct: True
hot key partial groups: 4
hot total: 1770
v not null           PASS
v >= 0               PASS
dimension matched    FAIL(7)
dim key unique       PASS
pipeline: STOP
```

<details><summary>Show solution and explanation</summary>

<pre><code>from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master(&quot;local[2]&quot;).config(&quot;spark.ui.enabled&quot;, &quot;false&quot;).config(&quot;spark.sql.shuffle.partitions&quot;, &quot;4&quot;).getOrCreate()
spark.sparkContext.setLogLevel(&quot;ERROR&quot;)
# Join strategy, data skew with salting, and a data-quality gate.
fact = spark.createDataFrame([(&quot;hot&quot;, i) for i in range(60)] + [(&quot;k%d&quot; % i, i) for i in range(10)], [&quot;key&quot;, &quot;v&quot;])
dim = spark.createDataFrame([(&quot;hot&quot;, &quot;H&quot;), (&quot;k0&quot;, &quot;A&quot;), (&quot;k1&quot;, &quot;B&quot;), (&quot;k2&quot;, &quot;C&quot;)], [&quot;key&quot;, &quot;label&quot;])

def plan_has(df, text):
    return text in df._jdf.queryExecution().executedPlan().toString()

spark.conf.set(&quot;spark.sql.autoBroadcastJoinThreshold&quot;, &quot;-1&quot;)
smj = fact.join(dim, &quot;key&quot;, &quot;left&quot;)
print(&quot;auto broadcast off -&gt;&quot;, &quot;SortMergeJoin&quot; if plan_has(smj, &quot;SortMergeJoin&quot;) else &quot;other&quot;)
bhj = fact.join(F.broadcast(dim), &quot;key&quot;, &quot;left&quot;)
print(&quot;with broadcast hint -&gt;&quot;, &quot;BroadcastHashJoin&quot; if plan_has(bhj, &quot;BroadcastHashJoin&quot;) else &quot;other&quot;)
print(&quot;same result:&quot;, sorted(map(tuple, smj.collect())) == sorted(map(tuple, bhj.collect())), &quot;| rows:&quot;, smj.count())

# Skew: one key holds most of the rows.
counts = fact.groupBy(&quot;key&quot;).count().orderBy(F.desc(&quot;count&quot;)).collect()
share = counts[0][&quot;count&quot;] / fact.count()
print(f&quot;hottest key: {counts[0][&#x27;key&#x27;]} with {share:.0%} of rows&quot;)

# Two-stage aggregation with salting gives the same answer while spreading the hot key.
N = 4
salted = fact.withColumn(&quot;salt&quot;, (F.col(&quot;v&quot;) % N))
partial = salted.groupBy(&quot;key&quot;, &quot;salt&quot;).agg(F.sum(&quot;v&quot;).alias(&quot;s&quot;), F.count(&quot;*&quot;).alias(&quot;c&quot;))
final = partial.groupBy(&quot;key&quot;).agg(F.sum(&quot;s&quot;).alias(&quot;total&quot;), F.sum(&quot;c&quot;).alias(&quot;n&quot;)).orderBy(&quot;key&quot;)
direct = fact.groupBy(&quot;key&quot;).agg(F.sum(&quot;v&quot;).alias(&quot;total&quot;), F.count(&quot;*&quot;).alias(&quot;n&quot;)).orderBy(&quot;key&quot;)
print(&quot;salted == direct:&quot;, final.collect() == direct.collect())
print(&quot;hot key partial groups:&quot;, partial.filter(&quot;key = &#x27;hot&#x27;&quot;).count())
print(&quot;hot total:&quot;, final.filter(&quot;key = &#x27;hot&#x27;&quot;).collect()[0][&quot;total&quot;])

# Data-quality gate.
def gate(df, name, cond):
    bad = df.filter(~cond).count()
    return (name, &quot;PASS&quot; if bad == 0 else f&quot;FAIL({bad})&quot;)
joined = smj
checks = [
    gate(fact, &quot;v not null&quot;, F.col(&quot;v&quot;).isNotNull()),
    gate(fact, &quot;v &gt;= 0&quot;, F.col(&quot;v&quot;) &gt;= 0),
    gate(joined, &quot;dimension matched&quot;, F.col(&quot;label&quot;).isNotNull()),
    gate(dim, &quot;dim key unique&quot;, F.lit(dim.select(&quot;key&quot;).distinct().count() == dim.count())),
]
for name, res in checks: print(f&quot;{name:20s} {res}&quot;)
print(&quot;pipeline:&quot;, &quot;STOP&quot; if any(r.startswith(&quot;FAIL&quot;) for _, r in checks) else &quot;CONTINUE&quot;)
</code></pre>

<p>Plans show whether Spark shuffles or broadcasts; salting spreads a hot key across partitions and a second aggregation restores the exact answer; gates stop bad data early. Verified by running the solution on PySpark 3.5 in local mode (small data, so timings would not show the real benefit).</p>
</details>
