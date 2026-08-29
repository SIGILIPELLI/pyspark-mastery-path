# 05 · Reading Data (CSV, JSON, Parquet)

!!! note "Not executed against a live cluster in this environment"
    Code and sample outputs below are hand-traced against documented PySpark
    reader behavior, not run against a live cluster here.

Every `spark.read` call follows the same pattern: pick a format
(`.csv`, `.json`, `.parquet`, ...), configure options for that format, and
call `.load(path)` or the format-specific shortcut (`.csv(path)`,
`.json(path)`, `.parquet(path)`). All of them return a lazily-evaluated
DataFrame — nothing is read from disk until an action is called.

## Reading CSV

Suppose `orders.csv` looks like this:

```csv
order_id,customer,country,amount,order_date
1,Alice,US,120.50,2026-01-05
2,Bob,IN,45.00,2026-01-06
3,Carla,US,300.25,2026-01-06
4,Deepak,DE,60.00,2026-01-07
```

```python
df = spark.read.csv(
    "orders.csv",
    header=True,          # first row is column names, not data
    inferSchema=True,     # scan the data to guess types (int, double, string...)
)

df.printSchema()
# root
#  |-- order_id: integer (nullable = true)
#  |-- customer: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- amount: double (nullable = true)
#  |-- order_date: string (nullable = true)   <- note: string, not date, unless told otherwise

df.show()
```

`inferSchema=True` is convenient but has a real cost: Spark must read the
file (or a sample of it, depending on version/settings) *twice* — once to
infer types, once to actually load — which matters on large files. It also
can guess wrong (note `order_date` came back as `string`, not `date`,
because CSV has no native date type — inference only sees text). Module 7
covers specifying an explicit schema to avoid both problems.

Useful CSV options:

```python
df = spark.read.csv(
    "orders.csv",
    header=True,
    inferSchema=True,
    sep=",",              # delimiter (use "\t" for TSV)
    nullValue="NA",       # treat the literal string "NA" as null
    mode="PERMISSIVE",    # default: malformed rows become null-filled rows
                            # instead of failing the whole read
)
```

`mode` is worth knowing about early: `"PERMISSIVE"` (default) keeps
malformed rows with nulls in place of bad fields, `"DROPMALFORMED"` silently
drops them, and `"FAILFAST"` throws an error as soon as one is found — pick
`"FAILFAST"` when you'd rather stop and investigate than silently lose rows.

## Reading JSON

Spark expects **line-delimited JSON** by default (one JSON object per line),
not a single JSON array spanning the whole file:

```json
{"user_id": 1, "event": "login", "ts": "2026-01-05T10:00:00"}
{"user_id": 2, "event": "click", "ts": "2026-01-05T10:01:15"}
{"user_id": 1, "event": "logout", "ts": "2026-01-05T10:15:00"}
```

```python
df = spark.read.json("events.json")
df.printSchema()
# root
#  |-- event: string (nullable = true)
#  |-- ts: string (nullable = true)
#  |-- user_id: long (nullable = true)

df.show(truncate=False)
```

Notice Spark infers schema from JSON automatically (types are explicit in
JSON — numbers, strings, booleans — so this is more reliable than CSV
inference, though nested/array fields can still need attention). If your
file is a single JSON array (`[{...}, {...}]`) rather than line-delimited,
pass `multiLine=True`:

```python
df = spark.read.option("multiLine", True).json("events_as_array.json")
```

## Reading Parquet

**Parquet** is a columnar, binary, self-describing file format — it stores
its own schema and per-column statistics inside the file, and Spark reads
it more efficiently than text formats (CSV/JSON) because it can skip columns
you don't need and skip entire row groups based on stored min/max stats
matching your filters.

```python
df = spark.read.parquet("events.parquet")
df.printSchema()   # schema comes from the file itself — no inference guesswork
df.show()
```

No `header`, `inferSchema`, or `sep` options needed — Parquet already knows
its own schema and types exactly, which is one of the reasons Module 9
(writing data) and later levels favor Parquet as the default storage format
for anything beyond a raw drop zone.

## Reading multiple files / a directory at once

All three readers accept a directory path or a glob and will read every
matching file as one logical DataFrame:

```python
df = spark.read.parquet("data/orders/")            # every file in the folder
df = spark.read.csv("data/logs/2026-01-*.csv", header=True, inferSchema=True)
```

This is exactly how Spark reads partitioned datasets written by `.write`
(Module 9) or by other systems — a "table" in the Spark/data-lake world is
usually just a directory of many files that get read together as one
DataFrame.

## Worked example: comparing the three for the same data

Given the same logical orders data available in all three formats, reading
each and checking row count and schema:

```python
csv_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
json_df = spark.read.json("orders.json")
parquet_df = spark.read.parquet("orders.parquet")

for name, d in [("csv", csv_df), ("json", json_df), ("parquet", parquet_df)]:
    print(name, d.count())   # all three print the same row count, e.g. 4

parquet_df.printSchema()   # most trustworthy schema: stored, not inferred
csv_df.printSchema()       # order_date likely comes back as string
```

The row counts should always agree (same underlying data); the schemas may
not, because CSV/JSON schema inference is a best-effort guess while Parquet's
schema is exact and stored.

## Exercise

You're given a directory `sales/` containing daily CSV exports named
`sales_2026-01-01.csv`, `sales_2026-01-02.csv`, etc., each with columns
`sale_id,product,quantity,price`.

1. Write the `spark.read` call to load all of them as a single DataFrame,
   with headers and schema inference enabled.
2. Suppose two of the daily files have an extra malformed row (missing a
   field). Which `mode` option would let the read succeed while still
   letting you find and inspect those bad rows afterward (rather than
   silently dropping or crashing on them)?
3. If you later receive the same dataset as Parquet instead of CSV, what
   changes about how confident you can be in the inferred schema, and why?

Answer for (2): `mode="PERMISSIVE"` (the default) — malformed rows are kept
with nulls filling missing fields, rather than being dropped
(`"DROPMALFORMED"`) or aborting the whole read (`"FAILFAST"`), so you can
filter for nulls afterward to find exactly which rows had problems.
