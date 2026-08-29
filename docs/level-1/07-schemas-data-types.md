# 07 · Schemas & Data Types

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

## Why explicit schemas matter

Module 5 showed `inferSchema=True` guessing types from CSV data. Inference
is convenient for exploration but risky for production pipelines, for three
concrete reasons:

1. **Performance** — inferring a schema requires an extra pass over the data
   (or a sample of it) before the real read happens.
2. **Correctness** — inference can guess wrong, especially with edge cases
   (a numeric-looking column that should be a zero-padded string ID, e.g.
   ZIP codes like `"02134"` misread as the integer `2134`).
3. **Stability** — if upstream data changes shape slightly (a new column, a
   changed type), silent inference can produce a different schema each run,
   breaking downstream code in ways that are hard to trace.

The fix: define the schema explicitly and pass it to the reader.

## Spark's type system

PySpark types live in `pyspark.sql.types`. The common ones:

| Spark type | Python equivalent | Example |
|---|---|---|
| `StringType` | `str` | `"Alice"` |
| `IntegerType` | `int` (32-bit) | `42` |
| `LongType` | `int` (64-bit) | `9999999999` |
| `DoubleType` | `float` (64-bit) | `19.99` |
| `FloatType` | `float` (32-bit) | `19.99` |
| `BooleanType` | `bool` | `True` |
| `DateType` | `datetime.date` | `2026-01-05` |
| `TimestampType` | `datetime.datetime` | `2026-01-05 10:00:00` |
| `ArrayType(elementType)` | `list` | `[1, 2, 3]` |
| `MapType(keyType, valueType)` | `dict` | `{"a": 1}` |
| `StructType([...])` | nested row/object | `{"street": "...", "city": "..."}` |

## Defining an explicit schema

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,
)

orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer", StringType(), nullable=False),
    StructField("country", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=False),
    StructField("order_date", DateType(), nullable=True),
])

df = spark.read.csv(
    "orders.csv",
    header=True,
    schema=orders_schema,   # explicit schema instead of inferSchema=True
)

df.printSchema()
# root
#  |-- order_id: integer (nullable = false)
#  |-- customer: string (nullable = false)
#  |-- country: string (nullable = true)
#  |-- amount: double (nullable = false)
#  |-- order_date: date (nullable = true)   <- correctly typed as date now
```

Notice: passing `schema=` and `inferSchema=True` together doesn't make
sense — when you provide an explicit schema, Spark uses it directly and
skips inference entirely, which is both faster and predictable. Also notice
`order_date` is now correctly a `DateType`, letting you use date functions
on it directly (covered more in later levels), instead of it silently coming
back as `string` the way Module 5's inferred read did.

!!! note "nullable is a declaration, not an enforced constraint on read"
    Setting `nullable=False` documents intent and helps Spark's optimizer,
    but for most file-based sources (CSV, JSON) Spark does not actually
    reject rows with nulls in a "non-nullable" column at read time — it will
    still load them. Real null-enforcement typically happens in your own
    validation logic (Module 9's Data Quality module in a later level) or in
    systems designed to enforce it, such as some table formats.

## Nested types: StructType and ArrayType

Real-world JSON often has nested structure. Given:

```json
{"user_id": 1, "name": "Alice", "address": {"city": "Austin", "zip": "78701"}, "tags": ["vip", "early_adopter"]}
```

```python
from pyspark.sql.types import ArrayType

user_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType()),
    ])),
    StructField("tags", ArrayType(StringType())),
])

df = spark.read.schema(user_schema).json("users.json")
df.printSchema()
# root
#  |-- user_id: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- address: struct (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- zip: string (nullable = true)
#  |-- tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)

# Access nested fields with dot notation:
df.select("name", "address.city", "tags").show(truncate=False)
```

## Casting columns

Once data is loaded, you can change a column's type with `.cast()`:

```python
df = df.withColumn("order_id", df.order_id.cast("string"))   # int -> string
df = df.withColumn("amount", df.amount.cast(DoubleType()))    # explicit type object

df.printSchema()
```

`.cast()` accepts either a string name (`"string"`, `"int"`, `"double"`,
`"date"`, ...) or a type object imported from `pyspark.sql.types` — both are
equivalent, string names are just more concise for common cases.

A cast that can't succeed for a given value produces `null` for that row
rather than raising an error (e.g. casting the string `"abc"` to
`IntegerType()`), so always check for unexpected nulls after casting
data you don't fully trust.

## printSchema() vs. schema

```python
df.printSchema()   # human-readable tree, prints to stdout, returns None
print(df.schema)   # the actual StructType object, useful for comparing schemas programmatically
print(df.dtypes)   # list of (column_name, type_name) tuples — quick and simple
```

`df.schema` is genuinely useful in pipeline code — for example, asserting
that an incoming file matches an expected schema before processing it
further:

```python
expected_columns = {"order_id", "customer", "country", "amount", "order_date"}
actual_columns = set(df.columns)
assert expected_columns == actual_columns, f"Unexpected columns: {actual_columns ^ expected_columns}"
```

## Worked example: fixing a bad inference

Given CSV data with a ZIP-code-like column that inference would mangle:

```csv
store_id,zip_code,revenue
1,02134,15000.50
2,90210,22000.00
```

```python
# BAD: inferSchema turns "02134" into the integer 2134, losing the leading zero
bad_df = spark.read.csv("stores.csv", header=True, inferSchema=True)
bad_df.show()
# zip_code column would print as 2134, not 02134 -- data corrupted silently

# GOOD: explicit schema keeps zip_code as a string
good_schema = StructType([
    StructField("store_id", IntegerType()),
    StructField("zip_code", StringType()),   # explicitly string, preserves leading zero
    StructField("revenue", DoubleType()),
])
good_df = spark.read.csv("stores.csv", header=True, schema=good_schema)
good_df.show()
# zip_code column correctly prints "02134"
```

This is exactly the kind of silent, hard-to-notice bug explicit schemas
exist to prevent.

## Exercise

You're given `transactions.json` with this shape per line:

```json
{"txn_id": "t1", "amount": 49.99, "is_refund": false, "items": ["sku_1", "sku_2"], "customer": {"id": 100, "loyalty_tier": "gold"}}
```

1. Write an explicit `StructType` schema for this data, with correct types
   for every field, including the nested `customer` struct and the `items`
   array.
2. Read the file using `spark.read.schema(...).json(...)` with your schema.
3. Select `txn_id`, `amount`, and `customer.loyalty_tier` into a flat
   DataFrame (no nested columns in the output).
4. Explain, in one or two sentences, why defining this schema explicitly is
   safer here than relying on inference — specifically for the `amount`
   field.

Answer for (4): a `false`/numeric mix of transaction amounts, or an
occasional integer-valued amount like `50` (no decimal in that particular
row), could make schema inference pick `LongType` if the sample it inspects
happens not to include a fractional value, silently truncating decimals for
every other row — an explicit `DoubleType` guarantees the correct type
regardless of what the first few sampled rows look like.
