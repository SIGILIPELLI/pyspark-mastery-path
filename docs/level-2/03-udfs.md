# 03 · Udfs

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

A User-Defined Function (UDF) lets you run arbitrary Python code per row
when the built-in `pyspark.sql.functions` library doesn't have what you
need. UDFs are a last resort, not a first choice — this module covers how
to write them correctly, and why you should reach for something else
whenever possible.

```python
data = [
    (1, "  Alice Smith  "),
    (2, "bob jones"),
    (3, "CARLA garcia"),
    (4, None),
]
df = spark.createDataFrame(data, ["id", "raw_name"])
```

## Why UDFs are the last resort

Every built-in function in `pyspark.sql.functions` (`upper`, `trim`,
`regexp_replace`, `when`, ...) runs inside the JVM, in Spark's native
execution engine — vectorized, no serialization overhead, and visible to
Catalyst's optimizer for further rewriting. A plain Python UDF has none of
these advantages: for every row, Spark must serialize the row from the
JVM, send it to a spawned Python process, run your Python function, and
serialize the result back. This round-trip is often 10-100x slower than an
equivalent built-in expression, and it's completely opaque to Catalyst —
the optimizer cannot push filters through it or reorder around it.

Before writing a UDF, check whether the built-ins can do it:

```python
from pyspark.sql.functions import initcap, trim, col

# instead of a Python UDF that strips + title-cases:
df.withColumn("clean_name", initcap(trim(col("raw_name")))).show()

# +---+----------------+----------+
# | id|        raw_name|clean_name|
# +---+----------------+----------+
# |  1|  Alice Smith   |Alice Smith|
# |  2|       bob jones|Bob Jones |
# |  3|    CARLA garcia|Carla Garcia|
# |  4|            null|      null|
# +---+----------------+----------+
```

`initcap` + `trim` solved this with zero Python UDFs. Reach for a UDF only
when the transformation genuinely can't be expressed with built-ins — a
niche parsing library, a custom checksum, calling into `re` with logic too
complex for `regexp_extract`.

## Writing and registering a basic UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def mask_name(name):
    if name is None:
        return None
    name = name.strip()
    if len(name) <= 2:
        return "*" * len(name)
    return name[0] + "*" * (len(name) - 2) + name[-1]

mask_name_udf = udf(mask_name, StringType())

df.withColumn("masked", mask_name_udf(col("raw_name"))).show()

# +---+----------------+--------------+
# | id|        raw_name|        masked|
# +---+----------------+--------------+
# |  1|  Alice Smith   |A**********h |
# |  2|       bob jones|b*******s     |
# |  3|    CARLA garcia|C**********a  |
# |  4|            null|          null|
# +---+----------------+--------------+
```

Two things are mandatory: declaring the return type (`StringType()` here —
Spark can't infer it from Python) and handling `None` explicitly, since
Spark will call your function with `None` for null input rather than
skipping the row.

## The decorator form

```python
@udf(returnType=StringType())
def shout(name):
    return name.upper() if name else None

df.withColumn("shouted", shout(col("raw_name"))).show()
```

Functionally identical to `udf(fn, StringType())` — pick whichever reads
better; the decorator form is common when the function is only ever used
as a UDF.

## Registering a UDF for use in Spark SQL

```python
spark.udf.register("mask_name_sql", mask_name, StringType())
df.createOrReplaceTempView("people")

spark.sql("SELECT id, mask_name_sql(raw_name) AS masked FROM people").show()
```

`spark.udf.register` makes the same Python function callable from raw SQL
strings — useful when a pipeline mixes DataFrame code and `spark.sql(...)`
queries.

## Pandas UDFs: the performance-conscious alternative

When you must use Python but need better performance than a row-at-a-time
UDF, use a **Pandas UDF** (vectorized UDF). Spark still ships data to
Python, but in Arrow-formatted *batches* processed with `pandas`/`numpy`
vectorized operations, instead of one Python function call per row —
typically several times faster than a plain UDF.

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def clean_name_pandas(names: pd.Series) -> pd.Series:
    return names.str.strip().str.title()

df.withColumn("clean_name", clean_name_pandas(col("raw_name"))).show()
```

The function signature takes a `pandas.Series` and returns a
`pandas.Series` of the same length — the batching is handled by Spark;
your code just needs to be a vectorized pandas operation. Pandas UDFs
require `pyarrow` installed on both driver and executors.

## Performance pitfall: UDFs block optimizer pushdown

```python
# A filter BEFORE a UDF can be pushed down to the data source (e.g. Parquet
# predicate pushdown); a filter that depends on a UDF's output cannot be:
df.filter(mask_name_udf(col("raw_name")).startswith("A")).explain()
# Catalyst cannot push this filter into a file scan — the UDF is opaque to it.
```

If a filter can be expressed with built-ins, always filter *before*
applying a UDF, so the optimizer can push the built-in filter down as
early as possible (ideally to the file scan itself) and the UDF only runs
on the smaller, already-filtered set of rows.

## Worked example: parsing a semi-structured field

Task: `raw_name` sometimes has irregular whitespace/casing (as above);
build a proper `first_name` / `last_name` split as a genuine UDF example
(built-ins alone can't cleanly do multi-value output like this from one
column, since `split` + array indexing gets awkward with irregular
whitespace).

```python
from pyspark.sql.types import StructType, StructField

schema = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
])

@udf(returnType=schema)
def split_name(raw):
    if raw is None:
        return None
    parts = raw.strip().split()
    if len(parts) == 0:
        return (None, None)
    if len(parts) == 1:
        return (parts[0].title(), None)
    return (parts[0].title(), " ".join(parts[1:]).title())

result = df.withColumn("parsed", split_name(col("raw_name"))) \
           .select("id", "parsed.first_name", "parsed.last_name")
result.show()

# +---+----------+---------+
# | id|first_name|last_name|
# +---+----------+---------+
# |  1|     Alice|    Smith|
# |  2|       Bob|    Jones|
# |  3|     Carla|   Garcia|
# |  4|      null|     null|
# +---+----------+---------+
```

A UDF returning a `StructType` produces a single struct column, which you
then unpack with `.select("parsed.first_name", "parsed.last_name")` —
this is the idiomatic way to get "multiple outputs" from one UDF call.

## Exercise

Using `df` from the top of this module:

1. Write a plain Python UDF that returns the length of `raw_name`'s
   stripped, trimmed value (return `0` for `None`), and confirm it matches
   `length(trim(raw_name))` computed with the built-in `length` function.
2. Convert that UDF to a Pandas UDF and confirm it produces the same
   result.
3. Explain (in a comment) why `df.filter(col("raw_name").isNotNull())`
   should be applied before a UDF call rather than after, referencing
   optimizer pushdown.
4. Write a UDF returning a `StructType` with `is_valid: BooleanType` and
   `reason: StringType`, flagging rows where `raw_name` is null or empty
   after stripping, and unpack it into two top-level columns.
