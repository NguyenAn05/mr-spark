# Task 2-2 - Dynamic Promotion-Count Percentiles by SKU-Month

## 1. Problem statement

For every `(month, SKU)` group, this task calculates the population standard deviation of order amounts after retaining orders whose promotion count is at or above a dynamic percentile threshold. Two thresholds are required for every group:

- **P80** (`p = 0.8`)
- **P90** (`p = 0.9`)

The task must be implemented twice:

1. Spark's built-in `percentile_approx` aggregate.
2. A self-implemented exact percentile using only Spark DataFrame/Dataset operations.

Promotion counts include every identifier in `promotion-ids`, including Amazon-issued promotions. The final output is one local Parquet file containing results for both methods.

## 2. Query interpretation

### 2.1. Record granularity and promotion count

Each CSV row is treated as one order record and `index` is used as its unique `row_id`. The `promotion-ids` field is parsed as a comma-separated list:

```text
null or "" -> promotion_count = 0
"A,B"      -> promotion_count = 2
```

Each identifier is trimmed and empty tokens are discarded. Identifiers are not deduplicated and Amazon-issued promotions are not excluded, because the problem asks for the number of promotion identifiers associated with each order.

### 2.2. Dynamic thresholds

The percentile is calculated independently for every `(month, sku)` group rather than once over the full dataset. Therefore, the qualifying condition is:

```text
promotion_count(order) >= percentile(month, sku, p)
```

An approximate and an exact threshold can differ without changing the final qualifying set: promotion counts are integers, so two thresholds can fall between the same adjacent integer values.

### 2.3. Population standard deviation

The final aggregate is:

```scala
stddev_pop(col("amount"))
```

Spark ignores null amounts for this aggregate. When fewer than two qualifying orders remain, or when the aggregate is null, the implementation outputs `0.0` as required.

### 2.4. Exact-percentile definition

The exact method uses linear interpolation. Given a group of `N` promotion counts sorted in ascending order as `x[0], ..., x[N-1]`, for percentile `p` it calculates:

```text
h  = (N - 1) * p
lo = floor(h)
hi = ceil(h)
Pp = x[lo] + (h - lo) * (x[hi] - x[lo])
```

For example, if `h = 8.1`, the percentile is interpolated between positions 8 and 9. This definition can produce a fractional threshold, unlike the approximate result, which is a value selected from the input distribution.

## 3. Input preparation

The CSV is read with an explicit nullable-string schema. Only the fields required by this task are projected into the `orders` DataFrame.

| Input column | Converted representation | Purpose |
|---|---|---|
| `index` | `Long` (`row_id`) | Deterministic row identifier and tie-breaker |
| `Date` | `Date`, then `yyyy-MM` | Month grouping key |
| `SKU` | Trimmed `String` | SKU grouping key |
| `Amount` | `Double` | Standard-deviation input |
| `promotion-ids` | Count of non-empty tokens | Percentile input |

Rows missing `row_id`, a parsable month, or a non-empty SKU are discarded. Rows with a null `Amount` remain in the percentile branches because the threshold is defined from promotion counts, not amounts.

The normalized schema is:

```text
row_id: Long
month: String
sku: String
amount: Double
promotion_count: Long
```

## 4. DataFrame query decomposition

```text
Amazon Sale Report.csv
          |
          v
  normalized orders
          |
    +-----+-----+
    |           |
    v           v
approximate   exact Window-based
thresholds    thresholds
    |           |
    +-----+-----+
          |
          v
orders joined with each threshold set
          |
          v
filter promotion_count >= threshold
          |
          v
stddev_pop(amount) per method / P80 / P90
          |
          v
Task_2-2.parquet
```

### 4.1. Approximate branch

`orders` is grouped by `(month, sku)`. The query invokes `percentile_approx` once with the array `[0.8, 0.9]` and an accuracy of `10000`. The two returned values are expanded into separate P80 and P90 rows and labelled `method = "approx"`.

The chosen accuracy is higher than the largest observed SKU-month group size, so the approximation parameter is not constrained by a lack of summary capacity for this input. Nevertheless, this branch remains conceptually approximate because it follows Spark's approximate percentile semantics rather than the selected interpolation definition.

### 4.2. Exact branch

The exact branch uses a window partitioned by `(month, sku)` and ordered by:

```text
promotion_count ASC, row_id ASC
```

`row_number` identifies the sorted position and a partition-level `count` supplies `N`. Each row is cross-joined with a two-row DataFrame containing P80 and P90. The query derives lower and upper positions from `h`, selects the corresponding values with conditional aggregates, and applies the interpolation formula from Section 2.4.

This branch uses `row_number`, `count`, `floor`, `ceil`, `when`, `max`, `groupBy`, and `crossJoin`; it does not call any built-in exact-percentile function or SQL string.

### 4.3. Final aggregation

Both threshold tables use the same downstream logic:

1. Join thresholds to `orders` by `(month, sku)`.
2. Keep orders where `promotion_count >= promotion_threshold`.
3. Group by month, SKU, method, percentile level, percentile value, and threshold.
4. Calculate `qualifying_order_count` and `stddev_pop(amount)`.
5. Replace a standard deviation for a group with fewer than two qualifying orders by `0.0`.

The approximate and exact result DataFrames are combined with `unionByName` and sorted by month, SKU, method, and percentile level.

## 5. Structured API compliance and execution plan

The implementation uses only Spark's DataFrame API, including `select`, `filter`, `split`, `transform`, `groupBy`, `percentile_approx`, `row_number`, `count`, `crossJoin`, `join`, `stddev_pop`, `unionByName`, and `orderBy`. No call to `spark.sql(...)` or SQL expression string is used.

`Task22App` prints `result.explain(extended = true)` before the Parquet write. The recorded physical plan shows the expected hash-partition exchanges on `(month, sku)` for grouped percentile calculation and window ordering. It also shows a small broadcast nested-loop cross join for the two percentile levels and broadcast hash joins when the threshold relations are combined with order rows. The two-level percentile DataFrame is constant-size, so broadcasting it avoids an unnecessary large shuffle.

## 6. Single-file Parquet export

Spark normally writes a directory containing `part-*` files. The shared `SingleParquetExporter` produces the required single local Parquet file by:

1. Writing `coalesce(1)` output to a unique temporary directory.
2. Finding the single `part-*.parquet` file.
3. Moving that part file to the requested target path.
4. Removing the temporary directory.

The exporter refuses to overwrite an existing output path. This prevents accidental replacement of a previous result and explains why a rerun must use a new output name or remove/archive the prior output first.

## 7. Output schema

The final file is `outputs/Task_2-2.parquet`.

| Column | Type | Description |
|---|---|---|
| `month` | String | Group month in `yyyy-MM` format |
| `sku` | String | Group SKU |
| `method` | String | `approx` or `exact` |
| `percentile_level` | String | `P80` or `P90` |
| `percentile` | Double | `0.8` or `0.9` |
| `promotion_threshold` | Double | Calculated threshold for the group |
| `qualifying_order_count` | Long | Number of retained order rows |
| `amount_stddev_pop` | Double | Population standard deviation of retained amounts |

Every `(month, sku)` group has four result rows: approximate/exact x P80/P90. Retaining thresholds and qualifying counts makes the computation auditable.

## 8. Experimental results

The task was evaluated on the provided Amazon Sale Report dataset using Spark 3.5.9 in `local[*]` mode.

| Metric | Observed value |
|---|---:|
| Normalized input orders | 128,975 |
| SKU-month groups | 16,486 |
| Largest SKU-month group | 426 orders |
| Result rows | 65,944 |
| Result rows per SKU-month group | 4 |
| Null cells in exported result | 0 |
| Negative standard deviations | 0 |

The largest group contains 426 orders, below the assignment's 1,000-order threshold. Manual repartitioning was therefore not introduced: the largest group is far below Spark's typical 128 MB partition target, and an additional repartition would add an avoidable shuffle without addressing a measured skew problem.

### 8.1. Approximate versus exact comparison

The two output branches were joined by `(month, sku, percentile_level)`, producing 32,972 comparisons.

| Comparison metric | Observed value |
|---|---:|
| Thresholds that differ | 13,221 |
| Mean absolute threshold difference | 0.982970399127 |
| Maximum absolute threshold difference | 23.2 |
| Different qualifying `row_id` sets | 1,307 |
| Different population standard deviations | 528 |
| Maximum absolute standard-deviation difference | 701.5 |

By percentile level:

| Level | Thresholds different | Qualifying sets different | Standard deviations different |
|---|---:|---:|---:|
| P80 | 6,053 | 1,002 | 403 |
| P90 | 7,168 | 305 | 125 |

One representative P80 group is `(2022-05, "SET397-KR-NP  -M")`:

| Method | Threshold | Qualifying orders | Population standard deviation |
|---|---:|---:|---:|
| Approximate | 1.0 | 62 | 135.610944 |
| Exact | 2.2 | 17 | 235.058824 |

This example demonstrates that an approximate threshold can select a substantially different set when it crosses an integer promotion-count boundary.

### 8.2. Benchmark

One warm-up action was excluded before measuring five `count()` actions for each branch. The actions used the same cached normalized input and Spark configuration.

| Method | Five measured runs (ms) | Mean (ms) | Population standard deviation (ms) |
|---|---|---:|---:|
| Approximate | 2239.36, 1998.31, 1519.73, 1735.24, 1352.21 | 1768.97 | 319.63 |
| Exact | 1968.50, 1601.49, 1601.32, 1359.37, 1239.17 | 1553.97 | 250.32 |

These measurements are specific to the provided dataset and local execution environment. They should not be generalized to larger or differently distributed inputs.

### 8.3. Parquet read-back verification

The exported file was successfully read by Pandas with the PyArrow engine. It contains 65,944 rows and the eight-column schema in Section 7, with both methods and both percentile levels present. There are no duplicate `(month, sku, method, percentile_level)` keys, no null output cells, and exactly four result rows per SKU-month group.

## 9. Implementation structure

| File | Responsibility |
|---|---|
| `Task22Config.scala` | Input schema, column names, date pattern, percentile accuracy, and benchmark minimum |
| `Task22Query.scala` | Normalization, approximate branch, exact Window-based branch, and final standard-deviation calculation |
| `Task22App.scala` | Spark entry point, benchmarking, execution-plan output, group-size measurement, and export coordination |
| `SingleParquetExporter.scala` | Shared Task 2-1 helper that exports exactly one local Parquet file |
| `build.sbt` | Registers the Task 2-2 source directory and Spark SQL dependency |

## 10. Conclusion

Task 2-2 is implemented as two fully DataFrame-based pipelines over the same normalized order records. The approximate branch uses Spark's `percentile_approx`; the exact branch independently derives P80 and P90 through ordered Window operations and linear interpolation.

The result is a single auditable Parquet file with both methods, their thresholds, qualifying-order counts, and population standard deviations. The measured comparison confirms that threshold differences are common, but only a subset changes the qualifying orders or final standard deviation. The largest observed group does not justify manual repartitioning for this dataset.
