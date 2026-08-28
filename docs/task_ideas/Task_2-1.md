# Task 2-1 — City-Level Cancellation Percentage with Spark Structured APIs

## 1. Problem statement

For each city, this task requires the percentage of cancelled orders with the `Standard` service level that satisfy both of the following conditions:

1. The order has at least three temporally valid promotions.
2. Its purchased `Amount` is smaller than the average amount of Merchant-fulfilled orders in the associated state whose `Courier Status` is `Shipped`.

A promotion is temporally valid when the difference between its last and first appearance dates in the entire dataset is at least two days. Amazon-issued promotions are included in exactly the same way as all other promotion identifiers.

The solution must use only Spark's DataFrame/Dataset API. Direct Spark SQL string queries are not used. The final result must be exported as one Parquet file on the normal local filesystem.

## 2. Query interpretation

### 2.1. Record granularity

The implementation treats each CSV data row as one order record. The `index` column is used as the unique row identifier when promotion counts are joined back to the original data.

Although an `Order ID` may occur on multiple rows, the dataset represents the fields required by this query, including `Status`, `Amount`, and `promotion-ids`, at row level. This interpretation is also consistent with Task 1-1, where bought orders were counted from qualifying input records.

### 2.2. Percentage denominator

The statement does not explicitly define the denominator of the requested percentage. The implementation uses the following interpretation:

```text
denominator(city)
    = number of Cancelled, Standard-service records in that city

numerator(city)
    = number of records in the denominator that:
        - have at least three temporally valid promotions, and
        - have Amount below the associated state average

percentage(city)
    = numerator(city) / denominator(city) * 100
```

In mathematical form, let `C_c` be the set of records in city `c` whose status is `Cancelled` and whose service level is `Standard`. Let `Q_c` be the subset of `C_c` satisfying the promotion and amount conditions. Then:

```text
percentage(c) = 100 * |Q_c| / |C_c|
```

This assumption is stated explicitly because alternative interpretations, such as dividing by all orders in the city, would produce a different denominator.

### 2.3. String and geographical values

Leading and trailing whitespace is removed from input values. Comparisons for status, fulfilment, courier status, and service level are case-insensitive.

City and state keys are trimmed but otherwise preserved. They are not converted to uppercase and spelling variants are not merged because the assignment does not define a geographical canonicalization rule.

## 3. Input fields and schema

All CSV fields are initially read as nullable strings using an explicit `StructType`. Only the columns that require numeric or date semantics are cast later in the query.

| Input column | Converted representation | Purpose |
|---|---|---|
| `index` | `Long` | Unique row identifier |
| `Order ID` | `String` | Retained order identifier |
| `Date` | `Date` | First and last promotion appearances |
| `Status` | `String` | Identify cancelled records |
| `Fulfilment` | `String` | Select Merchant-fulfilled records |
| `ship-service-level` | `String` | Select Standard service records |
| `Courier Status` | `String` | Select courier status `Shipped` |
| `Amount` | `Double` | Compare against the state average |
| `ship-city` | `String` | Final grouping key |
| `ship-state` | `String` | State-average grouping and join key |
| `promotion-ids` | `Array[String]` | Promotion validity and per-record counts |

Dates are parsed from `MM-dd-yy` using Spark's built-in `to_date` function.

The CSV reader handles quoted fields before the query processes `promotion-ids`. This is important because the promotion field itself contains commas. After CSV parsing, the field is split on commas, each identifier is trimmed, empty strings are removed, and `array_distinct` removes duplicate identifiers within the same row.

## 4. Query decomposition

The query is decomposed into four logical branches followed by a final aggregation:

```text
                         Input CSV
                             |
              +--------------+---------------+
              |                              |
              v                              v
    Global promotion validity       State Merchant/Shipped average
              |                              |
              v                              |
    Valid promotions per row                  |
              |                              |
              +---------------+--------------+
                              |
                              v
                Cancelled + Standard records
                              |
                              v
              Evaluate the two conditions
                              |
                              v
                 Aggregate percentage by city
                              |
                              v
                    Task_2-1.parquet
```

### 4.1. Preparing the order records

The raw DataFrame is projected into a smaller `orders` DataFrame containing only the fields required by the query. This step performs the following transformations:

- Cast `index` to `Long` as `row_id`.
- Parse `Date` into `order_date`.
- Cast `Amount` to `Double`.
- Trim textual fields.
- Convert `promotion-ids` into an array of distinct, non-empty promotion identifiers.

Rows with an invalid or missing `row_id` are removed because they cannot be safely joined after the promotion expansion.

### 4.2. Computing temporally valid promotions

The promotion arrays are expanded with `explode`, producing one row per `(order_date, promotion_id)` pair. Records with invalid dates are excluded from this branch.

The expanded records are grouped by `promotion_id`, after which Spark computes:

```text
first_appearance_date = min(order_date)
last_appearance_date  = max(order_date)
active_period_days    = datediff(last_appearance_date,
                                 first_appearance_date)
```

A promotion is retained when:

```text
active_period_days >= 2
```

This follows the assignment's definition of the active period as the number of days between the first and last appearances.

### 4.3. Counting valid promotions per record

The order promotion arrays are expanded a second time. The expanded promotion identifiers are inner-joined with the valid-promotion DataFrame. Therefore, invalid promotions are removed by the join.

The remaining rows are grouped by `row_id`, and `countDistinct(promotion_id)` calculates the number of valid promotions attached to each input record:

```text
row_id -> valid_promotion_count
```

The distinct count prevents a duplicated promotion identifier from increasing an order's count more than once.

When this result is left-joined back to the candidate records, a missing count is converted to zero.

### 4.4. Computing the state-level reference average

A separate branch selects records satisfying:

```text
Fulfilment     = Merchant
Courier Status = Shipped
```

The selected records are grouped by state, and Spark computes:

```text
state_merchant_shipped_avg_amount = avg(Amount)
```

Spark's `avg` ignores null amounts. Records with an empty state are removed from this branch because they cannot provide a valid state-level reference.

The resulting state-average DataFrame is left-joined to the Cancelled/Standard records by state.

### 4.5. Calculating the city percentage

The base records for the final calculation satisfy:

```text
Status                = Cancelled
ship-service-level    = Standard
ship-city             is non-empty
```

A base record contributes to the numerator when:

```text
valid_promotion_count >= 3
AND Amount is not null
AND state average is not null
AND Amount < state_merchant_shipped_avg_amount
```

The final `groupBy(city)` calculates:

```text
cancelled_standard_order_count = count(all base records)

qualifying_order_count = sum(
    1 when the record satisfies both conditions,
    0 otherwise
)

qualifying_order_percentage =
    qualifying_order_count * 100.0
    / cancelled_standard_order_count
```

The output is ordered by city to make repeated executions deterministic and easy to inspect.

## 5. Output schema

The final Parquet file has the following schema:

| Column | Type | Meaning |
|---|---|---|
| `city` | String | Trimmed city key from the dataset |
| `cancelled_standard_order_count` | Long | Percentage denominator |
| `qualifying_order_count` | Long | Percentage numerator |
| `qualifying_order_percentage` | Double | Numerator divided by denominator, multiplied by 100 |

Keeping the numerator and denominator in the output makes the percentage auditable rather than returning only the derived value.

## 6. Using only Structured APIs

The implementation uses DataFrame functions such as:

```text
select, filter, transform, split, array_distinct, explode,
groupBy, min, max, datediff, countDistinct, avg, join,
when, sum, withColumn, orderBy
```

No call to `spark.sql(...)` is made, and no SQL query string or SQL expression string is used. All query conditions and transformations are represented using typed `Column` expressions from Spark's Structured API.

## 7. Physical execution plan

The program calls:

```scala
result.explain(extended = true)
```

before executing the write action. The complete parsed, analyzed, optimized, and physical plans are recorded in `outputs/task21-run.log`.

The physical plan begins with:

```text
AdaptiveSparkPlan isFinalPlan=false
```

This means Adaptive Query Execution is enabled. The plan is marked non-final because `explain(true)` is called before the action. Spark can still coalesce shuffle partitions and make runtime adjustments while the query is executing.

### 7.1. Join strategies

Spark selected three `BroadcastHashJoin` operators:

| Join | Type | Build side |
|---|---|---|
| Expanded order promotions with globally valid promotions | Inner | Right |
| Cancelled/Standard records with valid-promotion counts | Left outer | Right |
| Candidate records with state averages | Left outer | Right |

The relevant physical-plan nodes are:

```text
BroadcastHashJoin [promotion_id], [promotion_id], Inner, BuildRight
BroadcastHashJoin [row_id], [row_id], LeftOuter, BuildRight
BroadcastHashJoin [state], [state], LeftOuter, BuildRight
```

Spark broadcasts the aggregated right-hand relations because they are sufficiently small. This avoids repartitioning the larger order-record branch for each join.

No `SortMergeJoin` or `BroadcastNestedLoopJoin` appears in the physical plan.

### 7.2. Exchange operators

The pre-action physical plan contains nine Exchange operators in total:

- Six shuffle exchanges:
  - Five `hashpartitioning` exchanges for aggregations.
  - One `rangepartitioning` exchange for the final global city ordering.
- Three `BroadcastExchange` operators that prepare the right-hand sides of the three broadcast joins.

It is useful to report both values separately: there are nine exchange operators, of which six are ordinary repartitioning shuffles and three create broadcast relations.

### 7.3. Spark stages

A `SparkListener` collected the completed stage identifiers during the Parquet write action:

```text
0, 1, 3, 5, 6, 8, 11, 12, 14, 16, 19
```

Therefore, the executed query and single-file write completed in:

```text
11 Spark stages
```

The stage IDs are not consecutive because Spark creates and reuses internal query stages while Adaptive Query Execution prepares broadcast relations and shuffle results.

The number of stages does not equal the number of Exchange nodes plus one. Broadcast preparation, adaptive query stages, result stages, and the final Parquet write all influence Spark's stage DAG.

## 8. Single-file Parquet export

Spark normally writes a directory containing one or more `part-*` files. However, the assignment requires a single Parquet file on the normal filesystem.

The export procedure is:

1. Apply `coalesce(1)` to produce one output partition.
2. Write the DataFrame to a uniquely named temporary local directory.
3. Locate the single `part-*.parquet` file.
4. Move that part file to the requested `Task_2-1.parquet` path.
5. Delete the temporary Spark output directory and metadata files.

The exporter refuses to overwrite an existing target file, preventing accidental replacement of a previous result.

The resulting file is a normal Apache Parquet file with Snappy compression and can be read directly with Spark or Pandas with a Parquet engine.

## 9. Experimental environment and results

The query was executed with:

| Component | Version or configuration |
|---|---|
| Spark | 3.5.9 |
| Scala | 2.12.18 |
| Java | OpenJDK 17.0.20 |
| Execution mode | `local[*]` |
| Adaptive Query Execution | Enabled |
| Input records | 128,975 data rows |
| Input file size | Approximately 69 MB |

The generated file was verified as an Apache Parquet file and successfully read back using Spark.

| Result metric | Observed value |
|---|---:|
| Output city keys | 1,639 |
| Total `cancelled_standard_order_count` | 6,906 |
| Total `qualifying_order_count` | 0 |
| Minimum percentage | 0.0 |
| Maximum percentage | 0.0 |
| Completed Spark stages | 11 |
| Output file size | Approximately 19 KB |

## 10. Explanation of the all-zero result

All output percentages are `0.0`. This is not caused by a failed join or an arithmetic error.

An independent input-data validation found:

```text
Cancelled + Standard records with a non-empty city: 6,906
Those records with at least one raw promotion ID:       0
Those records with at least three valid promotions:     0
```

Therefore, every record in the percentage denominator fails the promotion condition before the amount condition is evaluated. No record can enter the numerator, so:

```text
qualifying_order_count = 0
qualifying_order_percentage = 0.0
```

This result is a property of the provided dataset under the stated query interpretation. The numerator and denominator are retained in the final schema to make this conclusion transparent.

The same conclusion remains true if promotions are collected across duplicate rows sharing an `Order ID`: none of the relevant Cancelled/Standard records reaches three temporally valid promotions.

## 11. Implementation structure

| File | Responsibility |
|---|---|
| `Task21Config.scala` | Input schema, column names, date pattern, and thresholds |
| `Task21Query.scala` | Complete DataFrame query and all business logic |
| `SingleParquetExporter.scala` | Export to exactly one local Parquet file |
| `Task21App.scala` | Spark entry point, `explain(true)`, stage collection, and export coordination |
| `build.sbt` | Spark SQL 3.5.9 dependency and Task 2-1 source directory |

## 12. Conclusion

The solution decomposes the query into promotion-validity calculation, per-record promotion counting, state-level amount aggregation, and city-level percentage calculation. Each part is expressed entirely with Spark's DataFrame API.

Spark selected `BroadcastHashJoin` for all three joins, used six repartitioning shuffle exchanges and three broadcast exchanges, and completed the executed query and export in eleven stages. The result was successfully exported as one local Parquet file and read back with Spark.

Although every resulting percentage is zero, direct validation shows that this is expected for the supplied dataset because no Cancelled order with Standard service has an associated promotion identifier.
