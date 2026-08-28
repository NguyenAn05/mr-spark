# Task 1-1 — Dynamic-Length Sliding Windows with Hadoop MapReduce

## 1. Problem statement

For each state and each window date `d`, the task is to determine the product size that was purchased most frequently during the days preceding `d`.

The window length depends on the total number of bought orders associated with each state across the entire dataset:

- If a state has more than 10,000 bought orders, use a 5-day window from `d - 5` through `d - 1`.
- Otherwise, use a 10-day window from `d - 10` through `d - 1`.

A record is considered a bought order only when both conditions hold:

1. The `Status` column contains the word `shipped`, case-insensitively.
2. The value in the `Qty` column is not zero.

If multiple sizes have the same highest purchase count in a window, the following tie-breaking rules are applied in order:

1. Select the size whose `Amount` values have the smaller population variance.
2. If the variances are still equal, select the lexicographically smaller size.

The final result is exported as one CSV file on the local filesystem rather than as a Hadoop output directory.

## 2. Sliding-window interpretation

The date `d` represents the end date of a window and does not have to be present in the input dataset. Orders placed on `d` itself are excluded because the right endpoint of the window is `d - 1`.

Instead of visiting every date `d` and repeatedly scanning its preceding `w` days, the solution works in the opposite direction. A bought order placed on date `p` is mapped to every future window to which it contributes.

For a state with `w = 5`, an order on date `p` contributes to these buckets:

```text
p + 1, p + 2, p + 3, p + 4, p + 5
```

For example, an order dated `2022-04-30` contributes to the following `window_date` values:

```text
2022-05-01
2022-05-02
2022-05-03
2022-05-04
2022-05-05
```

Similarly, when `w = 10`, the record is emitted to ten buckets from `p + 1` through `p + 10`. This bucket-generation method also explains why the result may contain dates that do not appear directly in the input, as required by the task description.

Input dates use the `MM-dd-yy` format. They are parsed into `LocalDate` values so that date arithmetic remains correct across month boundaries, year boundaries, and leap years. Dates used as keys or written to output are formatted as ISO `yyyy-MM-dd`, which is unambiguous and chronologically sortable.

## 3. Data and preprocessing

The following input columns are used:

| Column | Zero-based index | Purpose |
|---|---:|---|
| `Date` | 2 | Purchase date of the record |
| `Status` | 3 | Check whether the value contains `shipped` |
| `Size` | 10 | Product size to aggregate |
| `Qty` | 13 | Check whether `Qty != 0` |
| `Amount` | 15 | Calculate population variance |
| `ship-state` | 17 | State used as the grouping key |

The CSV dataset contains quoted fields with embedded commas, particularly in `promotion-ids`. Therefore, the implementation uses an `inQuotes` state-based CSV parser instead of `String.split(",")`. The parser also handles escaped quotation marks represented by two consecutive double quotes.

A record is skipped if it is the header, is empty, has too few columns, contains
an invalid `Date` or `Qty`, or has a missing state or size. A missing or invalid
`Amount` does **not** make an otherwise valid bought order disappear: the order
still contributes to purchase frequency, while its Amount is excluded from the
variance statistics.

State names are preserved as they appear in the dataset after trimming leading and trailing whitespace. The assignment does not specify state-name canonicalization, so values such as `BIHAR`, `Bihar`, and `bihar` are treated as separate keys. This is a data-quality limitation that should be considered when interpreting the results.

## 4. MapReduce pipeline

The pipeline consists of three consecutive MapReduce jobs:

```text
Amazon Sale Report.csv
        |
        v
Job 1: Count bought orders by state
        |
        v
state -> totalBought -> windowLength
        |
        v
Job 2: Map orders to buckets and aggregate by state/date/size
        |
        v
Job 3: Select the winning size for each state/date
        |
        v
Task_1-1.csv on the local filesystem
```

### 4.1. Job 1 — Counting bought orders by state

The Mapper parses each input row. For every valid bought order, it emits:

```text
key   = state
value = 1
```

For example:

```text
(KARNATAKA, 1)
(KARNATAKA, 1)
(MAHARASHTRA, 1)
```

The Combiner and Reducer both calculate sums. Addition is associative and commutative, so the same implementation can safely be used for both stages.

Job 1 produces tab-separated output in the following form:

```text
state<TAB>totalBoughtOrders
```

Job 2 converts `totalBoughtOrders` into a window length using this rule:

```text
totalBoughtOrders > 10000  => 5
totalBoughtOrders <= 10000 => 10
```

The actual result shows that only two raw state keys use a 5-day window:

```text
KARNATAKA   14950
MAHARASHTRA 19103
```

All remaining raw state keys use a 10-day window.

### 4.2. Job 2 — Mapping to buckets and aggregating

The `part-*` files produced by Job 1 are provided to Job 2 through the Hadoop Distributed Cache. During `setup`, each Mapper reads the state counts and creates a local mapping:

```text
state -> windowLength
```

For every bought order `(state, p, size, optionalAmount)`, the Mapper obtains the corresponding `w` and emits `w` records:

```text
for offset = 1 to w:
    window_date = p + offset
    key = (state, window_date, size, window_length)
    if amount is valid:
        value = (purchaseCount=1, amountCount=1, amount, amount^2)
    otherwise:
        value = (purchaseCount=1, amountCount=0, 0, 0)
```

The complete key ensures that all orders belonging to the same state, window date, and size are assigned to the same Reducer group.

Instead of transferring the complete list of `Amount` values, each intermediate value stores four additive sufficient statistics:

```text
purchaseCount
amountCount
sumAmount
sumAmountSquared
```

The Combiner aggregates summaries locally before the shuffle. The Reducer combines them again and calculates the population variance:

```text
mean = sumAmount / amountCount

populationVariance = sumAmountSquared / amountCount - mean^2
```

The denominator is `N = amountCount`, not `N - 1`, because the assignment
requires population variance. `purchaseCount` remains the frequency used to
select the winning size, including bought orders whose Amount is missing. If
`amountCount` is zero, variance is undefined and is represented by an empty
field. Since floating-point error may produce a very small negative value when
the theoretical variance is zero, the implementation clamps a defined result
to a minimum of `0.0`.

Job 2 produces an intermediate tab-separated result with nine columns:

```text
state
window_date
size
window_length
purchase_count
amount_count
sum_amount
sum_amount_squared
population_variance
```

### 4.3. Job 3 — Selecting the winning size

The Job 3 Mapper reads Job 2 output and emits:

```text
key   = (state, window_date)
value = (size, window_length, purchase_count, population_variance)
```

For every `(state, window_date)` group, the Reducer retains the best candidate according to the following priority order:

```text
1. Greater purchase count
2. If counts are equal: a defined variance ranks before an undefined variance
3. If both variances are defined: smaller population variance
4. If variances are equal, or both are undefined: lexicographically smaller size
```

Lexicographic comparison is performed with `String.compareTo`. Therefore, the rule is deterministic and does not depend on the order in which values reach the Reducer.

Job 3 uses one Reducer because the final output is small and must be combined into a single CSV file. The Driver still merges the `part-*` files so that the export process does not depend on Hadoop's output-file organization.

## 5. Output schema

The final file is named `Task_1-1.csv` and has the following schema:

```csv
state,window_date,winning_size,purchase_count,population_variance,window_length
```

Example:

```csv
ANDAMAN & NICOBAR,2022-04-02,M,1,0.0,10
ANDAMAN & NICOBAR,2022-04-03,S,2,5700.25,10
ANDAMAN & NICOBAR,2022-04-04,S,3,76126.8888888889,10
```

The columns have the following meanings:

| Column | Meaning |
|---|---|
| `state` | Raw state key read from the dataset |
| `window_date` | Date `d` representing the window |
| `winning_size` | Winning size after applying the tie-breaking rules |
| `purchase_count` | Number of bought orders for the winning size in the window |
| `population_variance` | Population variance of the valid `Amount` values for the winning size; empty if no valid Amount exists |
| `window_length` | Window length for the state, either 5 or 10 |

The Driver adds the header exactly once and writes the file to the local filesystem using UTF-8. It uses `CREATE_NEW`, so an existing result file is not silently overwritten.

## 6. Time-complexity analysis

Let:

- `N` be the total number of input records.
- `B` be the number of records that satisfy the bought-order conditions.
- `w_s` be the window length for the state containing a record, either 5 or 10.
- `W = max(w_s) = 10`.
- `K` be the number of distinct `(state, window_date, size)` keys after aggregation.
- `G` be the number of distinct `(state, window_date)` groups.

Job 1 reads each record exactly once:

```text
O(N)
```

Job 2 emits each bought record to `w_s` buckets:

```text
O(sum of w_s over all bought records)
```

In the worst case:

```text
O(B * W) = O(10B) = O(B)
```

In Big-O notation, `W` is bounded by the constant 10 in this assignment. Nevertheless, the factor is significant in practice because the number of Mapper outputs can be five or ten times the number of bought records.

The map-to-buckets approach avoids rescanning all previous orders for every output date. Each record is emitted only to the windows to which it contributes. The additive summaries also avoid retaining or sorting complete lists of `Amount` values in the Reducer.

Job 3 reads `K` size candidates and selects one winner per group:

```text
O(K)
```

The overall time complexity is therefore:

```text
O(N + B*W + K)
```

Because `W <= 10`, the running time grows linearly with the input size and the number of aggregate keys.

## 7. Shuffle-complexity analysis

Without a Combiner, Job 1 could shuffle one value per bought order, producing `O(B)` shuffled values. The Combiner aggregates counts by state within each Mapper and reduces the shuffle volume to the number of local state keys.

The raw Mapper output of Job 2 is:

```text
O(B * W)
```

Each value has a fixed size consisting of two `Long` and two `Double` values.
The Combiner aggregates by `(state, window_date, size, window_length)`, so the
actual shuffle volume is closer to the number of distinct local keys than to
the number of raw bucket emissions.

Job 3 shuffles one candidate for every output group from Job 2:

```text
O(K)
```

Job 3 does not use a Combiner because its operation selects a winner from candidates that have already been aggregated. In the current pipeline, each `(state, window_date, size)` produces only one candidate after Job 2.

## 8. Experimental results

The corrected pipeline was executed successfully using Hadoop 3.5.0, YARN in
pseudo-distributed mode, Java 17, and Scala 2.12.18. All three MapReduce jobs
finished with zero failed shuffles.

### 8.1. Job 1 — State counts

| Counter | Value |
|---|---:|
| Map input records | 128,976 |
| Map output records | 109,566 |
| Combine input records | 109,566 |
| Combine output records | 67 |
| Reduce input groups | 67 |
| Reduce output records | 67 |
| Reduce shuffle bytes | 1,317 |
| `BOUGHT_ORDERS_WITHOUT_AMOUNT` | 115 |
| Failed shuffles | 0 |

The input count includes the CSV header. Job 1 emitted 109,566 usable bought
orders, including 115 orders whose Amount was missing. The Combiner reduced
109,566 Mapper outputs to 67 state-level records before the Reduce stage.

The corrected bought-order count for the raw state key `Gujarat` is:

```text
Gujarat    3858
```

The two states above the 10,000-order threshold remain unchanged:

```text
KARNATAKA   14950
MAHARASHTRA 19103
```

### 8.2. Job 2 — Bucket aggregation

| Counter | Value |
|---|---:|
| Map input records | 128,976 |
| Map output records | 925,395 |
| Combine input records | 925,395 |
| Combine output records | 29,267 |
| Reduce input groups | 29,267 |
| Reduce output records | 29,267 |
| Reduce shuffle bytes | 1,789,403 |
| `BOUGHT_ORDERS_WITHOUT_AMOUNT` | 115 |
| `BUCKETS_EMITTED` | 925,395 |
| Failed shuffles | 0 |

The bucket total can be verified independently:

```text
Orders in states where w=5:
(14950 + 19103) * 5 = 170265

Orders in states where w=10:
(109566 - 14950 - 19103) * 10 = 755130

Total bucket emissions:
170265 + 755130 = 925395
```

This exactly matches both `Map output records` and `BUCKETS_EMITTED`. The
Combiner reduced 925,395 Mapper outputs to 29,267 aggregate candidates before
the Reduce stage.

### 8.3. Job 3 — Winner selection

| Counter | Value |
|---|---:|
| Map input records | 29,267 |
| Map output records | 29,267 |
| Reduce input groups | 4,584 |
| Reduce input records | 29,267 |
| Reduce output records | 4,584 |
| `WINNERS_SELECTED` | 4,584 |
| Reduce shuffle bytes | 1,330,686 |
| Failed shuffles | 0 |

Each `(state, window_date)` group produced exactly one result: the number of
Reduce input groups, Reduce output records, and `WINNERS_SELECTED` are all
4,584.

### 8.4. Final output and correction verification

The exported local file contains:

```text
4,585 lines = 1 CSV header + 4,584 result rows
```

Retaining bought orders with missing Amount changed the winning size in the
three Gujarat windows identified during independent validation:

| Window date | Previous winner | Corrected winner | Corrected purchase count | Corrected population variance |
|---|---|---|---:|---:|
| `2022-05-26` | `3XL` | `L` | 67 | 60236.51360946754 |
| `2022-05-30` | `3XL` | `XL` | 60 | 79962.60427295923 |
| `2022-05-31` | `XXL` | `XL` | 59 | 74619.93057851237 |

These results confirm that missing Amount values no longer remove otherwise
valid bought orders from purchase-frequency calculations, while variance is
still calculated only from valid Amount observations.

## 9. Tie-breaking verification

The winner-selection logic is implemented as a sequential comparator. It first checks:

```text
candidate.purchaseCount > current.purchaseCount
```

If the counts are equal, a candidate with a defined variance is preferred over
one whose variance is undefined. If both variances are defined, it checks:

```text
candidate.variance < current.variance
```

If the variances are equal, or both are undefined, it checks:

```text
candidate.size.compareTo(current.size) < 0
```

The purchase-count, variance, and lexicographic rules follow the assignment.
The defined-before-undefined rule makes the otherwise unspecified missing-Amount
case explicit and deterministic. Selection depends only on candidate values and
not on the order in which Hadoop supplies values to the Reducer. Consequently,
the same input always produces the same winner.

## 10. Implementation structure

| File | Responsibility |
|---|---|
| `CsvParser.scala` | Parses input CSV records and escapes output CSV fields |
| `Task11Config.scala` | Defines column indexes, date formatters, threshold, and window lengths |
| `Task11Model.scala` | Defines the bought-order, summary, and candidate models |
| `Task11RecordParser.scala` | Parses records and filters bought orders |
| `AmountSummaryWritable.scala` | Stores purchase count, valid-Amount count, sum, and sum of squares as a Hadoop Writable |
| `StateCountJob.scala` | Job 1 — counts bought orders by state |
| `BucketAggregateJob.scala` | Job 2 — maps orders to buckets and aggregates them |
| `WinnerSelectionJob.scala` | Job 3 — applies tie-breaking rules and selects winners |
| `Task11Driver.scala` | Coordinates the pipeline and exports the local CSV file |

## 11. Conclusion

The solution follows the required map-to-buckets design, uses Combiners to
reduce shuffle traffic, and calculates population variance from the sufficient
statistics `amountCount`, `sum`, and `sum of squares` without storing all
`Amount` values in memory. Purchase frequency is maintained separately, so an
otherwise valid bought order is no longer discarded merely because its Amount
is missing.

The corrected pipeline ran successfully on YARN. It retained 115 bought orders
with missing Amount for purchase-frequency calculations, emitted 925,395
sliding-window buckets, selected 4,584 winners, and exported one valid CSV file
with exactly one header. The observed counters and the corrected Gujarat
windows agree with the independent validation.
