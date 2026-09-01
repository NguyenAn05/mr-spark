# Task 1-2 — Monthly State-Level Variety Median with Secondary Sort

## 1. Problem statement

For each month and each state, calculate the **median** of "Variety", but only for "Styles" that have been sold in size "XXL" in the same time interval and region. 
- "Variety" is defined as the number of distinct SKUs a Style has.
- The output format must be: `month,state,median_variety`.
- The result must be exported to a local CSV file, sorted by month and state.
- For an even number of values, the median is the average of the two middle values.

## 2. Approach interpretation

### 2.1. Ambiguity: Local vs Global XXL condition
The task description requires finding the median variety "only for Styles that have been sold in size 'XXL' in the same time interval and region." There is an inherent ambiguity in this statement:
- **Local condition (Chosen):** The style must have at least one XXL size sold *within the specific state and month being analyzed*.
- **Global condition:** The style must have at least one XXL size sold *anywhere in the dataset* (or across all states/months) to be considered valid for all groupings.

This implementation adopts the **Local condition** because the phrase "in the same time interval and region" strongly implies that the XXL sale must occur concurrently and geographically with the records being aggregated. The Local approach strictly bounds the filtering criteria to the MapReduce key being evaluated. If the Global condition were used instead, it would yield different results (as suggested by the lecture slides, up to a 31% difference in group counts).

### 2.2. Prioritization Framework for Ambiguities
Given the ambiguous nature of the dataset and task descriptions, the team adopted a strict two-tier priority framework to resolve conflicts:
1. **Primary Rule - Literal Wording:** Decisions are made by strictly adhering to the literal, explicit text of the problem statement. Implicit filters from previous tasks are ignored.
2. **Secondary Rule - Reference Checkpoints:** When the literal wording is silent on a data cleaning issue, the implementation defaults to maintaining strict consistency with provided reference checkpoints (such as expected row counts or specific slide warnings) rather than introducing unrequested transformations.

To find the median variety, the pipeline needs to first count the distinct SKUs (variety) for each `(month, state, style)` group, filter based on the presence of size "XXL", and then collect all varieties for a `(month, state)` to find the median.

Instead of using memory-intensive `HashSet` collections to count distinct SKUs in the Reducer, this solution uses **Hadoop Secondary Sort**. By sorting the composite keys up to the `sku` level, the Reducer receives records for a style ordered by SKU. It can then count distinct SKUs simply by detecting when the `sku` value changes between consecutive records, requiring `O(1)` memory.

The pipeline is split into two MapReduce jobs:
1. **Job 1 (Variety Count):** Computes the variety (distinct SKUs) for each style and filters out styles that do not have an "XXL" size sold.
2. **Job 2 (Median Variety):** Collects the variety counts for each `(month, state)`, sorts them, and computes the median.

Dates are formatted to `YYYY-MM` to represent the month.

## 3. Data and preprocessing

The following input columns are used:

| Column | Zero-based index | Purpose |
|---|---:|---|
| `Date` | 2 | Parsed to extract the month (`YYYY-MM`) |
| `Style` | 7 | Used as part of the grouping key to count variety |
| `SKU` | 8 | Used in secondary sort to count distinct SKUs |
| `Size` | 10 | Checked to filter styles containing "XXL" |
| `ship-state` | 17 | State used as the grouping key |

The same robust CSV parser from Task 1-1 is used to handle quoted fields with embedded commas. Invalid rows (missing values, malformed dates) are skipped. Sizes are mapped to a numeric rank (e.g., `S = 2`, `XXL = 6`) rather than compared as strings. This explicitly avoids the notorious "alphabet-sort trap" (where `"M" > "L" > "XXL"` alphabetically), ensuring that size threshold logic operates correctly.

**Normalization of States:**
As per the task constraints and common pitfalls, state names are normalized simply by applying `UPPER(TRIM())` to avoid trivial duplicates (e.g. `Delhi`, `DELHI`, `delhi` becoming separate keys). 

*Known Limitation (Semantic Aliases):* The team noticed that several state values in the dataset are actually varying representations of the same physical region (for example: `PB` and `PUNJAB`, `ORISSA` and `ODISHA`, `NEW DELHI` and `DELHI`, `PONDICHERRY` and `PUDUCHERRY`). Since the prompt does not explicitly demand deep semantic normalization (Primary Rule), and to maintain strict consistency with the reference checkpoint which expects exactly 128 output rows for the local condition (Secondary Rule), the team deliberately restricted the normalization to `UPPER(TRIM())` as highlighted in slide pitfall #2, bypassing alias-mapping. This is acknowledged as a known limitation of the spatial aggregation.

**Decision on "Bought" Status:**
Unlike Task 1-1, which explicitly defined a "bought order" as having `Status` containing "shipped" and `Qty > 0`, the problem statement for Task 1-2 makes no such distinction. Therefore, this implementation processes **all** valid records to count variety, regardless of their order status or quantity. This is a deliberate design choice to strictly adhere to the literal wording of Task 1-2 (Primary Rule), ensuring no implicit filters from previous tasks skew the data.

## 4. MapReduce pipeline

The pipeline consists of two consecutive MapReduce jobs:

```text
Amazon Sale Report.csv
        |
        v
Job 1: Secondary Sort to count distinct SKUs per Style and filter XXL
        |
        v
(month, state) -> varietyCount
        |
        v
Job 2: Aggregate variety counts and compute median
        |
        v
Task_1-2.csv on the local filesystem
```

### 4.1. Job 1 — Variety Count (Secondary Sort)

The Mapper parses each row and extracts the required fields. For every valid record, it emits:

```text
key   = (month, state, style, sku)
value = sizeRank
```

To enable Secondary Sort without `HashSet`, the job is configured with custom comparators:
- **Partitioner:** Routes records to Reducers based on the hash of `(month, state, style)`. This ensures all SKUs of a style end up in the same Reducer.
- **Grouping Comparator:** Groups records by `(month, state, style)`.
- **Sort Comparator:** Sorts records by `(month, state, style, sku)`.

In the Reducer, the `sku` field of the key object mutates as the iterator advances. The Reducer simply counts how many times the `sku` string changes. Simultaneously, it tracks if the maximum `sizeRank` in the group meets the requirement (XXL).

If the style contains XXL, the Reducer emits the intermediate result:
```text
key   = (month, state)
value = distinctSkuCount
```
This intermediate output is saved as an efficient Hadoop `SequenceFile` to be consumed by Job 2.

### 4.2. Job 2 — Median Variety

Job 2 reads the SequenceFiles. The Mapper acts as an identity mapper, passing the records directly to the Reducer.

*Implementation Note on `MedianKey`:* When Hadoop partitions keys for the Reducer, the default `HashPartitioner` uses the `hashCode()` of the key object. It is critical that `hashCode()` and `equals()` are explicitly overridden in `MedianKey` (based on `month` and `state`). Without this override, the partitioner falls back to the memory address (`Object.hashCode()`), which would cause records with the exact same `(month, state)` to be erroneously distributed across multiple Reducers when running on a cluster with `numReduceTasks > 1`. This critical bug is fully resolved in the `Task12Model` definitions, ensuring correctness regardless of cluster size.

```text
key   = (month, state)
value = varietyCount
```

The Reducer receives all variety counts for a specific `(month, state)`. Because the number of styles within a single state and month is relatively small compared to the raw dataset, the Reducer collects the variety counts into an in-memory `ArrayBuffer`.

Once all values are collected, the array is sorted. The median is computed as:
- **Odd size:** The middle element.
- **Even size:** The arithmetic mean of the two middle elements.

The Reducer writes the final string to output:
```text
key   = "month,state,median"
value = NullWritable
```

## 5. Output schema

The Driver merges the parts and writes a single file named `Task_1-2.csv` with the following schema:

```csv
month,state,median_variety
```

Example output:
```csv
2022-04,KARNATAKA,3.5
2022-04,MAHARASHTRA,4.0
2022-05,ASSAM,1.5
```

The Driver adds the header exactly once, safely creates the local output file, and exports the data using UTF-8 encoding.

## 6. Time-complexity analysis

Let:
- `N` be the total number of input records.
- `G` be the total number of distinct `(month, state, style)` groups.
- `G'` be the subset of groups that satisfy the XXL condition (`G' <= G`).
- `S` be the maximum number of distinct styles in any `(month, state)`.

**Job 1:**
The Mapper processes each record once: `O(N)`.
The framework sorts the records.
The Reducer scans through the sorted values in `O(1)` memory per group. The total reduce time is `O(N)`.

**Job 2:**
The Mapper processes `G'` records (only the filtered groups): `O(G')`.
The Reducer collects and sorts `S` values for each `(month, state)`. Sorting takes `O(S log S)`.
Overall Reduce time is bounded by `O(G' log S)`.

Overall pipeline complexity: `O(N + G' log S)`. Since `S` is very small relative to `N`, the process is highly efficient and scales well across distributed nodes.

## 7. Experimental results

The pipeline was executed locally via SBT (`Compile / run`). 

The final output file `outputs/Task_1-2.csv` contains exactly **129 lines** (1 header + 128 data records), matching the problem's strict output constraints perfectly.

**Verification of Smoke Tests:**
- *128 rows*: Output precisely meets the expected 128 groups for the "Local condition" interpretation.
- *March (2022-03)*: All 16 states for this month correctly display a median of `1.0`.
- *MAHARASHTRA 04/2022*: The output contains the exact expected value: `2022-04,MAHARASHTRA,4.0`.
- *Even-sized sets*: Values like `2022-04,KARNATAKA,3.5` and `2022-05,ASSAM,1.5` correctly demonstrate the average of two middle elements (yielding `.5`).

## 8. Implementation structure

| File | Responsibility |
|---|---|
| `Task12Config.scala` | Defines column indexes, size rank mappings, and XXL threshold |
| `Task12Model.scala` | Defines `VarietyKey`, `MedianKey`, and the Secondary Sort comparators (`VarietyPartitioner`, `VarietyGroupComparator`, `VarietySortComparator`) |
| `Task12RecordParser.scala` | Parses records and extracts required fields |
| `VarietyCountJob.scala` | Job 1 — Counts distinct SKUs per style using Secondary Sort and filters XXL |
| `MedianVarietyJob.scala` | Job 2 — Aggregates varieties per state/month and calculates the median |
| `Task12Driver.scala` | Coordinates Job 1 & 2, manages intermediate local paths, and exports the CSV |

## 9. Conclusion

The problem of calculating medians for conditionally filtered groups is solved efficiently using a two-stage MapReduce pipeline. The heavy-lifting of counting distinct items (Variety) is offloaded to the Hadoop framework using **Secondary Sort**, which completely eliminates the need for expensive in-memory `HashSet` data structures. Job 2 then elegantly collects the minimized data to compute the medians. The output format and tie-breaking math fully satisfy all requirements and perfectly align with the expected validation values.
