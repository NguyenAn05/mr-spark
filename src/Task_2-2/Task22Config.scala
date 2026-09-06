package task22

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Task22Config {
  val RowIdColumn = "index"
  val DateColumn = "Date"
  val SkuColumn = "SKU"
  val AmountColumn = "Amount"
  val PromotionIdsColumn = "promotion-ids"

  val InputDatePattern = "MM-dd-yy"
  val ApproximatePercentileAccuracy = 10000
  val MinimumBenchmarkRuns = 5

  val InputSchema: StructType = StructType(
    Seq(
      "index", "Order ID", "Date", "Status", "Fulfilment", "Sales Channel ",
      "ship-service-level", "Style", "SKU", "Category", "Size", "ASIN",
      "Courier Status", "Qty", "currency", "Amount", "ship-city", "ship-state",
      "ship-postal-code", "ship-country", "promotion-ids", "B2B", "fulfilled-by",
      "Unnamed: 22"
    ).map(name => StructField(name, StringType, nullable = true))
  )
}
