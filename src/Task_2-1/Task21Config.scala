package task21

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Task21Config {
  val RowIdColumn = "index"
  val OrderIdColumn = "Order ID"
  val DateColumn = "Date"
  val StatusColumn = "Status"
  val FulfilmentColumn = "Fulfilment"
  val ServiceLevelColumn = "ship-service-level"
  val AmountColumn = "Amount"
  val CityColumn = "ship-city"
  val StateColumn = "ship-state"
  val PromotionIdsColumn = "promotion-ids"
  val CourierStatusColumn = "Courier Status"

  val InputDatePattern = "MM-dd-yy"
  val MinimumPromotionDurationDays = 2
  val MinimumValidPromotions = 3

  val InputSchema: StructType = StructType(
    Seq(
      "index",
      "Order ID",
      "Date",
      "Status",
      "Fulfilment",
      "Sales Channel ",
      "ship-service-level",
      "Style",
      "SKU",
      "Category",
      "Size",
      "ASIN",
      "Courier Status",
      "Qty",
      "currency",
      "Amount",
      "ship-city",
      "ship-state",
      "ship-postal-code",
      "ship-country",
      "promotion-ids",
      "B2B",
      "fulfilled-by",
      "Unnamed: 22"
    ).map(name => StructField(name, StringType, nullable = true))
  )
}
