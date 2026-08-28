package task21

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object Task21Query {
  import Task21Config._

  private def normalized(column: Column): Column = lower(trim(column))

  def build(raw: DataFrame): DataFrame = {
    val promotionArray = array_distinct(filter(transform(split(coalesce(col(PromotionIdsColumn), lit("")), ","), promotion => trim(promotion)), promotion => length(promotion) > 0))

    val orders = raw
      .select(
        trim(col(RowIdColumn)).cast("long").as("row_id"),
        trim(col(OrderIdColumn)).as("order_id"),
        to_date(trim(col(DateColumn)), InputDatePattern).as("order_date"),
        trim(col(StatusColumn)).as("status"),
        trim(col(FulfilmentColumn)).as("fulfilment"),
        trim(col(ServiceLevelColumn)).as("service_level"),
        trim(col(CourierStatusColumn)).as("courier_status"),
        trim(col(AmountColumn)).cast("double").as("amount"),
        trim(col(CityColumn)).as("city"),
        trim(col(StateColumn)).as("state"),
        promotionArray.as("promotion_ids")
      )
      .filter(col("row_id").isNotNull)

    val validPromotions = orders
      .filter(col("order_date").isNotNull)
      .select(col("order_date"), explode(col("promotion_ids")).as("promotion_id"))
      .groupBy("promotion_id")
      .agg(min(col("order_date")).as("first_appearance_date"), max(col("order_date")).as("last_appearance_date"))
      .filter(datediff(col("last_appearance_date"), col("first_appearance_date")) >= MinimumPromotionDurationDays)
      .select("promotion_id")

    val validPromotionCounts = orders
      .select(col("row_id"), explode(col("promotion_ids")).as("promotion_id"))
      .join(validPromotions, Seq("promotion_id"), "inner")
      .groupBy("row_id")
      .agg(countDistinct(col("promotion_id")).as("valid_promotion_count"))

    val stateMerchantShippedAverages = orders
      .filter(normalized(col("fulfilment")) === "merchant" && normalized(col("courier_status")) === "shipped" && col("state").isNotNull && length(col("state")) > 0)
      .groupBy("state")
      .agg(avg(col("amount")).as("state_merchant_shipped_avg_amount"))

    val cancelledStandardOrders = orders
      .filter(normalized(col("status")) === "cancelled" && normalized(col("service_level")) === "standard" && col("city").isNotNull && length(col("city")) > 0)
      .join(validPromotionCounts, Seq("row_id"), "left")
      .withColumn("valid_promotion_count", coalesce(col("valid_promotion_count"), lit(0L)))
      .join(stateMerchantShippedAverages, Seq("state"), "left")

    val qualifies =
      col("valid_promotion_count") >= MinimumValidPromotions && col("amount").isNotNull && col("state_merchant_shipped_avg_amount").isNotNull && col("amount") < col("state_merchant_shipped_avg_amount")

    cancelledStandardOrders
      .groupBy("city")
      .agg(count(lit(1)).as("cancelled_standard_order_count"), sum(when(qualifies, lit(1L)).otherwise(lit(0L))).as("qualifying_order_count"))
      .withColumn("qualifying_order_percentage", col("qualifying_order_count").cast("double") * lit(100.0) / col("cancelled_standard_order_count").cast("double"))
      .select(col("city"), col("cancelled_standard_order_count"), col("qualifying_order_count"), col("qualifying_order_percentage"))
      .orderBy(col("city").asc)
  }
}
