package task22

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Task22Query {
  import Task22Config._

  private val OutputColumns = Seq(
    "month", "sku", "method", "percentile_level", "percentile",
    "promotion_threshold", "qualifying_order_count", "amount_stddev_pop"
  )

  /** Keeps every non-empty promotion identifier, including Amazon-issued ones. */
  def prepareOrders(raw: DataFrame): DataFrame = {
    val promotionIds = filter(
      transform(split(coalesce(col(PromotionIdsColumn), lit("")), ","), promotionId => trim(promotionId)),
      promotionId => length(promotionId) > 0
    )

    raw
      .select(
        trim(col(RowIdColumn)).cast("long").as("row_id"),
        to_date(trim(col(DateColumn)), InputDatePattern).as("order_date"),
        trim(col(SkuColumn)).as("sku"),
        trim(col(AmountColumn)).cast("double").as("amount"),
        size(promotionIds).cast("long").as("promotion_count")
      )
      .withColumn("month", date_format(col("order_date"), "yyyy-MM"))
      .filter(
        col("row_id").isNotNull && col("month").isNotNull &&
          col("sku").isNotNull && length(col("sku")) > 0
      )
      .select("row_id", "month", "sku", "amount", "promotion_count")
  }

  /** Uses Spark's approximate percentile aggregate for P80 and P90. */
  def approximateThresholds(orders: DataFrame): DataFrame = {
    val grouped = orders.groupBy("month", "sku").agg(
      percentile_approx(
        col("promotion_count"),
        array(lit(0.8), lit(0.9)),
        lit(ApproximatePercentileAccuracy)
      ).as("thresholds")
    )

    val p80 = grouped.select(
      col("month"), col("sku"), lit("approx").as("method"),
      lit("P80").as("percentile_level"), lit(0.8).as("percentile"),
      element_at(col("thresholds"), lit(1)).cast("double").as("promotion_threshold")
    )
    val p90 = grouped.select(
      col("month"), col("sku"), lit("approx").as("method"),
      lit("P90").as("percentile_level"), lit(0.9).as("percentile"),
      element_at(col("thresholds"), lit(2)).cast("double").as("promotion_threshold")
    )
    p80.unionByName(p90)
  }

  /**
    * Exact linear interpolation implemented with DataFrame and Window operations:
    * h=(N-1)p; Pp=x[floor(h)] + fractional(h) * (x[ceil(h)]-x[floor(h)]).
    */
  def exactThresholds(orders: DataFrame, spark: SparkSession): DataFrame = {
    val rankWindow = Window.partitionBy("month", "sku").orderBy(col("promotion_count").asc, col("row_id").asc)
    val groupWindow = Window.partitionBy("month", "sku")

    val ranked = orders
      .withColumn("row_number", row_number().over(rankWindow).cast("long"))
      .withColumn("group_size", count(lit(1)).over(groupWindow).cast("long"))
      .crossJoin(percentileLevels(spark))
      .withColumn("interpolated_index", (col("group_size").cast("double") - lit(1.0)) * col("percentile"))
      .withColumn("lower_position", floor(col("interpolated_index")).cast("long") + lit(1L))
      .withColumn("upper_position", ceil(col("interpolated_index")).cast("long") + lit(1L))

    ranked
      .groupBy(
        "month", "sku", "percentile_level", "percentile",
        "interpolated_index", "lower_position", "upper_position"
      )
      .agg(
        max(when(col("row_number") === col("lower_position"), col("promotion_count").cast("double"))).as("lower_value"),
        max(when(col("row_number") === col("upper_position"), col("promotion_count").cast("double"))).as("upper_value")
      )
      .withColumn(
        "promotion_threshold",
        col("lower_value") +
          (col("interpolated_index") - floor(col("interpolated_index"))) *
            (col("upper_value") - col("lower_value"))
      )
      .select(
        col("month"), col("sku"), lit("exact").as("method"),
        col("percentile_level"), col("percentile"), col("promotion_threshold")
      )
  }

  /** Applies one method's thresholds then calculates the requested population standard deviation. */
  def resultsForThresholds(orders: DataFrame, thresholds: DataFrame): DataFrame =
    orders
      .join(thresholds, Seq("month", "sku"), "inner")
      .filter(col("promotion_count").cast("double") >= col("promotion_threshold"))
      .groupBy("month", "sku", "method", "percentile_level", "percentile", "promotion_threshold")
      .agg(
        count(lit(1)).as("qualifying_order_count"),
        stddev_pop(col("amount")).as("computed_amount_stddev_pop")
      )
      .withColumn(
        "amount_stddev_pop",
        when(col("qualifying_order_count") < lit(2L), lit(0.0))
          .otherwise(coalesce(col("computed_amount_stddev_pop"), lit(0.0)))
      )
      .select(OutputColumns.map(col): _*)

  def approximateResults(orders: DataFrame): DataFrame =
    resultsForThresholds(orders, approximateThresholds(orders))

  def exactResults(orders: DataFrame, spark: SparkSession): DataFrame =
    resultsForThresholds(orders, exactThresholds(orders, spark))

  def build(orders: DataFrame, spark: SparkSession): DataFrame =
    approximateResults(orders)
      .unionByName(exactResults(orders, spark))
      .orderBy(col("month").asc, col("sku").asc, col("method").asc, col("percentile_level").asc)

  private def percentileLevels(spark: SparkSession): DataFrame =
    spark.range(1L)
      .select(
        explode(
          array(
            struct(lit("P80").as("percentile_level"), lit(0.8).as("percentile")),
            struct(lit("P90").as("percentile_level"), lit(0.9).as("percentile"))
          )
        ).as("level")
      )
      .select(col("level.percentile_level"), col("level.percentile"))
}
