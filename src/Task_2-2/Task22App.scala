package task22

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import task21.SingleParquetExporter

object Task22App {
  private final case class BenchmarkSummary(label: String, measurementsMillis: Vector[Double]) {
    private val meanMillis = measurementsMillis.sum / measurementsMillis.size
    private val standardDeviationMillis = math.sqrt(
      measurementsMillis.map(value => math.pow(value - meanMillis, 2)).sum / measurementsMillis.size
    )

    def print(): Unit = {
      println(label + " benchmark (ms): " + measurementsMillis.map(value => f"$value%.2f").mkString(", "))
      println(f"$label mean (ms): $meanMillis%.2f")
      println(f"$label population standard deviation (ms): $standardDeviationMillis%.2f")
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2 || args.length > 3) {
      System.err.println("Usage: Task22App <input-csv> <local-output-parquet> [benchmark-runs>=5]")
      System.exit(1)
    }

    val benchmarkRuns = parseBenchmarkRuns(args.lift(2))
    val spark = SparkSession.builder().appName("Lab03 Task 2-2").getOrCreate()
    var orders: DataFrame = null

    try {
      val raw = spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(Task22Config.InputSchema)
        .csv(args(0))

      orders = Task22Query.prepareOrders(raw).persist(StorageLevel.MEMORY_AND_DISK)
      println("Prepared orders: " + orders.count())

      val largestGroupSize = orders.groupBy("month", "sku").count()
        .agg(org.apache.spark.sql.functions.max("count"))
        .head()
        .getLong(0)
      println("Largest SKU-month group size: " + largestGroupSize)
      println("Manual repartition discussion required (>1000 orders): " + (largestGroupSize > 1000L))

      benchmark("approximate percentile", benchmarkRuns) {
        Task22Query.approximateResults(orders).count()
      }.print()
      benchmark("exact percentile", benchmarkRuns) {
        Task22Query.exactResults(orders, spark).count()
      }.print()

      val result = Task22Query.build(orders, spark)
      println("===== TASK 2-2 EXTENDED EXECUTION PLAN =====")
      result.explain(extended = true)
      println("===== END EXECUTION PLAN =====")

      SingleParquetExporter.write(result, args(1))
      println("Output written to: " + args(1))
    } finally {
      if (orders != null) {
        orders.unpersist(blocking = false)
      }
      spark.stop()
    }
  }

  private def parseBenchmarkRuns(value: Option[String]): Int = {
    val parsed = value.map(_.toInt).getOrElse(Task22Config.MinimumBenchmarkRuns)
    require(
      parsed >= Task22Config.MinimumBenchmarkRuns,
      "benchmark-runs must be at least " + Task22Config.MinimumBenchmarkRuns
    )
    parsed
  }

  private def benchmark(label: String, runs: Int)(action: => Long): BenchmarkSummary = {
    action // Warm-up is intentionally excluded from the reported measurements.
    val measurements = Vector.fill(runs) {
      val startedAt = System.nanoTime()
      action
      (System.nanoTime() - startedAt).toDouble / 1000000.0
    }
    BenchmarkSummary(label, measurements)
  }
}
