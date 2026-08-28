package task21

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

final class CompletedStageCollector extends SparkListener {
  private val completedStageIds = ConcurrentHashMap.newKeySet[Integer]()
  @volatile private var lastEventNanos = System.nanoTime()

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    completedStageIds.add(event.stageInfo.stageId)
    lastEventNanos = System.nanoTime()
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = lastEventNanos = System.nanoTime()

  def stageIds: Vector[Int] = completedStageIds.asScala.map(_.intValue()).toVector.sorted

  def awaitQuiescence(maxWaitMillis: Long, quietMillis: Long): Unit = {
    val deadline = System.nanoTime() + maxWaitMillis * 1000000L
    val quietNanos = quietMillis * 1000000L

    while (System.nanoTime() < deadline && System.nanoTime() - lastEventNanos < quietNanos) {
      Thread.sleep(50L)
    }
  }
}

object Task21App {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: Task21App <input-csv> <local-output-parquet>")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("Lab03 Task 2-1").getOrCreate()

    try {
      val raw = spark.read.option("header", "true").option("mode", "PERMISSIVE").schema(Task21Config.InputSchema).csv(args(0))

      val result = Task21Query.build(raw)

      println("===== TASK 2-1 EXTENDED EXECUTION PLAN =====")
      result.explain(extended = true)
      println("===== END EXECUTION PLAN =====")

      val stageCollector = new CompletedStageCollector
      spark.sparkContext.addSparkListener(stageCollector)

      SingleParquetExporter.write(result, args(1))

      // Listener callbacks are asynchronous, so allow a short quiet period
      // before reading the collected stage IDs.
      stageCollector.awaitQuiescence(maxWaitMillis = 10000L, quietMillis = 500L)

      println(s"Output written to: ${args(1)}")
      println(s"Completed Spark stage IDs: ${stageCollector.stageIds.mkString(", ")}")
      println(s"Number of completed Spark stages: ${stageCollector.stageIds.size}")
    } finally {
      spark.stop()
    }
  }
}
