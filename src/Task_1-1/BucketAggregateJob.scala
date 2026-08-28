package task11

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Job 2: map bought orders to sliding-window buckets and aggregate each (state, window_date, size) group
  * Intermediate output columns are tab-separated: state, window_date, size,
  * window_length, purchase_count, amount_count, sum_amount,
  * sum_amount_squared, population_variance
  */
object BucketAggregateJob {
  val Description = "Map bought orders to sliding-window buckets and aggregate"
  private val ColumnSeparator = "\t"
  private val CounterGroup = "Task11"

  /**
    * Mapper setup loads Job 1's state counts from Distributed Cache
    * For a bought order on date p, map emits one summary into p+1 through p+w
    */
  class BucketMapper
      extends Mapper[LongWritable, Text, Text, AmountSummaryWritable] {
    private val stateWindows = mutable.HashMap.empty[String, Int]
    private val outputKey = new Text()
    private val outputSummary = new AmountSummaryWritable()

    override def setup(context: Mapper[LongWritable, Text, Text, AmountSummaryWritable]#Context): Unit = {
      val cacheFiles = Option(context.getCacheFiles).getOrElse(Array.empty[URI])

      cacheFiles.foreach {cacheUri =>
        val localName = Option(cacheUri.getFragment).getOrElse { new Path(cacheUri.getPath).getName}
        val lines = Files.readAllLines(Paths.get(localName), StandardCharsets.UTF_8)

        lines.asScala.foreach { line =>
          val columns = line.split(ColumnSeparator, 2)
          if (columns.length == 2) {
            Try(columns(1).trim.toLong).toOption.foreach { totalBought =>
              stateWindows.update(columns(0), Task11Config.windowLength(totalBought))
            }
          }
        }
      }

      if (stateWindows.isEmpty) {
        throw new IllegalStateException("No state counts were loaded from the Job 1 Distributed Cache files")
      }
    }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, AmountSummaryWritable]#Context): Unit = {
      Task11RecordParser.parseBought(value.toString).foreach { order =>
        stateWindows.get(order.state) match {
          case Some(windowLength) =>
            val (amountCount, sumAmount, sumAmountSquared) = order.amount match {
              case Some(amount) => (1L, amount, amount * amount)
              case None         => (0L, 0.0, 0.0)
            }

            if (order.amount.isEmpty) {
              context
                .getCounter(CounterGroup, "BOUGHT_ORDERS_WITHOUT_AMOUNT")
                .increment(1L)
            }

            outputSummary.set(
              purchaseCount = 1L,
              amountCount = amountCount,
              sumAmount = sumAmount,
              sumAmountSquared = sumAmountSquared
            )
            var offset = 1
            while (offset <= windowLength) {
              val windowDate = order.purchaseDate.plusDays(offset.toLong)
              outputKey.set(Seq(order.state, Task11Config.formatDate(windowDate), order.size, windowLength.toString).mkString(ColumnSeparator))
              context.write(outputKey, outputSummary)
              context.getCounter(CounterGroup, "BUCKETS_EMITTED").increment(1L)
              offset += 1
            }

          case None =>
            context.getCounter(CounterGroup, "MISSING_STATE_WINDOW").increment(1L)
        }
      }
    }
  }

  /** Combiner: aggregate summaries locally before shuffle */
  class SummaryCombiner
      extends Reducer[Text, AmountSummaryWritable, Text, AmountSummaryWritable] {
    private val combined = new AmountSummaryWritable()

    override def reduce(key: Text, values: java.lang.Iterable[AmountSummaryWritable], context: Reducer[Text, AmountSummaryWritable, Text, AmountSummaryWritable]#Context): Unit = {
      val iterator = values.iterator()
      var purchaseCount = 0L
      var amountCount = 0L
      var sumAmount = 0.0
      var sumAmountSquared = 0.0

      while (iterator.hasNext) {
        val value = iterator.next()
        purchaseCount += value.purchaseCount
        amountCount += value.amountCount
        sumAmount += value.sumAmount
        sumAmountSquared += value.sumAmountSquared
      }

      combined.set(purchaseCount, amountCount, sumAmount, sumAmountSquared)
      context.write(key, combined)
    }
  }

  /** Reducer: produce one aggregate candidate for every bucket and size */
  class SummaryReducer 
    extends Reducer[Text, AmountSummaryWritable, NullWritable, Text] {
    private val outputLine = new Text()

    override def reduce(key: Text, values: java.lang.Iterable[AmountSummaryWritable], context: Reducer[Text, AmountSummaryWritable, NullWritable, Text]#Context): Unit = {
      val iterator = values.iterator()
      var purchaseCount = 0L
      var amountCount = 0L
      var sumAmount = 0.0
      var sumAmountSquared = 0.0

      while (iterator.hasNext) {
        val value = iterator.next()
        purchaseCount += value.purchaseCount
        amountCount += value.amountCount
        sumAmount += value.sumAmount
        sumAmountSquared += value.sumAmountSquared
      }

      val summary = AmountSummary(
        purchaseCount,
        amountCount,
        sumAmount,
        sumAmountSquared
      )
      val variance = summary.populationVariance
        .map(java.lang.Double.toString)
        .getOrElse("")

      outputLine.set(
        Seq(
          key.toString,
          purchaseCount.toString,
          amountCount.toString,
          java.lang.Double.toString(sumAmount),
          java.lang.Double.toString(sumAmountSquared),
          variance
        ).mkString(ColumnSeparator)
      )
      context.write(NullWritable.get(), outputLine)
    }
  }

  /** Configure Job 2 and attach every Job 1 part file to Distributed Cache */
  def configure(input: String, stateCountOutput: String, output: String): Job = {
    val configuration = new Configuration()
    val stateCountPath = new Path(stateCountOutput)
    val fileSystem = stateCountPath.getFileSystem(configuration)
    val stateCountParts = fileSystem.listStatus(stateCountPath).filter(status => status.isFile && status.getPath.getName.startsWith("part-"))

    require(stateCountParts.nonEmpty, s"No Job 1 part files found under $stateCountOutput")

    val job = Job.getInstance(configuration, Description)
    job.setJarByClass(classOf[BucketMapper])
    job.setMapperClass(classOf[BucketMapper])
    job.setCombinerClass(classOf[SummaryCombiner])
    job.setReducerClass(classOf[SummaryReducer])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[AmountSummaryWritable])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    stateCountParts.foreach { status =>
      val sourceUri = status.getPath.toUri.toString
      val localName = status.getPath.getName
      job.addCacheFile(new URI(s"$sourceUri#$localName"))
    }

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    job
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: BucketAggregateJob <input> <state-count-output> <output>")
      System.exit(1)
    }

    val job = configure(args(0), args(1), args(2))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
