package task11

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
  * Job 1: count bought orders by state
  * Output format: state<TAB>totalBoughtOrders
  */
object StateCountJob {
  val Description = "Count bought orders by state and derive window length"
  private val CounterGroup = "Task11"

  /** Mapper: emit one contribution for every valid bought order */
  class BoughtOrderMapper
      extends Mapper[LongWritable, Text, Text, LongWritable] {
    private val outputState = new Text()
    private val one = new LongWritable(1L)

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
      Task11RecordParser.parseBought(value.toString).foreach { order =>
        if (order.amount.isEmpty) {
          context.getCounter(CounterGroup, "BOUGHT_ORDERS_WITHOUT_AMOUNT").increment(1L)
        }
        outputState.set(order.state)
        context.write(outputState, one)
      }
    }
  }

  /** Combiner and reducer: sum contributions for each state */
  class SumReducer
      extends Reducer[Text, LongWritable, Text, LongWritable] {
    private val outputCount = new LongWritable()

    override def reduce(state: Text, values: java.lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
      val iterator = values.iterator()
      var total = 0L

      while (iterator.hasNext) {
        total += iterator.next().get()
      }

      outputCount.set(total)
      context.write(state, outputCount)
    }
  }

  /** Configure one Hadoop MapReduce job for state counting */
  def configure(input: String, output: String): Job = {
    val configuration = new Configuration()
    val job = Job.getInstance(configuration, Description)

    job.setJarByClass(classOf[BoughtOrderMapper])
    job.setMapperClass(classOf[BoughtOrderMapper])
    job.setCombinerClass(classOf[SumReducer])
    job.setReducerClass(classOf[SumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    job
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: StateCountJob <input> <output>")
      System.exit(1)
    }

    val job = configure(args(0), args(1))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
