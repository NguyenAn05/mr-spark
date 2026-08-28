package task11

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.util.Try

/**
  * Job 3: select one winning size for each (state, window_date)
  * Tie-breaking order:
  *   1. highest purchase count
  *   2. lowest defined population variance; a defined variance ranks before
  *      an undefined variance
  *   3. lexicographically smallest size when variance does not decide the tie
  */
object WinnerSelectionJob {
  val Description = "Select the winning size for each state and window date"
  private val ColumnSeparator = "\t"
  private val CounterGroup = "Task11"

  private final case class Candidate(
      size: String,
      windowLength: Int,
      purchaseCount: Long,
      populationVariance: Option[Double]
  )

  private def parseVariance(value: String): Option[Option[Double]] = {
    val trimmed = value.trim
    if (trimmed.isEmpty) Some(None)
    else {
      Try(trimmed.toDouble).toOption
        .filter(value => java.lang.Double.isFinite(value))
        .map(Some(_))
    }
  }

  class CandidateMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val outputKey = new Text()
    private val outputValue = new Text()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val columns = value.toString.split(ColumnSeparator, -1)

      if (columns.length == 9) {
        val state = columns(0)
        val windowDate = columns(1)
        val size = columns(2)

        val parsed = for {
          windowLength <- Try(columns(3).toInt).toOption
          purchaseCount <- Try(columns(4).toLong).toOption
          variance <- parseVariance(columns(8))
        } yield (windowLength, purchaseCount, variance)

        parsed match {
          case Some((windowLength, purchaseCount, variance)) =>
            outputKey.set(Seq(state, windowDate).mkString(ColumnSeparator))
            outputValue.set(
              Seq(
                size,
                windowLength.toString,
                purchaseCount.toString,
                variance.map(java.lang.Double.toString).getOrElse("")
              ).mkString(ColumnSeparator)
            )
            context.write(outputKey, outputValue)

          case None =>
            context.getCounter(CounterGroup, "MALFORMED_JOB2_RECORD").increment(1L)
        }
      } else {
        context.getCounter(CounterGroup, "MALFORMED_JOB2_RECORD").increment(1L)
      }
    }
  }

  class WinnerReducer extends Reducer[Text, Text, NullWritable, Text] {
    private val outputLine = new Text()

    private def isBetter(candidate: Candidate, current: Candidate): Boolean = {
      if (candidate.purchaseCount != current.purchaseCount) {
        candidate.purchaseCount > current.purchaseCount
      } else {
        (candidate.populationVariance, current.populationVariance) match {
          case (Some(candidateVariance), Some(currentVariance)) =>
            val comparison = java.lang.Double.compare(candidateVariance, currentVariance)
            if (comparison != 0) comparison < 0
            else candidate.size.compareTo(current.size) < 0

          case (Some(_), None) => true
          case (None, Some(_)) => false
          case (None, None)    => candidate.size.compareTo(current.size) < 0
        }
      }
    }

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, NullWritable, Text]#Context): Unit = {
      val iterator = values.iterator()
      var winner: Option[Candidate] = None

      while (iterator.hasNext) {
        val columns = iterator.next().toString.split(ColumnSeparator, -1)
        if (columns.length == 4) {
          val parsed = for {
            windowLength <- Try(columns(1).toInt).toOption
            purchaseCount <- Try(columns(2).toLong).toOption
            variance <- parseVariance(columns(3))
          } yield Candidate(columns(0), windowLength, purchaseCount, variance)

          parsed.foreach { candidate =>
            winner match {
              case Some(current) if !isBetter(candidate, current) =>
              case _ => winner = Some(candidate)
            }
          }
        }
      }

      winner.foreach { selected =>
        val keyColumns = key.toString.split(ColumnSeparator, 2)
        if (keyColumns.length == 2) {
          outputLine.set(
            CsvParser.formatRow(
              Seq(
                keyColumns(0),
                keyColumns(1),
                selected.size,
                selected.purchaseCount.toString,
                selected.populationVariance.map(java.lang.Double.toString).getOrElse(""),
                selected.windowLength.toString
              )
            )
          )
          context.write(NullWritable.get(), outputLine)
          context.getCounter(CounterGroup, "WINNERS_SELECTED").increment(1L)
        }
      }
    }
  }

  def configure(input: String, output: String): Job = {
    val configuration = new Configuration()
    val job = Job.getInstance(configuration, Description)

    job.setJarByClass(classOf[CandidateMapper])
    job.setMapperClass(classOf[CandidateMapper])
    job.setReducerClass(classOf[WinnerReducer])
    job.setNumReduceTasks(1)

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    job
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WinnerSelectionJob <input> <output>")
      System.exit(1)
    }

    val job = configure(args(0), args(1))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
