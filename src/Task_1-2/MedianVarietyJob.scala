package task12

import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

class MedianVarietyMapper extends Mapper[MedianKey, IntWritable, MedianKey, IntWritable] {
  override def map(key: MedianKey, value: IntWritable, context: Mapper[MedianKey, IntWritable, MedianKey, IntWritable]#Context): Unit = {
    context.write(key, value)
  }
}

class MedianVarietyReducer extends Reducer[MedianKey, IntWritable, Text, NullWritable] {
  private val outKey = new Text()

  override def reduce(key: MedianKey, values: java.lang.Iterable[IntWritable], context: Reducer[MedianKey, IntWritable, Text, NullWritable]#Context): Unit = {
    val varieties = ArrayBuffer[Int]()
    val it = values.iterator()
    while (it.hasNext) {
      varieties += it.next().get()
    }

    if (varieties.nonEmpty) {
      val sorted = varieties.sorted
      val n = sorted.length
      val median = if (n % 2 == 1) {
        sorted(n / 2).toDouble
      } else {
        (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
      }

      val escapedState = task11.CsvParser.escapeField(key.state)
      outKey.set(s"${key.month},$escapedState,$median")
      context.write(outKey, NullWritable.get())
    }
  }
}

object MedianVarietyJob {
  def configure(input: String, output: String): Job = {
    val job = Job.getInstance()
    job.setJobName("Task1-2 Median Variety Job")
    job.setJarByClass(classOf[MedianVarietyMapper])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[MedianKey, IntWritable]])

    job.setMapperClass(classOf[MedianVarietyMapper])
    job.setReducerClass(classOf[MedianVarietyReducer])

    job.setMapOutputKeyClass(classOf[MedianKey])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    job
  }
}
