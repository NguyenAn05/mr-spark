package task12

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer, Partitioner}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

class VarietyCountMapper extends Mapper[org.apache.hadoop.io.LongWritable, Text, VarietyKey, IntWritable] {
  private val outValue = new IntWritable()

  override def map(key: org.apache.hadoop.io.LongWritable, value: Text, context: Mapper[org.apache.hadoop.io.LongWritable, Text, VarietyKey, IntWritable]#Context): Unit = {
    Task12RecordParser.parseRecord(value.toString).foreach { record =>
      val outKey = VarietyKey(record.month, record.state, record.style, record.sku)
      outValue.set(record.sizeRank)
      context.write(outKey, outValue)
    }
  }
}

class VarietyPartitioner extends Partitioner[VarietyKey, IntWritable] {
  override def getPartition(key: VarietyKey, value: IntWritable, numPartitions: Int): Int = {
    val hash = (key.month + key.state + key.style).hashCode()
    (hash & Integer.MAX_VALUE) % numPartitions
  }
}

class VarietyCountReducer extends Reducer[VarietyKey, IntWritable, MedianKey, IntWritable] {
  private val outValue = new IntWritable()

  override def reduce(key: VarietyKey, values: java.lang.Iterable[IntWritable], context: Reducer[VarietyKey, IntWritable, MedianKey, IntWritable]#Context): Unit = {
    var distinctSkuCount = 0
    var maxSizeRank = -1
    var lastSku = ""

    val it = values.iterator()
    while (it.hasNext) {
      val sizeRank = it.next().get()
      
      // NOTE: This is safe because of Hadoop's Secondary Sort pattern.
      // The framework reuses the `key` object and mutates its fields (including sku) 
      // each time `it.next()` is called, reflecting the current record's actual key.
      val currentSku = key.sku
      
      if (lastSku == "" || lastSku != currentSku) {
        distinctSkuCount += 1
        lastSku = currentSku
      }
      if (sizeRank > maxSizeRank) {
        maxSizeRank = sizeRank
      }
    }

    if (maxSizeRank >= Task12Config.TargetSizeRank) {
      outValue.set(distinctSkuCount)
      context.write(MedianKey(key.month, key.state), outValue)
    }
  }
}

object VarietyCountJob {
  def configure(input: String, output: String): Job = {
    val job = Job.getInstance()
    job.setJobName("Task1-2 Variety Count Job")
    job.setJarByClass(classOf[VarietyCountMapper])

    job.setMapperClass(classOf[VarietyCountMapper])
    job.setReducerClass(classOf[VarietyCountReducer])

    job.setPartitionerClass(classOf[VarietyPartitioner])
    job.setGroupingComparatorClass(classOf[VarietyGroupComparator])
    job.setSortComparatorClass(classOf[VarietySortComparator])

    job.setMapOutputKeyClass(classOf[VarietyKey])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[MedianKey])
    job.setOutputValueClass(classOf[IntWritable])

    job.setOutputFormatClass(classOf[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[MedianKey, IntWritable]])

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    job
  }
}
