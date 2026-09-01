package task12

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => LocalPath, Paths, StandardOpenOption}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

object Task12Driver {
  private val Header = "month,state,median_variety"

  private def requireSuccessful(job: Job, label: String): Unit = {
    println(s"Starting $label: ${job.getJobName}")
    if (!job.waitForCompletion(true)) {
      throw new IllegalStateException(s"$label failed")
    }
  }

  private def resolveLocalPath(value: String): LocalPath = {
    if (value.startsWith("file:")) Paths.get(new URI(value))
    else Paths.get(value).toAbsolutePath.normalize()
  }

  private def exportFinalCsv(finalOutput: String, localOutputValue: String): LocalPath = {
    val configuration = new Configuration()
    val finalPath = new Path(finalOutput)
    val fileSystem = finalPath.getFileSystem(configuration)
    val partFiles = fileSystem.listStatus(finalPath).filter(status => status.isFile && status.getPath.getName.startsWith("part-")).sortBy(_.getPath.getName)

    require(partFiles.nonEmpty, s"No part files found under $finalOutput")

    val localOutput = resolveLocalPath(localOutputValue)
    Option(localOutput.getParent).foreach { parent =>
      Files.createDirectories(parent)
    }

    val writer = Files.newBufferedWriter(localOutput, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

    try {
      writer.write(Header)
      writer.newLine()

      partFiles.foreach { status =>
        val reader = new BufferedReader(new InputStreamReader(fileSystem.open(status.getPath), StandardCharsets.UTF_8))
        try {
          var line = reader.readLine()
          while (line != null) {
            writer.write(line)
            writer.newLine()
            line = reader.readLine()
          }
        } finally reader.close()
      }
    } finally writer.close()

    localOutput
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2 || args.length > 3) {
      System.err.println("Usage: Task12Driver <input> <local-output-csv> [work-directory]")
      System.exit(1)
    }

    val input = args(0)
    val localOutput = args(1)
    val userName = System.getProperty("user.name")
    val workDirectory = if (args.length == 3) args(2).stripSuffix("/") else s"temp-lab03-task12/run-${System.currentTimeMillis()}"

    val varietyCountOutput = s"$workDirectory/job1-variety-count"
    val medianVarietyOutput = s"$workDirectory/job2-median-variety"

    val configuration = new Configuration()
    val workPath = new Path(workDirectory)
    val fileSystem = workPath.getFileSystem(configuration)

    if (fileSystem.exists(workPath)) {
      System.err.println(s"Work directory already exists: $workDirectory. Choose a new path.")
      System.exit(1)
    }

    try {
      requireSuccessful(VarietyCountJob.configure(input, varietyCountOutput), "Job 1 (Variety Count)")
      requireSuccessful(MedianVarietyJob.configure(varietyCountOutput, medianVarietyOutput), "Job 2 (Median Variety)")

      val exportedPath = exportFinalCsv(medianVarietyOutput, localOutput)
      println(s"Task 1-2 completed successfully: $exportedPath")
      println(s"Intermediate Hadoop outputs: $workDirectory")
    } catch {
      case error: Exception =>
        System.err.println(s"Task 1-2 failed: ${error.getMessage}")
        error.printStackTrace(System.err)
        System.exit(1)
    }
  }
}
