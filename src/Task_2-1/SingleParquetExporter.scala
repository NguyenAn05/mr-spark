package task21

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.UUID

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object SingleParquetExporter {
  /**
    * This helper writes to a temporary directory, then moves the only Parquet part file to the exact local output path required by the assignment
    */
  def write(data: DataFrame, outputValue: String): Unit = {
    val output = Paths.get(outputValue).toAbsolutePath.normalize()
    if (Files.exists(output)) {throw new IllegalArgumentException(s"Output already exists: $output")}

    val parent = Option(output.getParent).getOrElse(Paths.get(".").toAbsolutePath.normalize())
    Files.createDirectories(parent)

    val temporaryDirectory = parent.resolve(s".${output.getFileName}.spark-tmp-${UUID.randomUUID()}")

    try {
      data.coalesce(1).write.mode("errorifexists").parquet(temporaryDirectory.toUri.toString)

      val listing = Files.list(temporaryDirectory)
      val partFiles = try {
        listing.iterator().asScala.filter(path => {
            val name = path.getFileName.toString
            name.startsWith("part-") && name.endsWith(".parquet")
          }).toVector
      } finally {
        listing.close()
      }

      if (partFiles.size != 1) {
        throw new IllegalStateException(s"Expected exactly one Parquet part file, found ${partFiles.size}")
      }

      Files.move(partFiles.head, output, StandardCopyOption.ATOMIC_MOVE)
    } finally {
      deleteRecursively(temporaryDirectory)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val paths = Files.walk(path)
      try {
        paths.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists)
      } finally {
        paths.close()
      }
    }
  }
}
