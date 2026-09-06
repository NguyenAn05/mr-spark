ThisBuild / organization := "vn.edu.hcmus.bigdata"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "lab03-mr-spark",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_1-1",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_1-2",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_2-1",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_2-2",
    Compile / run / fork := true,
    Compile / console / scalacOptions --= Seq("-Xfatal-warnings"),
    libraryDependencies ++= Seq(
      // Hadoop is supplied by the Hadoop installation at runtime.
      "org.apache.hadoop" % "hadoop-client" % "3.5.0" % Provided,
      // Spark is supplied by spark-submit at runtime.
      "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided
    ),
    Compile / doc / sources := Seq.empty
  )
