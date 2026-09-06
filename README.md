# Lab 03 — Advanced MapReduce and Spark Structured APIs

This repository contains Scala implementations for four problems from the Introduction to Big Data Analysis lab:

- **Task 1-1:** a dynamic-length sliding-window computation implemented with Hadoop MapReduce.
- **Task 1-2:** a conditionally filtered median aggregation implemented with Hadoop Secondary Sort.
- **Task 2-1:** a city-level cancellation-percentage query implemented exclusively with Spark's DataFrame API.
- **Task 2-2:** exact and approximate dynamic percentile computations implemented exclusively with Spark's DataFrame API.

The implementations use Scala 2.12.18, Hadoop 3.5.0, Spark 3.5.9, Java 17, and sbt.

## Repository structure

```text
mr-spark/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── data/
│   ├── Amazon Sale Report.csv
│   └── shapes.parquet(legacy)
├── docs/
│   ├── Lab 3 - MR-Spark.pdf
│   └── task_ideas/
│       ├── Task_1-1.md
│       ├── Task_1-2.md
│       └── Task_2-1.md
├── src/
│   ├── Task_1-1/
│   ├── Task_1-2/
│   └── Task_2-1/
└── outputs/
```

## Implemented tasks

### Task 1-1

Task 1-1 uses three Hadoop MapReduce jobs:

1. Count bought orders by state and derive a 5-day or 10-day window.
2. Map each bought record into its future window buckets and aggregate by `(state, window_date, size)`.
3. Select the winning size by purchase count, population variance, and lexicographic order.

The final result is exported as one local CSV file.

### Task 1-2

Task 1-2 uses two Hadoop MapReduce jobs with Hadoop Secondary Sort:

1. Count distinct SKUs (variety) for each style and filter out styles that do not have an "XXL" size sold.
2. Collect the variety counts for each `(month, state)`, sort them, and compute the median.

The final result is exported as one local CSV file.

### Task 2-1

Task 2-1 uses Spark Structured APIs to:

1. Derive the active period of every promotion.
2. Count temporally valid promotions per order record.
3. Calculate the average Merchant/Shipped amount for each state.
4. Calculate the qualifying percentage for every city.

The program prints `explain(true)`, records the executed Spark stage IDs, and exports the final result as one local Parquet file.

## Required environment

The tested environment is:

| Component             | Version                        |
| --------------------- | ------------------------------ |
| Operating system      | Ubuntu Linux / WSL2            |
| Java                  | OpenJDK 17                     |
| Hadoop                | 3.5.0, pseudo-distributed mode |
| Spark                 | 3.5.9 for Hadoop 3             |
| Scala binary version  | 2.12                           |
| Project Scala version | 2.12.18                        |
| sbt                   | 2.0.1                          |

The system-wide `scala` command does not need to be Scala 2.12.18. sbt downloads the Scala compiler specified by `build.sbt`, while Spark 3.5.9 supplies its own compatible Scala 2.12 runtime.

Official references:

- [Hadoop 3.5.0 single-node setup](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark 3.5.9 documentation](https://spark.apache.org/docs/3.5.9/)
- [sbt installation](https://www.scala-sbt.org/download/)

## Environment setup

### 1. Install base packages

On Ubuntu or WSL2:

```bash
sudo apt update
sudo apt install openjdk-17-jdk openssh-server pdsh curl tar
```

Install sbt using the instructions on the official sbt download page. The file `project/build.properties` pins the project to sbt 2.0.1.

### 2. Install Hadoop and Spark

Download and extract:

- Hadoop 3.5.0 to `$HOME/hadoop-3.5.0`.
- Spark 3.5.9 built for Hadoop 3 to `$HOME/tools/spark-3.5.9`.

Different installation directories are allowed, but the environment variables below must be updated accordingly.

### 3. Configure shell variables

Add this block near the end of `~/.bashrc`, after any early non-interactive-shell return:

```bash
# Java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Hadoop
export HADOOP_HOME="$HOME/hadoop-3.5.0"
export HADOOP_INSTALL="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
export YARN_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Spark
export SPARK_HOME="$HOME/tools/spark-3.5.9"

export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"
```

Reload the shell configuration:

```bash
source ~/.bashrc
```

Verify the tools:

```bash
java -version
hadoop version
spark-submit --version
sbt --version
```

Expected key versions are Java 17, Hadoop 3.5.0, Spark 3.5.9, and Scala 2.12.18 in the Spark version output.

### 4. Configure Hadoop pseudo-distributed mode

Use the following essential configuration.

`$HADOOP_HOME/etc/hadoop/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/mapred-site.xml`:

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>localhost:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>localhost:19888</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/yarn-site.xml`:

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
```

### `mapred --daemon start historyserver`5. Configure passwordless SSH

Hadoop's daemon scripts require SSH access to localhost:

```bash
sudo service ssh start
test -f ~/.ssh/id_ed25519 || ssh-keygen -t ed25519
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost
```

Accept the host fingerprint when prompted. A subsequent `ssh localhost` should not request the key passphrase.

### 6. Initialize and start Hadoop

Format the NameNode only for a new Hadoop installation with no existing HDFS data:

```bash
hdfs namenode -format
```

Do not repeat this command for an existing filesystem because formatting creates a new HDFS namespace.

Start HDFS and YARN:

```bash
start-dfs.sh
start-yarn.sh
mapred --daemon start historyserver
jps
```

The expected Java processes are:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
JobHistoryServer
Jps
```

The default web interfaces are:

- NameNode: [http://localhost:9870](http://localhost:9870)
- ResourceManager: [http://localhost:8088](http://localhost:8088)

## Build the project

From the repository root:

```bash
sbt compile
sbt assembly
```

The assembly JAR is created at:

```text
target/out/jvm/scala-2.12.18/lab03-mr-spark/
lab03-mr-spark-assembly-0.1.0-SNAPSHOT.jar
```

The warning about multiple main classes is expected because Task 1-1, Task 1-2, and Task 2-1 have different entry points. Each run command explicitly supplies the required main class.

Set reusable paths after building:

```bash
JAR_PATH="./target/out/jvm/scala-2.12.18/lab03-mr-spark/lab03-mr-spark-assembly-0.1.0-SNAPSHOT.jar"
mkdir -p "./outputs"
```

## Run Task 1-1

Task 1-1 runs on Hadoop MapReduce through YARN and reads its input from HDFS.

### 1. Upload the CSV to HDFS

```bash
hdfs dfs -mkdir -p "/user/$USER/lab03/input"
hdfs dfs -put "./data/Amazon%20Sale%20Report.csv" "/user/$USER/lab03/input/asr.csv"
hdfs dfs -ls "/user/$USER/lab03/input"
```

If `asr.csv` already exists, skip the `-put` command or choose a different HDFS filename. Renaming the HDFS copy does not rename or modify the original local dataset.

### 2. Run the complete three-job pipeline

```bash
TASK11_WORK="/user/$USER/lab03/task11/run-01"

hadoop jar "$JAR_PATH" \
  task11.Task11Driver \
  "/user/$USER/lab03/input/asr.csv" \
  "./outputs/Task_1-1.csv" \
  "$TASK11_WORK"
```

Both `TASK11_WORK` and the local output file must not already exist. Use a new work path, such as `run-02`, when repeating the task, and move or rename the previous local output if it must be preserved.

Inspect the result:

```bash
wc -l "./outputs/Task_1-1.csv"
head -n 20 "./outputs/Task_1-1.csv"
hdfs dfs -ls -R "$TASK11_WORK"
```

Expected output filename:

```text
outputs/Task_1-1.csv
```

## Run Task 1-2

Task 1-2 runs on Hadoop MapReduce through YARN and reads its input from HDFS. Ensure the CSV is uploaded to HDFS (refer to Task 1-1, step 1).

### Run the complete two-job pipeline

```bash
TASK12_WORK="/user/$USER/lab03/task12/run-01"

hadoop jar "$JAR_PATH" \
  task12.Task12Driver \
  "/user/$USER/lab03/input/asr.csv" \
  "./outputs/Task_1-2.csv" \
  "$TASK12_WORK"
```

Both `TASK12_WORK` and the local output file must not already exist. Use a new work path, such as `run-02`, when repeating the task, and move or rename the previous local output if it must be preserved.

Inspect the result:

```bash
wc -l "./outputs/Task_1-2.csv"
head -n 20 "./outputs/Task_1-2.csv"
hdfs dfs -ls -R "$TASK12_WORK"
```

Expected output filename:

```text
outputs/Task_1-2.csv
```

## Run Task 2-1

Task 2-1 runs Spark in local mode and reads the CSV from the normal local filesystem.

Because Hadoop is configured with `hdfs://localhost:9000` as its default filesystem, the run command explicitly overrides Spark's Hadoop filesystem setting with `file:///`.

```bash
spark-submit \
  --master 'local[*]' \
  --conf 'spark.hadoop.fs.defaultFS=file:///' \
  --class task21.Task21App \
  "$JAR_PATH" \
  "./data/Amazon Sale Report.csv" \
  "./outputs/Task_2-1.parquet" \
  2>&1 | tee "./outputs/task21-run.log"
```

The local Parquet output must not already exist. The log contains the required extended execution plan from `explain(true)` and the completed Spark stage IDs.

Expected output filename:

```text
outputs/Task_2-1.parquet
```

### Inspect the Parquet output

Start Spark Shell:

```bash
spark-shell \
  --master 'local[*]' \
  --conf 'spark.hadoop.fs.defaultFS=file:///'
```

Then run:

```scala
val result = spark.read.parquet(
  "file:///absolute/path/to/Lab03/outputs/Task_2-1.parquet"
)

result.printSchema()
println(result.count())
result.show(20, truncate = false)
```

Exit Spark Shell with:

```scala
:quit
```

## Tested results

The completed local run produced:

| Task                        | Result                                              |
| --------------------------- | --------------------------------------------------- |
| Task 1-1                    | 4,584 result rows plus one CSV header               |
| Task 1-2                    | 128 result rows plus one CSV header                 |
| Task 2-1                    | 1,639 city rows in one Parquet file                 |
| Task 2-1 physical joins     | Three`BroadcastHashJoin` operators                |
| Task 2-1 Exchange operators | Six shuffle exchanges and three broadcast exchanges |
| Task 2-1 completed stages   | 11                                                  |

Task 2-1 returns zero qualifying orders for the provided dataset because all Cancelled/Standard records have empty promotion lists. The report in `docs/task_ideas/Task_2-1.md` explains this result and the percentage-denominator assumption.

## Reports

Detailed design and execution analysis are available in:

- `docs/task_ideas/Task_1-1.md`
- `docs/task_ideas/Task_1-2.md`
- `docs/task_ideas/Task_2-1.md`
