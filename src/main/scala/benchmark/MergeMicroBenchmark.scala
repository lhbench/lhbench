package benchmark

import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.Locale

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MergeMicroBenchmarkConf(
  protected val format: Option[String] = None,
  tableType: String = "cow",
  sizeInGB: Int = 1,
  sourcePercent: Option[Double] = None,
  tableUpdatePercent: Option[Double] = None,
  benchmarkPath: Option[String] = None,
  dbName: Option[String] = None,
  partitionTables: Boolean = false,
  partitionCount: Int = 1,
  mergeRowLimit: Option[Int] = None,
) extends BenchmarkConf {
  def formatName: String = format.getOrElse {
    throw new IllegalArgumentException("format must be specified")
  }
}

object MergeMicroBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[MergeMicroBenchmarkConf]
  private val argParser = {
    import builder._

    OParser.sequence(
      programName("MergeMicroBenchmark"),
      opt[String]("size-in-gb")
        .required()
        .valueName("<table size in GBs>")
        .action((x, c) => c.copy(sizeInGB = x.toInt))
        .text("Size of initial benchmark table in GBs"),
      opt[String]("format")
        .required()
        .valueName("<delta|iceberg|hudi>")
        .action((x, c) => c.copy(format = Some(x)))
        .text("Spark's short name for the file format to use"),
      opt[String]("table-type")
        .optional()
        .valueName("<cow|mor>")
        .validate { x =>
          val valid = Set("cow", "mor")
          if (valid.exists(_.equalsIgnoreCase(x))) success
          else failure("valid values are " + valid)
        }
        .action((x, c) => c.copy(tableType = x))
        .text("Spark's short name for the file format to use"),
      opt[Double]("source-percent")
        .optional()
        .action((x, c) => c.copy(sourcePercent = Some(x)))
        .text("How large is the source compared to the table"),
      opt[Double]("table-update-percent")
        .optional()
        .action((x, c) => c.copy(tableUpdatePercent = Some(x)))
        .text("What fraction of the table to update by the merge, upper bounded by source percent"),
      opt[String]("benchmark-path")
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[Boolean]("partition-table")
        .optional()
        .valueName("true/false")
        .action((x, c) => c.copy(partitionTables = x))
        .text("Whether to partition the table"),
      opt[Int]("partition-count")
        .optional()
        .valueName("<number of partitions>")
        .action((x, c) => c.copy(partitionCount = x))
        .text("Number of partitions to use for the table"),
      opt[Int]("merge-row-limit")
        .optional()
        .valueName("<number of rows to merge>")
        .action((x, c) => c.copy(mergeRowLimit = Some(x)))
        .text("Number of rows to merge"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(dbName = Some(x)))
        .text("Database name to use for the table"),
    )
  }

  def parse(args: Array[String]): Option[MergeMicroBenchmarkConf] = {
    OParser.parse(argParser, args, MergeMicroBenchmarkConf())
  }
}

class MergeMicroBenchmark(conf: MergeMicroBenchmarkConf) extends Benchmark(conf) {
  lazy val dbName = conf.dbName.getOrElse(s"merge_micro_${conf.sizeInGB}gb_${conf.formatName}")
  lazy val dbLocation = conf.dbLocation(dbName, suffix=benchmarkId.replace("-", "_"))
  lazy val fullBenchmarkTableName = s"`$dbName`.`$benchmarkTableName`"
  def tableLocation(tableName: String) = s"$dbLocation/$tableName"
  val numReadQueryIterations = 3
  val targetFileSizeInGB = 0.1

  val numRowsPerGB = 70L * 1000L * 1000L

  // If not specified, source is 1% of benchmark table
  val sourcePercent = conf.sourcePercent.getOrElse(1.0)

  // If not specified, half the source are updates.
  val tableUpdatePercent = conf.tableUpdatePercent.getOrElse(sourcePercent / 2)

  // partitioning info
  val partitionTables = conf.partitionTables
  val partitionCount = conf.partitionCount
  val mergeRowLimit = conf.mergeRowLimit

  lazy val benchmarkTableName = s"benchmarkTable_src_${toStr(sourcePercent)}_upd_${toStr(tableUpdatePercent)}"

  override protected def runInternal(): Unit = {
    // print all arguments
    log(s"Running with arguments: ${conf.toString}")
    log(s"dbName: $dbName")
    log(s"dbLocation: $dbLocation")
    log(s"fullBenchmarkTableName: $fullBenchmarkTableName")
    log(s"benchmarkTableName: $benchmarkTableName")
    log(s"tableLocation: ${tableLocation(benchmarkTableName)}")
    log(s"sourcePercent: $sourcePercent")
    log(s"tableUpdatePercent: $tableUpdatePercent")
    log(s"partitionTables: $partitionTables")
    log(s"partitionCount: $partitionCount")
    log(s"mergeRowLimit: $mergeRowLimit")

    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${dbName} LOCATION '${dbLocation}'")
    spark.sql(s"USE ${dbName}")

    val numRows = conf.sizeInGB * numRowsPerGB
    val numFiles = math.max(1, (conf.sizeInGB / targetFileSizeInGB).toInt)

    // run_cluster_warmup()
    createBenchmarkTable(numRows = numRows, numFiles = numFiles)
    runReadQueries("init")
    printTableSize()

    runMerge(sourcePercent = sourcePercent, tableUpdatePercent = tableUpdatePercent)
    runReadQueries("post-merge")
    printTableSize()
    // runCompaction()
    // runReadQueries("post-compaction")

    printTableHistory()
  }

  // ==================== Utility functions ====================

  def createBenchmarkTable(numRows: Long, numFiles: Int): Unit = {
    createData(start = 0, end = numRows, numSplits = numFiles).createOrReplaceTempView("baseTableData")
    val benchmarkTableLocation = tableLocation(benchmarkTableName)

    runQuery(s"DROP TABLE IF EXISTS $fullBenchmarkTableName", s"drop-table-$fullBenchmarkTableName")
    val tableOptions = if (conf.formatName == "hudi") {
      s"OPTIONS ( type = '${conf.tableType}', primaryKey = 'key' )"
    } else ""
    val tableProps =  if (conf.formatName == "iceberg" && conf.tableType != "cow") {
      s"TBLPROPERTIES ( 'write.merge.mode' = 'merge-on-read',  'format-version'='2')"
    } else ""
    val partitionBy = if (partitionTables) "PARTITIONED BY (partition)" else ""
    val orderBy = if (partitionTables) "ORDER BY partition" else ""

    runQuery(s"""
      CREATE TABLE $fullBenchmarkTableName
      USING ${conf.formatName}
      $partitionBy
      $tableOptions
      $tableProps
      LOCATION '$benchmarkTableLocation'
      AS SELECT * FROM baseTableData
      $orderBy
      """,
      s"create-table-$fullBenchmarkTableName",
      ignoreError = false)
  }

  def runMerge(sourcePercent: Double, tableUpdatePercent: Double): Unit = {
    assert(tableUpdatePercent <= sourcePercent)

    val mergeSourceTableName = "mergeSource"
    createMergeSourceData(sourcePercent, tableUpdatePercent)
      .createOrReplaceTempView("mergeSourceData")

    runQuery(s"DROP TABLE IF EXISTS $mergeSourceTableName",
      queryName = s"drop-$mergeSourceTableName",
      ignoreError = false)
    runQuery(
      s"""
         |CREATE TABLE $mergeSourceTableName
         |USING parquet
         |LOCATION '${tableLocation(mergeSourceTableName)}'
         |AS SELECT * FROM mergeSourceData""".stripMargin,
      queryName = s"create-$mergeSourceTableName", ignoreError = false)

    val mergeSourceSize = spark.table(mergeSourceTableName).count()
    log(s"Created $mergeSourceTableName with $mergeSourceSize rows")
    runQuery(
      s"""
         |MERGE INTO $fullBenchmarkTableName AS t
         |USING $mergeSourceTableName AS s
         |ON t.key = s.key
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin,
      queryName = s"merge-source-${toStr(sourcePercent)}%-update-${toStr(tableUpdatePercent)}%",
      ignoreError = false
    )
    log("Merge done")
  }

  def runCompaction(): Unit = {
    if (conf.formatName == "delta") {
      runQuery(s"OPTIMIZE $fullBenchmarkTableName", queryName = "compact")
    } else if (conf.formatName == "iceberg") {
      runQuery(s"CALL spark_catalog.system.rewrite_data_files('$fullBenchmarkTableName')", queryName = "compact")
    }
  }

  def createData(start: Long, end: Long, numSplits: Int): DataFrame = {
    log(s"Generating data from $start to $end with $numSplits splits")
    spark.range(start, end, 1,  numSplits).toDF("key")
      .selectExpr("key", "(key * 7.1 + 13.4) AS value", "cast((key * 13 * 7) AS STRING) AS value2", "key % 1000 AS partition")
  }

  def createMergeSourceData(sourcePercent: Double, targetUpdatePercent: Double): DataFrame = {
    import spark.implicits._
    assert(targetUpdatePercent <= sourcePercent)

    val (maxKey, targetTotalRowCount) = spark.table(benchmarkTableName)
      .agg(max($"key"), count("*")).as[(Long, Long)].collect().head
    val sourceRowCount = (sourcePercent * targetTotalRowCount / 100).toLong
    val sourceSize = sourceRowCount / numRowsPerGB
    val numSourceSplits = math.max((sourceSize / targetFileSizeInGB).toInt, 1)

    val updateRowCount = (targetTotalRowCount * targetUpdatePercent / 100).toLong
    assert(updateRowCount <= sourceRowCount, "more rows being updated than target source size")
    val insertRowCount = sourceRowCount - updateRowCount
    log(s"Benchmark table has ${targetTotalRowCount} rows")
    log(s"Generating mergeSource data with target ${sourceRowCount} rows, approx $updateRowCount updates and $insertRowCount inserts")
    val insertRows = createData(start = maxKey + 1, end = maxKey + 1 + insertRowCount, numSplits = numSourceSplits)
    val updateRows = spark.table(benchmarkTableName)
      .select(insertRows.schema.fieldNames.map(name => col(name)): _*)
      .filter((rand() * 10000) <= targetUpdatePercent * 100)
    insertRows.union(updateRows)
  }

  def runReadQueries(querySetName: String): Unit = {
    val queries = Seq(
      "1. full-count" -> s"SELECT COUNT(*) FROM $fullBenchmarkTableName",
      "2. filter-by-value-count" -> s"SELECT COUNT(*) FROM $fullBenchmarkTableName WHERE key IS NOT NULL",
      "3. full-scan" -> s"SELECT SUM(key), SUM(value), SUM(CAST(value2 AS DOUBLE)) FROM $fullBenchmarkTableName"
    )

    for (i <- 1 to numReadQueryIterations) {
      queries.foreach { case (name, query) =>
        runQuery(query, queryName = s"read-$querySetName-$name", iteration = Some(i))
      }
    }
  }

  def printTableSize(): Unit = {
    if (conf.formatName == "delta") {
      runQuery(s"DESCRIBE DETAIL $fullBenchmarkTableName", queryName = "details", printRows = true)
    } else if (conf.formatName == "iceberg") {
      runQuery(s"SELECT count(*), sum(file_size_in_bytes) FROM spark_catalog.$fullBenchmarkTableName.files", queryName = "details", printRows = true)
    }
  }

  def printTableHistory(): Unit = {
    if (conf.formatName == "delta") {
      runQuery(s"DESCRIBE HISTORY $fullBenchmarkTableName", queryName = "history", printRows = true)
    } else if (conf.formatName == "iceberg") {
      runQuery(s"SELECT * FROM ice.${fullBenchmarkTableName}.snapshots", queryName = s"history", printRows = true)
    }
  }


  def run_cluster_warmup(): Unit = {
    val warmupTableName = s"warmup_table_${conf.formatName}"
    val fullWarmupTableName = s"$dbName.$warmupTableName"
    val testTableLocation = s"${dbLocation}/${warmupTableName}__${benchmarkId.replace("-", "_")}/"
    val numIterations = 3
    runFunc("warmup") {
      log(s"Warmup starting by $numIterations times creating table ${fullWarmupTableName} at $testTableLocation")
      for (i <- 1 to numIterations) {
        log(s"\tWarmup iteration ${i}")
        spark.range(0, 100000000, 1, numPartitions = partitionCount).toDF("value")
          .write.mode("overwrite").format(conf.formatName)
          .option("path", testTableLocation)
          .saveAsTable(fullWarmupTableName)
        spark.table(fullWarmupTableName).filter("value is not null").count()
      }
      log("Warmup done")
    }
  }

  val df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
  df.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

  def toStr(d: Double) = df.format(d).replace(".", "_") + "_pct"
}


object MergeMicroBenchmark {
  def main(args: Array[String]): Unit = {
    MergeMicroBenchmarkConf.parse(args).foreach { conf =>
      new MergeMicroBenchmark(conf).run()
    }
  }
}
