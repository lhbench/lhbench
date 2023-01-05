package benchmark

import java.time.LocalTime

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.SparkContext


case class FileCountBenchmarkConf(
  protected val format: Option[String] = None,
  scaleInGB: Int = 0,
  sourceTablePath: Option[String] = None,
  benchmarkPath: Option[String] = Some(s"s3://databricks-tdas/cidr-benchmarks/"),
  benchmarkTablePath: Option[String] = None,
  numFiles: Option[Int] = None,
  loadMode: String = "loadIfNeeded",
  override val userDefinedDbName: Option[String] = None,
  profile: Boolean = false,
  customSortedTable: Boolean = false,
  debugLogs: Boolean = false
) extends TPCDSConf {
  override def dbName: String = userDefinedDbName.getOrElse(s"file_count_benchmark")
  override def dbLocation: String = dbLocation(dbName)
}

object FileCountBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[FileCountBenchmarkConf]
  private val argParser = {
    import builder._

    OParser.sequence(
      programName("FileCountMicroBenchmark"),
      opt[String]("scale-in-gb")
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the TPCDS benchmark"),
      opt[String]("num-files")
        .required()
        .action((x, c) => c.copy(numFiles = Some(x.toInt))),
      opt[String]("format")
        .required()
        .valueName("<delta|iceberg|hudi>")
        .action((x, c) => c.copy(format = Some(x)))
        .text("Spark's short name for the file format to use"),
      opt[String]("source-table-path")
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(sourceTablePath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("benchmark-path")
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("benchmark-table-path")
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkTablePath = Some(x)))
        .text("Cloud path of the table to be used for benchmark queries, overrides other paths and table names"),
      opt[String]("loadMode")
        .valueName("<loadIfNeeded|reload|noload>")
        .validate { x =>
          val valid = Set("loadIfNeeded", "reload", "noload")
          if (valid.exists(_.equalsIgnoreCase(x))) success
          else failure("valid values are " + valid)
        }
        .action((x, c) => c.copy(loadMode = x.toLowerCase))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[Unit]("profile")
        .action((x, c) => c.copy(profile = true))
        .text("Whether to optimize the benchmark for frame profiling"),
      opt[Unit]("sorted")
        .action((x, c) => c.copy(customSortedTable = true))
        .text("Whether to use custom sorted table for high data skipping"),
      opt[Unit]("debug")
        .action((x, c) => c.copy(debugLogs = true))
        .text("Whether to use custom sorted table for high data skipping"),

    )
  }

  def parse(args: Array[String]): Option[FileCountBenchmarkConf] = {
    OParser.parse(argParser, args, FileCountBenchmarkConf())
  }
}

object FileCountBenchmark {
  def main(args: Array[String]): Unit = {
    FileCountBenchmarkConf.parse(args).foreach { conf =>
      new FileCountBenchmark(conf).run()
    }
  }
}


class FileCountBenchmark(conf: FileCountBenchmarkConf) extends Benchmark(conf) {

  val numFiles = conf.numFiles.getOrElse {
    throw new IllegalArgumentException("num files not specified")
  }
  val useCustomSortedTable = conf.customSortedTable
  val (dbName, tableName) = if (useCustomSortedTable) {
    (s"${conf.dbName}_custom_${numFiles}_files_${conf.formatName}", "table_sorted")
  } else {
    if (conf.scaleInGB == 0) {
      throw new IllegalArgumentException("scale in GB not specified")
    }
    (
      s"${conf.dbName}_tpcds_sf${conf.scaleInGB}_${numFiles}_files_${conf.formatName}",
      "store_sales"
    )
  }
  val fullTableName = s"`$dbName`.`$tableName`"

  lazy val dbLocation = conf.dbLocation(dbName)
  lazy val tableLocation = s"${dbLocation}/${tableName}__${benchmarkId.replace("-", "_")}/"

  override protected def runInternal(): Unit = {

    val shouldRunLoad = conf.loadMode match {
      case "noload" =>
        log(s"Skipping any attempt to load table")
        false
      case "reload" =>
        log(s"Reloading table")
        true
      case _ =>
        val allTables = spark.sql("SHOW TABLES")
        allTables.show(truncate = false)
        val tableExists = allTables.where(s"tableName = '${tableName}'").count() > 0
        log(s"Table ${fullTableName} exists = ${tableExists}, no loading table")
        !tableExists
    }

    if (shouldRunLoad) {
      runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", ignoreError = false)
      runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName} LOCATION '${dbLocation}'", s"create-database", ignoreError = false)
      runQuery(s"USE ${dbName}", "use-database", ignoreError = false)

      if (useCustomSortedTable) {
        runLoadCustomSortedTable()
      } else {
        runLoadTpcdsStoreSalesTable()
      }
    } else {
      run_cluster_warmup(dbName)
    }

    val tableToRead = conf.benchmarkTablePath
      .map { path => s"delta.`$path`"}
      .getOrElse(fullTableName)

    if (conf.formatName == "delta") {
      runDescribeDetail(tableToRead)
    }

    runReadQueries(tableToRead)
  }

  def runLoadTpcdsStoreSalesTable(): Unit = {
    val sourceTablePath = conf.sourceTablePath.getOrElse {
      s"s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf${conf.scaleInGB}_parquet/"
    }

    val partitionedBy = if (conf.formatName == "hudi") {
      "PARTITIONED BY (ss_sold_date_sk)"
    } else ""

    val tableOptions = if (conf.formatName == "hudi")  {
      s"OPTIONS ( type = 'cow', primaryKey = 'ss_item_sk' )"
    } else ""

    val sourceDF = spark.read.format("parquet").load(sourceTablePath)
      .where("ss_sold_date_sk IS NOT NULL")
    val repartitionedSourceDF = if (conf.formatName == "hudi")  {
      sourceDF
    } else {
      sourceDF.repartition(numFiles)
    }
    repartitionedSourceDF.createOrReplaceTempView("repartitionedData")

    runQuery(s"DROP TABLE IF EXISTS $fullTableName", s"drop-table")

    runQuery(s"""CREATE TABLE $fullTableName
                   USING ${conf.formatName}
                   $partitionedBy $tableOptions
                   LOCATION '$tableLocation'
                   SELECT * FROM repartitionedData
                """, "create-table", ignoreError = false)
  }

  def runLoadCustomSortedTable(): Unit = {
    log(s"Loading custom sorted table '$fullTableName' at location '$tableLocation'")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val numRecordsPerFile = 166666L  // create 10MB files
    val numKeys = numFiles * numRecordsPerFile
    runQuery(s"DROP TABLE IF EXISTS $fullTableName")

    runFunc("create-table") {
      // Create a table with # partitions = # files needed, that is, 1 file per partition.
      // Generate the sorted data with # splits = # files, such that each task writes out
      // 1 file in 1 partition.
      spark.range(0, numKeys, 1, numFiles)
        .withColumn("ss_item_sk", $"id")
        .withColumn("ss_sold_date_sk",  ($"id" / numRecordsPerFile).cast("bigint")) // partition column
        .withColumn("ss_sold_time_sk", $"id" * 23 + 7)
        .withColumn("ss_customer_sk", $"id" % 123)
        .withColumn("ss_cdemo_sk", $"id" % 37)
        .withColumn("ss_hdemo_sk", $"id" % 51)
        .withColumn("ss_addr_sk", $"id" % 117)
        .withColumn("ss_store_sk", $"id" % 13)
        .withColumn("ss_promo_sk", $"id" % 511)
        .withColumn("ss_quantity", $"id" % 7)
        .withColumn("ss_ticket_number", ($"id" * 91 + 23).cast("bigint"))
        .withColumn("ss_quantity", $"id" % 7)
        .withColumn("ss_wholesale_cost", (randn() * 1000).cast("decimal(7,2)"))
        .withColumn("ss_list_price", (randn() * 2000).cast("decimal(7,2)"))
        .withColumn("ss_wholesale_cost", (randn() * 3000).cast("decimal(7,2)"))
        .withColumn("ss_sales_price", (randn() * 4000).cast("decimal(7,2)"))
        .withColumn("ss_ext_discount_amt", (randn() * 5000).cast("decimal(7,2)"))
        .withColumn("ss_ext_sales_price", (randn() * 6000).cast("decimal(7,2)"))
        .withColumn("ss_ext_wholesale_cost", (randn() * 7000).cast("decimal(7,2)"))
        .withColumn("ss_ext_list_price", (randn() * 8000).cast("decimal(7,2)"))
        .withColumn("ss_ext_tax", (randn() * 9000).cast("decimal(7,2)"))
        .withColumn("ss_coupon_amt", (randn() * 10000).cast("decimal(7,2)"))
        .withColumn("ss_net_paid", (randn() * 11000).cast("decimal(7,2)"))
        .withColumn("ss_net_paid_inc_tax", (randn() * 12000).cast("decimal(7,2)"))
        .withColumn("ss_net_profit", (randn() * 13000).cast("decimal(7,2)"))
        .drop("id")
        // Sort within partition by the partition column ss_sold_date_sk just for Iceberg as it needs so.
        // In addition, sort by ss_item_sk, to create good file stats with disjoint min/max ranges
        .sortWithinPartitions("ss_sold_date_sk", "ss_item_sk")
        .write
        .format(conf.formatName)
        .partitionBy("ss_sold_date_sk")
        .option("path", tableLocation)
        .saveAsTable(fullTableName)
    }
  }

  def runReadQueries(tableName: String): Unit = {
    val numIterations = if (conf.profile) 10 else 4
    val numInitIterationsForProfiling = 3
    val queries = Seq(
      "1. select-limit-1" -> s"SELECT * FROM $tableName LIMIT 1",
      "2. full-count" -> s"SELECT COUNT(*) FROM $tableName",
      "3. filter-by-partition" -> s"SELECT SUM(ss_quantity) FROM $tableName WHERE ss_sold_date_sk = 100",
      "4. filter-by-value" -> s"SELECT SUM(ss_quantity) FROM $tableName WHERE ss_item_sk = 100",
    )
    if (conf.debugLogs) spark.sparkContext.setLogLevel("INFO")
    val queryTimesToSummarize = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    for (i <- 1 to numIterations) {
      if (conf.profile && i == numInitIterationsForProfiling + 1) {
        log("\n\n" + ("=" * 40) + "\nStart the profiler\n" + ("=" * 40))
        Thread.sleep(1000 * 60)
      }
      queries.foreach { case (name, query) =>
        val queryPlanningTimeMs = measureQueryPlanningTimeMs(queryName = name) {
          runQuery(query, queryName = name, iteration = Some(i), ignoreError = false)
        }
        val queryTimeMs = getQueryResults().last.durationMs.get
        reportQueryResult(s"$name--planning", queryPlanningTimeMs, iteration = Some(i))

        if (i > 1) {
          queryTimesToSummarize += (s"$name--warm" -> queryTimeMs)
          queryTimesToSummarize += (s"$name--planning-warm" -> queryPlanningTimeMs)
        }
      }
      if (conf.profile && i == numIterations) {
        log("\n\n" + ("=" * 40) + "\nStop the profiler\n" + ("=" * 40))
        Thread.sleep(1000 * 60)
      }
    }
    val allMedians = queryTimesToSummarize.groupBy(_._1).mapValues(x => median(x.map(_._2))).toSeq
    val (queryPlanningTimeMedians, queryTimeMedians) = allMedians.partition(_._1.contains("planning"))
    Seq(queryTimeMedians, queryPlanningTimeMedians).foreach { medians =>
      medians.sortBy(_._1).foreach { case (name, duration) => reportQueryResult(s"$name-median", duration) }
    }
  }

  def runDescribeDetail(tableName: String): Unit = {
    runQuery(s"DESCRIBE DETAIL $tableName", queryName = "details", printRows = true)
  }

  def run_cluster_warmup(dbName: String): Unit = {
    val warmupTableName = s"warmup_table_${conf.formatName}"
    val fullWarmupTableName = s"$dbName.$warmupTableName"
    val testTableLocation = s"${dbLocation}/${warmupTableName}__${benchmarkId.replace("-", "_")}/"
    val numIterations = 5
    runFunc("warmup") {
      spark.sql(s"DROP TABLE IF EXISTS $fullWarmupTableName")
      log(s"Warmup starting by $numIterations times creating table ${fullWarmupTableName} at $testTableLocation")
      for (i <- 1 to numIterations) {
        log(s"\tWarmup iteration ${i}")
        spark.range(0, 100000000, 1, numPartitions = 1000).toDF("value")
          .write.mode("overwrite").format(conf.formatName)
          .option("path", testTableLocation)
          .saveAsTable(fullWarmupTableName)
        spark.table(fullWarmupTableName).filter("value is not null").count()
      }
      log("Warmup done")
    }
  }

  class DataProcessingnStartListener(expectedJobName: String) extends SparkListener {
    @volatile var processingStartTimeMs = -1L
    @volatile var processingStartTime: Option[LocalTime] = None
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      val jobId = jobStart.jobId
      val jobDesc = jobStart.properties.getProperty("spark.job.description", "")
      if (conf.debugLogs) log(s"DataProcessingnStartListener: Job $jobId [$jobDesc] started, props: " + jobStart.properties)
      if (jobDesc.startsWith(expectedJobName) && processingStartTimeMs < 0) {
        processingStartTimeMs = System.currentTimeMillis()
        processingStartTime = Some(LocalTime.now())
      }
    }
  }

  def measureQueryPlanningTimeMs(queryName: String)(f: => Unit): Long = {
    val listener = new DataProcessingnStartListener(queryName)
    spark.sparkContext.addSparkListener(listener)
    val queryStartTimeMs = System.currentTimeMillis()
    val queryStartTime = LocalTime.now()
    try {
      f
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    log(s"QueryPlanningMeasurement: Query started at $queryStartTime [$queryStartTimeMs ms], " +
      s"data job started at ${listener.processingStartTime} [${listener.processingStartTimeMs} ms]")
    if (listener.processingStartTimeMs > 0) {
      val ms = listener.processingStartTimeMs - queryStartTimeMs
      log(s"QueryPlanningMeasurement: Query planning took ${ms} ms")
      ms
    } else -1L
  }
}
