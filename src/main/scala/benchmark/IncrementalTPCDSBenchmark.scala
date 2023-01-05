package benchmark

import benchmark.TPCDSDataLoad.{tableColumnSchemas, tablePartitionKeys, tablePrimaryKeys}
import benchmark.TPCDSRefreshSchema.{getViewQuery, viewNames}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import java.util.UUID


case class IncrementalTPCDSBenchmarkConf(
                                          protected val format: Option[String] = None,
                                          scaleInGB: Int = 0,
                                          userDefinedDbName: Option[String] = None,
                                          sourcePath: Option[String] = None,
                                          cachedPath: Option[String] = None,
                                          refreshCount: Int = 0,
                                          benchmarkPath: Option[String] = None,
                                          excludeNulls: Boolean = true,
                                          partitionTables: Boolean = true,
                                          iterations: Int = 3,
                                          tableMode: Option[String] = None,
                                        ) extends TPCDSConf

object IncrementalTPCDSBenchmarkConf {

  import scopt.OParser

  private val builder = OParser.builder[IncrementalTPCDSBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("Incremental Benchmark"),
      opt[String]("format")
        .required()
        .valueName("<delta/iceberg/hudi>")
        .action((x, c) => c.copy(format = Some(x)))
        .text("file format to use"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud storage path to be used for creating table and generating reports"),
      opt[String]("refresh-count")
        .optional()
        .valueName("<refresh count>")
        .action((x, c) => c.copy(refreshCount = x.toInt))
        .text("Number of times to refresh the table"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(userDefinedDbName = Some(x)))
        .text("Name of the target database to create with TPC-DS tables in necessary format"),
      opt[Int]("scale")
        .required()
        .valueName("<scale in GB>")
        .action((x, c) => c.copy(scaleInGB = x))
        .text("Scale of the TPC-DS data to be generated"),
      opt[String]("source-path")
        .optional()
        .valueName("<path to the TPC-DS raw input data>")
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("The location of the TPC-DS raw input data"),
      opt[String]("cache-path")
        .optional()
        .valueName("<path to the cached TPC-DS data in parquet format>")
        .action((x, c) => c.copy(cachedPath = Some(x)))
        .text("The location of the cached TPC-DS data"),
      opt[String]("table-mode")
        .optional()
        .valueName("<cow/mor>")
        .action((x, c) => c.copy(tableMode = Some(x)))
        .text("The mode of the table to be created, either copy-on-write or merge-on-read"),
      opt[String]("exclude-nulls")
        .optional()
        .valueName("true/false")
        .action((x, c) => c.copy(excludeNulls = x.toBoolean))
        .text("Whether to remove null primary keys when loading data, default = false"),
      opt[String]("partition-tables")
        .optional()
        .valueName("true/false")
        .action((x, c) => c.copy(partitionTables = x.toBoolean))
        .text("Whether to partition the tables, default = true"),
      opt[String]("iterations")
        .optional()
        .valueName("<number of iterations>")
        .action((x, c) => c.copy(iterations = x.toInt))
        .text("Number of times to run the queries"),
    )
  }

  def parse(args: Array[String]): Option[IncrementalTPCDSBenchmarkConf] = {
    OParser.parse(argParser, args, IncrementalTPCDSBenchmarkConf())
  }
}

class IncrementalTPCDSBenchmark(conf: IncrementalTPCDSBenchmarkConf) extends Benchmark(conf) {
  lazy val baseTablePaths: Map[String, String] = conf.scaleInGB match {
    case 3000 => tables.map(table => {
      table -> s"s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf${conf.scaleInGB}_parquet/$table"
    }).toMap
    case _ => cacheBaseTables.toMap
  }
  lazy val refreshTablePaths: IndexedSeq[Map[String, String]] = cacheRefreshTables(baseTablePaths)
  val dbName: String = conf.dbName
  val dbLocation: String = conf.dbLocation(dbName, suffix = benchmarkId.replace("-", "_"))
  val dbCatalog = "spark_catalog"
  val tableType: String = conf.tableMode.getOrElse("cow")
  val partitionTables = conf.partitionTables
  val primaryKeys = true
  val refreshSchema: Seq[Table] = TPCDSRefreshSchema.tables(spark.sqlContext)
  val tables: Seq[String] = TPCDSDataLoad.tableNamesTpcds

  // val filteredTablesBase = Seq("inventory")
  // val filteredTablesMerge = Seq("inventory")
  // val filteredQueries = Seq()
  val filteredTablesBase = tables
  val filteredTablesMerge = tables
  val filteredQueries = Seq("q3", "q9", "q34", "q42", "q59")

  def runInternal(): Unit = {
    // print all arguments
    println("Arguments:")
    println(s"dbName: $dbName")
    println(s"dbLocation: $dbLocation")
    println(s"dbCatalog: $dbCatalog")
    println(s"tableType: $tableType")
    println(s"partitionTables: $partitionTables")
    println(s"primaryKeys: $primaryKeys")
    println(s"filteredTablesBase: $filteredTablesBase")
    println(s"filteredTablesMerge: $filteredTablesMerge")
    println(s"filteredQueries: $filteredQueries")

    // create database
    runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", s"prep:drop-database")
    runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName}", s"prep:create-database", ignoreError = false)
    runQuery(s"USE ${dbName}", "prep:use-database")
    runQuery("SHOW TABLES", printRows = true, queryName = s"prep:setup-show-tables")

    // create all tables
    baseTablePaths
      .filter { case (table, _) => filteredTablesBase.contains(table) }
      .foreach { case (table, basePath) => createTable(table, basePath) }

    runQuery(s"USE ${dbCatalog}.${dbName};", s"prep:setup-use-database")
    runQuery("SHOW TABLES", printRows = true, queryName = s"prep:setup-show-tables")

    // compact after initial load
    printTableStats(filteredTablesBase)
    compact("load", filteredTablesBase)
    printTableStats(filteredTablesBase)

    // run queries
    runTPCDSQueries(1, Some(filteredQueries))
    if (conf.formatName == "hudi" && tableType == "mor") {
      runTPCDSQueries(1, Some(filteredQueries), Some("_rt"))
      runTPCDSQueries(1, Some(filteredQueries), Some("_ro"))
    }

    // run refreshes
    for (i <- 1 to conf.refreshCount) {
      refreshTablePaths(i - 1)
        .filter { case (table, _) => filteredTablesMerge.contains(table) }
        .foreach { case (table, basePath) => runMergeIteration(i, table, basePath) }
    }
    printTableStats(filteredTablesMerge)

    // run query before compaction
    runTPCDSQueries(2, Some(filteredQueries))
    if (conf.formatName == "hudi" && tableType == "mor") {
      runTPCDSQueries(2, Some(filteredQueries), Some("_rt"))
      runTPCDSQueries(2, Some(filteredQueries), Some("_ro"))
    }

    compact("merge", filteredTablesMerge)
    printTableStats(filteredTablesMerge)

    // // run query after compaction
    // runTPCDSQueries(3, Some(filteredQueries))
    // if (conf.formatName == "hudi" && tableType == "mor") {
    //   runTPCDSQueries(3, Some(filteredQueries), Some("_rt"))
    //   runTPCDSQueries(3, Some(filteredQueries), Some("_ro"))
    // }
  }

  protected def compact(message: String, tables: Seq[String]): Unit = {
    if (conf.formatName == "delta" || conf.formatName == "iceberg") {
      tables
        .filter(table => tables.contains(table))
        .foreach(tableName => {
          runQuery(conf.formatName match {
            case "delta" => s"OPTIMIZE delta.`${dbLocation}/$tableName`"
            case "iceberg" => s"CALL ${dbCatalog}.system.rewrite_data_files('${dbName}.$tableName')"
          }, s"compact_$message:$tableName")
        })
    } else {
      tables
        .filter(table => tables.contains(table))
        .foreach(tableName => {
          runQuery(s"CALL ${dbCatalog}.system.run_compaction('run', '${dbName}.$tableName')",
            s"compact_$message:$tableName")
        })
    }
  }

  protected def printTableStats(tables: Seq[String]): Unit = {
    tables
      .foreach(tableName => {
        if (conf.formatName == "iceberg") {
          runQuery(s"SELECT * FROM ice.${dbName}.$tableName.snapshots",
            printRows = true, queryName = s"prep:file-stats-$tableName")
        } else if (conf.formatName == "delta") {
          runQuery(s"DESCRIBE history delta.`${dbLocation}/$tableName`",
            printRows = true, queryName = s"prep:file-stats-$tableName")
        }
      })
  }

  /** main benchmark stages */

  protected def createTable(tableName: String, parquet_path: String): Unit = {
    val targetLocation = osPathJoin(dbLocation, tableName)

    // create schema
    runQuery(s"DROP TABLE IF EXISTS $dbName.`$tableName`", s"createtable:drop-table-$tableName")
    val partitionedByCreate =
      if (!partitionTables || tablePartitionKeys(tableName).head.isEmpty) ""
      else "PARTITIONED BY " + tablePartitionKeys(tableName).mkString("(", ", ", ")")
    val tableOptionsCreate = if (conf.formatName == "hudi") {
      if (!primaryKeys || tablePrimaryKeys(tableName).head.isEmpty) s"OPTIONS ( type = '$tableType')"
      else s"OPTIONS (type = '$tableType', primaryKey = " + tablePrimaryKeys(tableName).mkString("'", ",", "')")
    } else {
      ""
    }
    val tablePropsCreate = if (conf.formatName == "iceberg" && tableType == "mor") {
      s"TBLPROPERTIES ( 'write.merge.mode' = 'merge-on-read',  'format-version'='2')"
    } else ""
    val excludeNulls =
      if (!partitionTables || tablePartitionKeys(tableName).head.isEmpty) ""
      else "WHERE " + tablePartitionKeys(tableName).head + " IS NOT NULL"
    // if partitioned and conf.formatName == "iceberg", then we must sort the view by partition keys
    // https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables
    val sortIceberg = {
      if (partitionTables && tablePartitionKeys(tableName).head.nonEmpty) {
        s"ORDER BY ${tablePartitionKeys(tableName).mkString(", ")}"
      } else {
        ""
      }
    }
    runQuery(
      s"""CREATE TABLE $dbName.`$tableName`
          USING ${conf.formatName}
          $partitionedByCreate $tableOptionsCreate $tablePropsCreate
          LOCATION '$targetLocation'
          SELECT * FROM parquet.`$parquet_path` $excludeNulls $sortIceberg""".stripMargin,
      s"create:$tableName", ignoreError = false)
  }

  protected def runMergeIteration(i: Int, table: String, refreshPath: String): DataFrame = {
    val excludeNulls = if (!partitionTables || tablePartitionKeys(table).head.isEmpty) "" else "WHERE " + tablePartitionKeys(table).head + " IS NOT NULL"
    // https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables
    val sortIceberg = if (partitionTables && tablePartitionKeys(table).head.nonEmpty) {
      s"ORDER BY ${tablePartitionKeys(table).mkString(", ")}"
    } else ""

    val viewName = s"refresh_${table}_$i"
    val tempViewName = s"temp_${i}_${viewName}_${UUID.randomUUID().toString.replace("-", "")}"
    runQuery(
      s"""CREATE OR REPLACE TEMPORARY VIEW $tempViewName
          AS SELECT * FROM parquet.`$refreshPath` $excludeNulls $sortIceberg""".stripMargin,
      s"create:$tempViewName", ignoreError = false)
    // get list of column names from schema
    val colNames = spark.table(tempViewName).columns
    val mergeOn = Seq((tablePrimaryKeys(table).filter(_.nonEmpty) ++ tablePartitionKeys(table).filter(_.nonEmpty)): _*).toSeq
    val merge_query =
      s"""MERGE INTO $dbName.`$table` AS target
          USING $tempViewName AS source
          ON ${mergeOn.map(pk => s"target.$pk = source.$pk").mkString(" AND ")}
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""".stripMargin
    runQuery(merge_query, s"merge:$i:${table}", ignoreError = false)
    runQuery(s"DROP VIEW IF EXISTS $tempViewName", s"drop:$tempViewName")
  }

  protected def runTPCDSQueries(query_iteration: Int, filteredQueries: Option[Seq[String]] = None, hudi_table_postfix: Option[String] = None): Unit = {
    val fullQueryMap: Map[String, String] = {
      if (conf.scaleInGB <= 3000) TPCDSBenchmarkQueries.TPCDSQueries3TB
      else if (conf.scaleInGB == 10000) TPCDSBenchmarkQueries.TPCDSQueries10TB
      else throw new IllegalArgumentException(
        s"Unsupported scale factor of ${conf.scaleInGB} GB")
    }

    val queriesFiltered = filteredQueries match {
      case Some(q) => fullQueryMap.filterKeys(q.contains)
      case None => fullQueryMap
    }

    // rewrite queries so that c_last_review_date becomes c_last_review_date_sk
    val queries = queriesFiltered.map { case (name, sql) =>
      val sqlRewritten = sql.replaceAll("c_last_review_date", "c_last_review_date_sk")
      (name, sqlRewritten)
    }

    // hudi mor requires all tables be postfixed with _rt, substitute table names here
    val queriesSubstituted = hudi_table_postfix match {
      case Some(x) => {
        conf.formatName match {
          case "hudi" if tableType == "mor" =>
            queries
              .map { case (name, sql) =>
                var sqlSub = sql
                tables.foreach { table =>
                  sqlSub = sqlSub.replaceAll(s"\\b${table}\\b", s"$table$x")
                }
                (name, sqlSub)
              }
          case _ => queries
        }
      }
      case None => queries
    }

    if (queries.nonEmpty) {
      spark.conf.set("spark.sql.broadcastTimeout", "7200")
      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      spark.sparkContext.setLogLevel("WARN")
      log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))
      runQuery(s"USE ${dbCatalog}.${dbName}", s"prepare-query${query_iteration}:use-db")

      for (iteration <- 1 to conf.iterations) {
        val postfix = hudi_table_postfix.getOrElse("")
        queriesSubstituted
          .toSeq
          .sortBy(_._1)
          .foreach { case (name, sql) =>
            runQuery(sql, iteration = Some(iteration), queryName = s"query${query_iteration}${postfix}:$name")
          }
      }
      // val results = getQueryResults().filter(_.name.startsWith(s"query${query_iteration}:q"))
      // if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty)) {
      //   val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
      //     assert(results.length == conf.iterations)
      //     val medianMs = results.map(_.durationMs.get).sorted
      //       .drop(math.floor(conf.iterations / 2.0).toInt).head
      //     (q, medianMs / 1000.0)
      //   }
      //   val sumOfMedians = medianDurationSecPerQuery.values.sum
      //   reportExtraMetric(s"tpcds-result-seconds-$query_iteration", sumOfMedians)
      // }
    }
  }

  /** load tables */

  protected def cacheBaseTables: Seq[(String, String)] = {
    log("Parsing and caching base data as parquet files")
    val baseTablePaths = tables.map { table =>
      val basePath = osPathJoin(conf.cachedPath.get, s"${conf.scaleInGB}gb", table, "base") + "/"
      if (!parquetPathExists(basePath)) {
        log(s"\tTable $table is not cached at $basePath, caching now")
        val df = loadTable(Left(table), "base")
        val partition_keys: Seq[String] = tablePartitionKeys.getOrElse(table, Seq())
        val writer = if (partitionTables && partition_keys.nonEmpty && partition_keys.head != "") {
          df.repartition(partition_keys.map(col): _*)
            .write
            .format("parquet")
            .mode("overwrite")
            .partitionBy(partition_keys: _*)
        } else {
          df.write
            .format("parquet")
            .mode("overwrite")
        }
        writer.save(basePath)
      } else {
        log(s"\tTable $table is cached at $basePath")
      }
      table -> basePath
    }
    baseTablePaths
  }

  protected def cacheRefreshTables(baseTablePaths: Map[String, String]): IndexedSeq[Map[String, String]] = {
    (1 to conf.refreshCount).map { i =>
      log(s"Parsing and caching refresh data as parquet files for iteration $i of ${conf.refreshCount}")
      val paths = viewNames
        .map(viewBaseName => {
          val factTable = TPCDSRefreshSchema.map_view_to_dest(viewBaseName)
          factTable -> (osPathJoin(conf.cachedPath.get, s"${conf.scaleInGB}gb", factTable, s"refresh$i") + "/")
        }).toMap
      val pathStatus = paths.map { case (table, path) => table -> parquetPathExists(path) }
      if (!pathStatus.forall(_._2)) {
        refreshSchema
          .map { table => table.name -> loadTable(Right(table), s"refresh$i", file_suffix = s"_$i") }
          .foreach { case (table, df) => df.createOrReplaceTempView(table) }
        baseTablePaths
          .foreach { case (table, basePath) =>
            val df = spark.read.parquet(basePath)
            df.createOrReplaceTempView(table)
          }
        viewNames
          .foreach(viewBaseName => {
            val factTable = TPCDSRefreshSchema.map_view_to_dest(viewBaseName)
            val path = osPathJoin(conf.cachedPath.get, s"${conf.scaleInGB}gb", factTable, s"refresh$i") + "/"
            if (!pathStatus(factTable)) {
              log(s"Table $factTable is not cached at $path, caching now")
              spark.sql(getViewQuery(viewBaseName, i))
                .write.format("parquet").mode("overwrite").save(path)
            } else {
              log(s"\tTable $factTable is cached at $path")
            }
          })
        refreshSchema.foreach(x => spark.catalog.dropTempView(x.name))
        baseTablePaths.foreach { case (table, _) => spark.catalog.dropTempView(table) }
      } else {
        log(s"\tAll tables are cached")
      }
      paths
    }
  }

  protected def loadTable(tableSchema: Either[String, Table], revision: String = "base", file_prefix: String = "", file_suffix: String = ""): DataFrame = {
    val tableName = tableSchema match {
      case Left(name) => name
      case Right(t) => t.name
    }
    val banner = s"parse-dsdgen-${tableName}-$revision"
    spark.sparkContext.setJobGroup(banner, banner, interruptOnCancel = true)
    val table_path = osPathJoin(conf.sourcePath.get, revision, s"${file_prefix}${tableName}${file_suffix}.dat")
    val reader = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "|")
    val df = tableSchema match {
      case Left(tableName) =>
        var schema_string = tableColumnSchemas(tableName)
        schema_string = schema_string.replaceAll("char\\([0-9]+\\)", "string")
        schema_string = schema_string.replaceAll("varstring", "string")
        reader.schema(schema_string).load(table_path)
      case Right(table) => reader.schema(table.schema).load(table_path)
    }
    spark.sparkContext.clearJobGroup()
    df
  }

  /** utils */
  protected def parquetPathExists(s3_directory_prefix: String): Boolean = {
    try {
      val s3Path = new Path(s3_directory_prefix)
      val s3FileSystem = s3Path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      s3FileSystem.exists(s3Path) || s3FileSystem.listStatus(s3Path).nonEmpty
    } catch {
      case _: Exception => false
    }
  }

  protected def osPathJoin(paths: String*): String = {
    var out = ""
    for (path <- paths) {
      if (out.isEmpty) {
        out = path
      } else if (out.endsWith("/")) {
        out += path
      } else {
        out += "/" + path
      }
    }
    out
  }
}

object IncrementalTPCDSBenchmark {
  def main(args: Array[String]): Unit = {
    println("All command line args = " + args.toSeq)
    IncrementalTPCDSBenchmarkConf.parse(args).foreach { conf =>
      new IncrementalTPCDSBenchmark(conf).run()
    }
  }
}
