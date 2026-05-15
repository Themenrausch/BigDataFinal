package com.tipdm.covid19

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object Redcode {
    def main(args: Array[String]): Unit = {
        // 集群版本只有主函数有修改，其他函数没有动过。
        // 手动指定参数删了，因为这里是外部参数传进来。
        if (args.length != 9) {
            PrintUsage()
            System.exit(1)
        }
        // 集群版本的 SparkSession 创建。
        val spark = SparkSession.builder()
            .appName("Redcode")
            .enableHiveSupport()
            .getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        val io_cdinfo = args(0)
        val io_infected = args(1)
        val io_infected_info = args(2)
        val io_all_timerange = args(3)
        val io_infected_timerange = args(4)
        val io_contacts = args(5)
        val include = if (args(6).toLowerCase() == "include") {
                true
            } else if (args(6).toLowerCase() == "exclude") {
                false
            } else {
                PrintUsage()
                System.exit(1)
                false
            }
        val robust = if (args(7).toLowerCase() == "robust") {
                true
            } else if (args(7).toLowerCase() == "simple") {
                false
            } else {
                PrintUsage()
                System.exit(1)
                false
            }
        val outputPath = args(8)

        val tables = Seq(
            io_infected_info,
            io_all_timerange,
            io_infected_timerange,
            io_contacts
        )
        tables.foreach { table =>
            spark.sql(s"drop table if exists $table purge")
            println(s"Dropped: $table")
        }

        val cdinfo = spark.table(io_cdinfo)
        val infected = spark.table(io_infected)
        val infected_info = cdinfo.join(
            infected.select("phone").distinct(),
            Seq("phone"),
            "left_semi"
        ).cache()

        infected_info.count()

        println("processing infected_info")
        infected_info.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(io_infected_info)
        println("after infected_info")

        spark.catalog.clearCache()

        TimeRange(
            spark=spark,
            inputTable=io_cdinfo,
            includePhone=true,
            outputTable=io_all_timerange,
            robust=robust)

        TimeRange(
            spark=spark,
            inputTable=io_infected_info,
            includePhone=true,
            outputTable=io_infected_timerange,
            robust=robust)

        FindContacts(
            spark=spark,
            inputTableInfected=io_infected_timerange,
            inputTableAll=io_all_timerange,
            inputTableInfo=io_infected,
            includeInfected=include,
            outputTable=io_contacts,
            outputPath=outputPath)

        tables.foreach { table =>
            spark.sql("refresh table dingdongji.contacts")
            println("refreshed")
        }
        spark.stop()
    }

    case class RawRecord(
                            phone: Long,
                            cellid: Long,
                            times: Long,
                            register: Int
                        )

    case class IntervalRecord(
                                phone: Long,
                                cellid: Long,
                                enter_time: Long,
                                exit_time: Long
                            )

    def TimeRangeRobust(
                        spark: SparkSession,
                        inputTable: String,
                        includePhone: Boolean = true,
                        outputTable: String
                        ): Unit = {
        import spark.implicits._
        // 读取并转 Dataset
        val ds = spark.table(inputTable)
            .select(
                col("phone").cast("long"),
                col("cellid").cast("long"),
                col("times").cast("long"),
                col("register").cast("int")
            )
            .as[RawRecord]
            .cache()
        ds.count()

        // 分组状态机处理
        val intervalDS = ds
            .groupByKey(r => (r.phone, r.cellid))
            .flatMapGroups {
                case ((phone, cellid), iter) =>
            // 按时间排序
                val rows = iter.toList.sortBy(_.times)
                val result = scala.collection.mutable.ListBuffer[IntervalRecord]()
                var inside = false
                var enterTime: Option[Long] = None
                var lastExitTime: Option[Long] = None
                for (i <- rows.indices) {
                    val row = rows(i)
                    val t = row.times
                    val reg = row.register
                    // ENTER
                    if (reg == 1) {
                    // 不在基站
                        if (!inside) {
                            inside = true
                            enterTime = Some(t)
                            lastExitTime = None
                        }
                        // 连续 enter
                        else {
                            // 忽略
                        }
                    }
                    // EXIT
                    else if (reg == 2) {
                    // 必须已经 inside
                        if (inside) {
                            // 更新最后 exit
                            lastExitTime = Some(t)
                            // 判断是否结束区间
                            val nextIsEnter = if (i == rows.length - 1) {
                                    true
                                } else {
                                    rows(i + 1).register == 1
                                }
                            if (nextIsEnter) {
                                result += IntervalRecord(
                                    phone,
                                    cellid,
                                    enterTime.get,
                                    lastExitTime.get
                                )
                                inside = false
                                enterTime = None
                                lastExitTime = None
                            }
                        }
                    }
                }
            result.iterator
        }
        intervalDS.cache()
        intervalDS.count()
        // 输出
        val resultDF = (if (includePhone) {
                intervalDS.toDF()
            } else {
                intervalDS
                    .drop("phone")
                    .toDF()
            }).cache()
        resultDF.count()

        resultDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        spark.catalog.clearCache()
    }

    def TimeRangeSimple(
                        spark: SparkSession,
                        inputTable: String,
                        includePhone: Boolean = true,
                        outputTable: String
                        ): Unit = {
        val df = spark.table(inputTable).cache()
        df.count()
        // 同一 phone + cellid 内按时间排序
        val windowSpec = Window.partitionBy("phone", "cellid")
            .orderBy("times")

        val resultDF = df
        // 前一条 register
            .withColumn(
                "prev_register",
                lag(col("register"), 1).over(windowSpec)
            )
            // 前一条时间
            .withColumn(
                "prev_time",
                lag(col("times"), 1).over(windowSpec)
            )
            // 只保留：
            // 前一条是 enter(1)
            // 当前是 exit(2)
            .filter(
                col("prev_register") === 1 &&
                col("register") === 2
            )
            // 生成区间
            .withColumn("enter_time", col("prev_time"))
            .withColumn("exit_time", col("times"))
            .cache()
        resultDF.count()

        val finalDF = (if (includePhone) {
            resultDF.select(
                col("phone"),
                col("cellid"),
                col("enter_time"),
                col("exit_time")
            )
            } else {
                resultDF.select(
                col("cellid"),
                col("enter_time"),
                col("exit_time")
                )
            }).cache()
        finalDF.count()
        finalDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        spark.catalog.clearCache()
    }

    def TimeRange(
                    spark: SparkSession,
                    inputTable: String,
                    includePhone: Boolean = true,
                    outputTable: String,
                    robust: Boolean = true
                ): Unit = {
        println("processing time range")
        if (robust) {
            TimeRangeRobust(
                spark=spark,
                inputTable=inputTable,
                includePhone=includePhone,
                outputTable=outputTable)
        } else {
            TimeRangeSimple(
                spark=spark,
                inputTable=inputTable,
                includePhone=includePhone,
                outputTable=outputTable)
        }
    }
    def FindContacts(
                        spark: SparkSession,
                        inputTableInfected: String,
                        inputTableAll: String,
                        inputTableInfo: String,
                        includeInfected: Boolean = false,
                        outputTable: String,
                        outputPath: String
                    ): Unit = {
        println("processing contacts")
        val infectedDF = spark.table(inputTableInfected)
            .select(
                col("phone"),
                col("cellid"),
                col("enter_time"),
                col("exit_time")
            )
            .alias("infected")
            .cache()
        infectedDF.count()

        val allDF = spark.table(inputTableAll)
            .select(
                col("phone"),
                col("cellid"),
                col("enter_time"),
                col("exit_time")
            )
            .alias("all")
            .cache()
        allDF.count()
        // 区间重叠
        val overlapDF = infectedDF
            .join(
                allDF,
                col("infected.cellid") === col("all.cellid") &&
                col("all.enter_time") <= col("infected.exit_time") &&
                col("all.exit_time") >= col("infected.enter_time"),
                "inner"
            )
            .select(col("all.phone"))
            .distinct()
            .cache()
        overlapDF.count()
        val resultDF = (if (includeInfected) {
                overlapDF.orderBy("phone")
            } else {
                val infectedPhonesDF = spark.table(inputTableInfo)
                    .select(col("phone"))
                    .distinct()
                overlapDF
                .join(
                    infectedPhonesDF,
                    Seq("phone"),
                    "left_anti"
                )
                .orderBy("phone")
            }).cache()
        resultDF.count()
        resultDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        
        resultDF.select(col("phone").cast("string"))
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(outputPath)
        spark.catalog.clearCache()
    }
    def PrintUsage(): Unit = {
        val buff = new StringBuilder
        buff.append("Usage : com.tipdm.covid19.Redcode").append(" ")
            .append("<io_cdinfo>").append(" ")
            .append("<io_infected>").append(" ")
            .append("<io_infected_info>").append(" ")
            .append("<io_all_timerange>").append(" ")
            .append("<io_infected_timerange>").append(" ")
            .append("<io_contacts>").append(" ")
            .append("<include_infected(include|exclude)>").append(" ")
            .append("<robustness(robust|simple)>").append(" ")
            .append("<output_path>")
        println(buff.toString())
    }
}