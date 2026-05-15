package com.tipdm.covid19

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object Redcode {
    def main(args: Array[String]): Unit = {
        val args = Array(
            "dingdongji.cdinfo", // "cdinfo_fixed.txt" 所对应数据表
            "dingdongji.infected", // "infected.txt" 所对应数据表
            "dingdongji.infected_info", // 中间表，感染者的 cdinfo 数据
            "dingdongji.all_timerange", // 中间表，所有用户的基站停留时间段
            "dingdongji.infected_timerange", // 中间表，感染者的基站停留时间段
            "dingdongji.contacts", // 输出表，密接人员列表
            "include", // 是否包含感染者自己，"include" or "exclude"
            "robust" // 时间区间计算的鲁棒性，"robust" or "simple"
        )
        if (args.length != 8) {
            PrintUsage()
            System.exit(1)
        }
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("Redcode")
            .config(
                "spark.sql.warehouse.dir",
                "hdfs://master:8020/user/hive/warehouse"
            )
            .enableHiveSupport()
            .getOrCreate()
        ///////// 打印 Hive 配置以调试，可以注释掉 //////////
        println(
            spark.sparkContext.hadoopConfiguration.get(
                "hive.metastore.uris"
            )
        )
        println(
            spark.conf.get(
                "spark.sql.warehouse.dir",
                "NOT_FOUND"
            )
        )
        ///////////////////////////////////////////////////
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
                false // 这个 false 是为了满足编译器，实际不会执行到这里，因为上面已经 exit 了。
            }
        val robust = if (args(7).toLowerCase() == "robust") {
                true
            } else if (args(7).toLowerCase() == "simple") {
                false
            } else {
                PrintUsage()
                System.exit(1)
                false // 这个 false 是为了满足编译器，实际不会执行到这里，因为上面已经 exit 了。
            }
        // 这里是第一座屎山，如果表已经存在就删除，避免后续写入失败，我也不知道是否必要，反正就干了 //
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
        ////////////////////////////////////////////////////////////////////////////////////////
        val cdinfo = spark.table(io_cdinfo)
        val infected = spark.table(io_infected)
        // 找感染者的信息
        val infected_info = cdinfo.join(
            infected.select("phone").distinct(),
            Seq("phone"),
            "left_semi"
        ).cache()
        // 第二座屎山，cache 之后 count 一下触发计算。
        infected_info.count()
        // 两个输出语句，当时为了确保这个表真的算到了，不是代码的问题，而是 Hive 的问题，因为就是这里当时 spark-shell 警告我说 infected_info 不存在。
        println("processing infected_info")
        infected_info.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(io_infected_info)
        println("after infected_info")
        // 清除缓存，我也不知道要不要清啊，当时还是因为表不存在的错误才加的，我也不知道要不要加。
        spark.catalog.clearCache()
        // 算所有人的时间区间，includePhone 一律 true 了。但是后续如果确定表是没有异常进出记录的，可以用 robust=false 的简单版本，性能会好很多（吧？）。
        TimeRange(
            spark=spark,
            inputTable=io_cdinfo,
            includePhone=true,
            outputTable=io_all_timerange,
            robust=robust)
        // 算感染者的时间区间
        TimeRange(
            spark=spark,
            inputTable=io_infected_info,
            includePhone=true,
            outputTable=io_infected_timerange,
            robust=robust)
        // 找密接者，includeInfected 参数控制是否把感染者自己也算作密接者（因为他们自己也是在重叠区间内的）。
        FindContacts(
            spark=spark,
            inputTableInfected=io_infected_timerange,
            inputTableAll=io_all_timerange,
            inputTableInfo=io_infected,
            includeInfected=include,
            outputTable=io_contacts)
        // 这里也是屎山，之前在 spark-shell 里找表的时候，明明写入了表，但是后续查询的时候说表不存在，后来加了这个刷新表的代码才解决的，我也不知道为什么会有这种问题，反正就加了。
        tables.foreach { table =>
            spark.sql("refresh table dingdongji.contacts")
            println("refreshed")
        }
        spark.stop()
    }
    // 定义输入输出的 case class，方便 Dataset 操作
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
        // 又是屎山，触发计算
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
        // 依旧屎山，cache 之后触发计算
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
        // 依旧屎山，触发计算
        resultDF.count()

        resultDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        // 依旧屎山，清除缓存，我也不知道要不要清啊，当时还是因为表不存在的错误才加的，我也不知道要不要加。
        spark.catalog.clearCache()
    }

    def TimeRangeSimple(
                        spark: SparkSession,
                        inputTable: String,
                        includePhone: Boolean = true,
                        outputTable: String
                        ): Unit = {
        val df = spark.table(inputTable).cache()
        // 屎山，触发计算
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
        // 依旧屎山，触发计算
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
        // 依旧屎山，触发计算
        finalDF.count()
        finalDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        // 依旧屎山，清除缓存，我也不知道要不要清啊，当时还是因为表不存在的错误才加的，我也不知道要不要加。
        spark.catalog.clearCache()
    }

    def TimeRange(
                    spark: SparkSession,
                    inputTable: String,
                    includePhone: Boolean = true,
                    outputTable: String,
                    robust: Boolean = true
                ): Unit = {
        // 当时是为了确保这个函数被执行到了，加一个输出，也是因为当时表不存在。
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
                        outputTable: String
                    ): Unit = {
        // 当时是为了确保这个函数被执行到了，加一个输出，也是因为当时表不存在。
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
        // 依旧屎山，触发计算
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
        // 依旧屎山，触发计算
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
        // 依旧屎山，触发计算
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
        // 依旧屎山，触发计算
        resultDF.count()
        resultDF.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(outputTable)
        // 依旧屎山，清除缓存，我也不知道要不要清啊，当时还是因为表不存在的错误才加的，我也不知道要不要加。
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
            .append("<robustness(robust|simple)>")
        println(buff.toString())
    }
}