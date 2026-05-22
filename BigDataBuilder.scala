package com.tipdm.covid19

import org.apache.spark.sql.SparkSession

object BigDataBuilder {
  case class CdRecord(
                       cellid: String,
                       times: String,
                       register: String,
                       phone: String
                     )
  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println(
        "Usage: BigDataBuilder <sample_cdinfo_hdfs> <sample_infected_hdfs> " +
          "<copies> <infected_copies> <output_root_hdfs> <partitions>"
      )
      System.exit(1)
    }
    val sampleCdinfoPath = args(0)
    val sampleInfectedPath = args(1)
    val copies = args(2).toInt
    val infectedCopies = args(3).toInt
    val outputRoot = args(4).stripSuffix("/")
    val partitions = args(5).toInt
    val spark = SparkSession.builder()
      .appName("BigDataBuilder")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val sampleRecords = sc.textFile(sampleCdinfoPath)
      .filter(_.trim.nonEmpty)
      .map { line =>
        val arr = line.split(",").map(_.trim)
        CdRecord(
          cellid = arr(0),
          times = arr(1),
          register = arr(2),
          phone = arr(3)
        )
      }
      .collect()
      .toArray
    val infectedPhones = sc.textFile(sampleInfectedPath)
      .filter(_.trim.nonEmpty)
      .map(_.trim)
      .distinct()
      .collect()
      .toArray
    val allPhones = (sampleRecords.map(_.phone) ++ infectedPhones)
      .distinct
      .sortBy(_.toLong)
    val allCells = sampleRecords.map(_.cellid)
      .distinct
      .sortBy(_.toLong)
    val phoneIndex = allPhones.zipWithIndex.toMap
    val cellIndex = allCells.zipWithIndex.toMap
    val phoneCount = allPhones.length
    val cellCount = allCells.length
    val bcRecords = sc.broadcast(sampleRecords)
    val bcInfected = sc.broadcast(infectedPhones)
    val bcPhoneIndex = sc.broadcast(phoneIndex)
    val bcCellIndex = sc.broadcast(cellIndex)
    def mapPhone(phone: String, copyId: Int): Long = {
      13000000000L + copyId.toLong * phoneCount + bcPhoneIndex.value(phone)
    }
    def mapCell(cellid: String, copyId: Int): Long = {
      55000000000L + copyId.toLong * cellCount + bcCellIndex.value(cellid)
    }
    val copyIds = sc.parallelize(0 until copies, partitions)
    val cdinfoRdd = copyIds.flatMap { copyId =>
      bcRecords.value.iterator.map { r =>
        val newCell = mapCell(r.cellid, copyId)
        val newPhone = mapPhone(r.phone, copyId)
        s"$newCell,${r.times},${r.register},$newPhone"
      }
    }
    cdinfoRdd.saveAsTextFile(s"$outputRoot/cdinfo_fixed_big")
    val infectedRdd = sc.parallelize(0 until infectedCopies, math.min(infectedCopies, partitions))
      .flatMap { copyId =>
        bcInfected.value.iterator.map { phone =>
          mapPhone(phone, copyId).toString
        }
      }
      .distinct()
    infectedRdd.saveAsTextFile(s"$outputRoot/infected_big")

    // 给 HBase ImportTsv 准备的数据：rowkey, cellid, times, register, phone
    val cdinfoHBaseTsv = copyIds.flatMap { copyId =>
      bcRecords.value.iterator.map { r =>
        val newCell = mapCell(r.cellid, copyId)
        val newPhone = mapPhone(r.phone, copyId)
        val rowkey = s"${newCell}_${r.times}_${newPhone}_${r.register}"
        s"$rowkey\t$newCell\t${r.times}\t${r.register}\t$newPhone"
      }
    }
    cdinfoHBaseTsv.saveAsTextFile(s"$outputRoot/hbase_cdinfo_tsv")

    // 给 HBase ImportTsv 准备的数据：rowkey, phone
    val infectedHBaseTsv = sc.parallelize(0 until infectedCopies, math.min(infectedCopies, partitions))
      .flatMap { copyId =>
        bcInfected.value.iterator.map { phone =>
          val newPhone = mapPhone(phone, copyId)
          s"$newPhone\t$newPhone"
        }
      }
      .distinct()
    infectedHBaseTsv.saveAsTextFile(s"$outputRoot/hbase_infected_tsv")
    println(s"Generated cdinfo path    : $outputRoot/cdinfo_fixed_big")
    println(s"Generated infected path  : $outputRoot/infected_big")
    println(s"HBase cdinfo TSV path    : $outputRoot/hbase_cdinfo_tsv")
    println(s"HBase infected TSV path  : $outputRoot/hbase_infected_tsv")
    println(s"copies                   : $copies")
    println(s"infected copies          : $infectedCopies")
    println(s"sample records           : ${sampleRecords.length}")
    println(s"generated records        : ${sampleRecords.length.toLong * copies}")
    spark.stop()
  }
}