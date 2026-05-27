package com.tipdm.covid19

import org.apache.spark.sql.SparkSession
import scala.util.Random

object BigDataBuilder {
  case class Visit(
                    phone: Long,
                    cellid: Long,
                    enterSec: Int,
                    exitSec: Int
                  )
  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      println(
        "Usage: BigDataBuilder " +
          "<phone_count> <cell_count> <avg_visits_per_phone> " +
          "<infected_count> <contacts_per_infected> " +
          "<date_yyyyMMdd> <output_root_hdfs> <partitions> <sort_output(true|false)>"
      )
      System.exit(1)
    }
    val phoneCount = args(0).toInt
    val cellCount = args(1).toInt
    val avgVisitsPerPhone = args(2).toInt
    val infectedCount = args(3).toInt
    val contactsPerInfected = args(4).toInt
    val date = args(5)
    val outputRoot = args(6).stripSuffix("/")
    val partitions = args(7).toInt
    val sortOutput = args(8).toBoolean
    val spark = SparkSession.builder()
      .appName("BigDataBuilder")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val phoneBase = 13000000000L
    val cellBase = 55000000000L
    val safeInfectedCount = math.min(infectedCount, phoneCount)
    val hotspotCount = math.max(1, cellCount / 10)
    def phoneOf(id: Int): Long = phoneBase + id.toLong
    def cellOf(id: Int): Long = cellBase + id.toLong
    val phoneIds = sc.parallelize(0 until phoneCount, partitions)
    val normalVisits = phoneIds.flatMap { pid =>
      val rnd = new Random(1000003L + pid)
      val minVisits = math.max(1, avgVisitsPerPhone / 2)
      val maxExtra = math.max(1, avgVisitsPerPhone)
      val visitCount = minVisits + rnd.nextInt(maxExtra + 1)
      val result = scala.collection.mutable.ArrayBuffer[Visit]()
      var currentSec = rnd.nextInt(3600)
      for (_ <- 0 until visitCount if currentSec < 86000) {
        val chooseHotspot = rnd.nextDouble() < 0.70
        val cellId =
          if (chooseHotspot) {
            cellOf(rnd.nextInt(hotspotCount))
          } else {
            cellOf(rnd.nextInt(cellCount))
          }
        val gap = rnd.nextInt(1800)
        val enterSec = math.min(86390, currentSec + gap)
        val duration = 300 + rnd.nextInt(3600)
        val exitSec = math.min(86399, enterSec + duration)
        if (enterSec < exitSec) {
          result += Visit(
            phone = phoneOf(pid),
            cellid = cellId,
            enterSec = enterSec,
            exitSec = exitSec
          )
        }
        currentSec = exitSec + rnd.nextInt(2400)
      }
      result.iterator
    }.cache()
    val infectedPhoneIds = (0 until safeInfectedCount).toArray
    val infectedPhoneSet = infectedPhoneIds.map(phoneOf).toSet
    val bcInfectedPhoneSet = sc.broadcast(infectedPhoneSet)
    val infectedVisitsLocal = normalVisits
      .filter(v => bcInfectedPhoneSet.value.contains(v.phone))
      .collect()
    val bcInfectedVisits = sc.broadcast(infectedVisitsLocal)
    val extraContactCount =
      if (phoneCount > safeInfectedCount && infectedVisitsLocal.nonEmpty) {
        safeInfectedCount * contactsPerInfected
      } else {
        0
      }
    val contactVisits = sc.parallelize(0 until extraContactCount, partitions)
      .flatMap { idx =>
        val infectedVisits = bcInfectedVisits.value
        if (infectedVisits.isEmpty) {
          Iterator.empty
        } else {
          val rnd = new Random(3000017L + idx)
          val baseVisit = infectedVisits(rnd.nextInt(infectedVisits.length))
          val nonInfectedCount = phoneCount - safeInfectedCount
          val contactPid = safeInfectedCount + (idx % nonInfectedCount)
          val overlapPoint =
            if (baseVisit.exitSec > baseVisit.enterSec) {
              baseVisit.enterSec + rnd.nextInt(baseVisit.exitSec - baseVisit.enterSec + 1)
            } else {
              baseVisit.enterSec
            }
          val enterSec = math.max(0, overlapPoint - rnd.nextInt(900))
          val exitSec = math.min(86399, overlapPoint + 300 + rnd.nextInt(1800))
          if (enterSec < exitSec) {
            Iterator(
              Visit(
                phone = phoneOf(contactPid),
                cellid = baseVisit.cellid,
                enterSec = enterSec,
                exitSec = exitSec
              )
            )
          } else {
            Iterator.empty
          }
        }
      }
    val allVisits = normalVisits.union(contactVisits).cache()
    def formatTimeString(day: String, sec: Int): String = {
      val safeSec = math.max(0, math.min(86399, sec))
      val hour = safeSec / 3600
      val minute = (safeSec % 3600) / 60
      val second = safeSec % 60
      f"$day$hourd$minuted$secondd"
    }

    val eventRdd = allVisits.flatMap { v =>
      val enterTime = formatTimeString(date, v.enterSec)
      val exitTime = formatTimeString(date, v.exitSec)
      Iterator(
        (v.cellid, enterTime, 1, v.phone),
        (v.cellid, exitTime, 2, v.phone)
      )
    }
    val finalEventRdd =
      if (sortOutput) {
        eventRdd.sortBy(x => (x._1, x._2, x._3, x._4))
      } else {
        eventRdd
      }
    val cdinfoLines = finalEventRdd.map {
      case (cellid, times, register, phone) =>
        s"$cellid,$times,$register,$phone"
    }
    cdinfoLines.saveAsTextFile(s"$outputRoot/cdinfo_fixed_big")
    val infectedLines = sc.parallelize(
      infectedPhoneIds,
      math.min(math.max(1, safeInfectedCount), partitions)
    ).map(id => phoneOf(id).toString)
    infectedLines.saveAsTextFile(s"$outputRoot/infected_big")
    val hbaseCdinfoLines = finalEventRdd.map {
      case (cellid, times, register, phone) =>
        val rowkey = s"${cellid}_${times}_${phone}_${register}"
        s"$rowkey%t$cellid%t$times%t$register%t$phone"
    }
    hbaseCdinfoLines.saveAsTextFile(s"$outputRoot/hbase_cdinfo_tsv")
    val hbaseInfectedLines = sc.parallelize(
      infectedPhoneIds,
      math.min(math.max(1, safeInfectedCount), partitions)
    ).map { id =>
      val phone = phoneOf(id)
      s"$phone%t$phone"
    }
    hbaseInfectedLines.saveAsTextFile(s"$outputRoot/hbase_infected_tsv")
    println(s"Generated cdinfo path       : $outputRoot/cdinfo_fixed_big")
    println(s"Generated infected path     : $outputRoot/infected_big")
    println(s"HBase cdinfo TSV path       : $outputRoot/hbase_cdinfo_tsv")
    println(s"HBase infected TSV path     : $outputRoot/hbase_infected_tsv")
    println(s"phone count                 : $phoneCount")
    println(s"cell count                  : $cellCount")
    println(s"avg visits per phone        : $avgVisitsPerPhone")
    println(s"infected count              : $safeInfectedCount")
    println(s"contacts per infected       : $contactsPerInfected")
    println(s"normal visit count          : ${normalVisits.count()}")
    println(s"extra contact visit count   : ${contactVisits.count()}")
    println(s"event count                 : ${allVisits.count() * 2}")
    spark.stop()
  }
}
