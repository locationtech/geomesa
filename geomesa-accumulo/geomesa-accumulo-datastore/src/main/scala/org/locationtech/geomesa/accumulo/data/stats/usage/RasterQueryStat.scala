/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.encoders.lexi.LongReverseEncoder
import org.locationtech.geomesa.utils.monitoring.UsageStat

/**
 * Class for capturing query-related stats
 */
case class RasterQueryStat(typeName:   String,
                           date:          Long,
                           rasterQuery:   String,
                           planningTime:  Long,
                           scanTime:      Long,
                           mosaicTime:    Long,
                           numResults:    Int,
                           deleted: Boolean = false) extends UsageStat {
  override def storeType: String = RasterQueryStat.storeType
}

object RasterQueryStat {
  val storeType = "raster-accumulo"
}

/**
 * Maps query stats to accumulo
 */
object RasterQueryStatTransform extends UsageStatTransform[RasterQueryStat] {

  private val CQ_QUERY      = new Text("rasterQuery")
  private val CQ_PLANTIME   = new Text("timePlanning_ms")
  private val CQ_SCANTIME   = new Text("timeScanning_ms")
  private val CQ_MOSAICTIME = new Text("timeMosaicing_ms")
  private val CQ_TIME       = new Text("timeTotal_ms")
  private val CQ_HITS       = new Text("hits")
  val reverseEncoder = new LongReverseEncoder()
  val NUMBER_OF_CQ_DATA_TYPES = 6

  override def createMutation(stat: UsageStat) = {
    new Mutation(s"${stat.typeName}~${reverseEncoder.encode(stat.date)}")
  }

  override def statToMutation(stat: RasterQueryStat): Mutation = {
    val mutation = createMutation(stat)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_QUERY,      new Value(stat.rasterQuery.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_PLANTIME,   new Value(s"${stat.planningTime}".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_SCANTIME,   new Value(s"${stat.scanTime}".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_MOSAICTIME, new Value(s"${stat.mosaicTime}".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_TIME,       new Value(s"${stat.scanTime + stat.planningTime + stat.mosaicTime}".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_HITS,       new Value(stat.numResults.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  val ROWID = "(.*)~(.*)".r

  override def rowToStat(entries: Iterable[Entry[Key, Value]]): RasterQueryStat = {
    if (entries.isEmpty) {
      return null
    }

    val ROWID(featureName, dateString) = entries.head.getKey.getRow.toString
    val date = reverseEncoder.decode(dateString)
    val values = collection.mutable.Map.empty[Text, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier match {
        case CQ_QUERY => values.put(CQ_QUERY, e.getValue.toString)
        case CQ_PLANTIME => values.put(CQ_PLANTIME, e.getValue.toString.toLong)
        case CQ_SCANTIME => values.put(CQ_SCANTIME, e.getValue.toString.toLong)
        case CQ_MOSAICTIME => values.put(CQ_MOSAICTIME, e.getValue.toString.toLong)
        case CQ_HITS => values.put(CQ_HITS, e.getValue.toString.toInt)
        case CQ_TIME => // time is an aggregate, doesn't need to map back to anything
        case _ => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    val rasterQuery = values.getOrElse(CQ_QUERY, "").asInstanceOf[String]
    val planTime = values.getOrElse(CQ_PLANTIME, 0L).asInstanceOf[Long]
    val scanTime = values.getOrElse(CQ_SCANTIME, 0L).asInstanceOf[Long]
    val mosaicTime = values.getOrElse(CQ_MOSAICTIME, 0L).asInstanceOf[Long]
    val hits = values.getOrElse(CQ_HITS, 0).asInstanceOf[Int]

    RasterQueryStat(featureName, date, rasterQuery, planTime, scanTime, mosaicTime, hits)
  }

  def decodeStat(entry: Entry[Key, Value]): String = {
    val ROWID(featureName, dateString) = entry.getKey.getRow.toString
    val decodedDate = new Date(reverseEncoder.decode(dateString)).toString
    val cqVal = entry.getValue.toString

    entry.getKey.getColumnQualifier match {
      case CQ_QUERY => statToCSVStr(decodedDate, featureName, CQ_QUERY.toString, cqVal)
      case CQ_PLANTIME => statToCSVStr(decodedDate, featureName, CQ_PLANTIME.toString, cqVal)
      case CQ_SCANTIME => statToCSVStr(decodedDate, featureName, CQ_SCANTIME.toString, cqVal)
      case CQ_MOSAICTIME => statToCSVStr(decodedDate, featureName, CQ_MOSAICTIME.toString, cqVal)
      case CQ_HITS => statToCSVStr(decodedDate, featureName, CQ_HITS.toString, cqVal)
      case CQ_TIME => statToCSVStr(decodedDate, featureName, CQ_TIME.toString, cqVal)
      case _ => statToCSVStr(decodedDate, featureName, "unmappedCQType", cqVal)
    }
  }

  def statToCSVStr(dateString: String, featureName: String, cqType: String, cqVal: String): String = {
    s"$dateString,$featureName,$cqType,$cqVal"
  }
}
