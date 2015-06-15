/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.stats

import java.util.Date
import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.calrissian.mango.types.encoders.lexi.LongReverseEncoder

/**
 * Class for capturing query-related stats
 */
case class RasterQueryStat(featureName:   String,
                           date:          Long,
                           rasterQuery:   String,
                           planningTime:  Long,
                           scanTime:      Long,
                           mosaicTime:    Long,
                           numResults:    Int) extends Stat

/**
 * Maps query stats to accumulo
 */
object RasterQueryStatTransform extends StatTransform[RasterQueryStat] {

  private val CQ_QUERY = "rasterQuery"
  private val CQ_PLANTIME = "timePlanning_ms"
  private val CQ_SCANTIME = "timeScanning_ms"
  private val CQ_MOSAICTIME = "timeMosaicing_ms"
  private val CQ_TIME = "timeTotal_ms"
  private val CQ_HITS = "hits"
  val reverseEncoder = new LongReverseEncoder()
  val NUMBER_OF_CQ_DATA_TYPES = 6

  override def createMutation(stat: Stat) = {
    new Mutation(s"${stat.featureName}~${reverseEncoder.encode(stat.date)}")
  }

  override def statToMutation(stat: RasterQueryStat): Mutation = {
    val mutation = createMutation(stat)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_QUERY, stat.rasterQuery)
    mutation.put(cf, CQ_PLANTIME, s"${stat.planningTime}")
    mutation.put(cf, CQ_SCANTIME, s"${stat.scanTime}")
    mutation.put(cf, CQ_MOSAICTIME, s"${stat.mosaicTime}")
    mutation.put(cf, CQ_TIME, s"${stat.scanTime + stat.planningTime + stat.mosaicTime}")
    mutation.put(cf, CQ_HITS, stat.numResults.toString)
    mutation
  }

  val ROWID = "(.*)~(.*)".r

  override def rowToStat(entries: Iterable[Entry[Key, Value]]): RasterQueryStat = {
    if (entries.isEmpty) {
      return null
    }

    val ROWID(featureName, dateString) = entries.head.getKey.getRow.toString
    val date = reverseEncoder.decode(dateString)
    val values = collection.mutable.Map.empty[String, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier.toString match {
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

    entry.getKey.getColumnQualifier.toString match {
      case CQ_QUERY => statToCSVStr(decodedDate, featureName, CQ_QUERY, cqVal)
      case CQ_PLANTIME => statToCSVStr(decodedDate, featureName, CQ_PLANTIME, cqVal)
      case CQ_SCANTIME => statToCSVStr(decodedDate, featureName, CQ_SCANTIME, cqVal)
      case CQ_MOSAICTIME => statToCSVStr(decodedDate, featureName, CQ_MOSAICTIME, cqVal)
      case CQ_HITS => statToCSVStr(decodedDate, featureName, CQ_HITS, cqVal)
      case CQ_TIME => statToCSVStr(decodedDate, featureName, CQ_TIME, cqVal)
      case _ => statToCSVStr(decodedDate, featureName, "unmappedCQType", cqVal)
    }
  }

  def statToCSVStr(dateString: String, featureName: String, cqType: String, cqVal: String): String = {
    s"$dateString,$featureName,$cqType,$cqVal"
  }
}