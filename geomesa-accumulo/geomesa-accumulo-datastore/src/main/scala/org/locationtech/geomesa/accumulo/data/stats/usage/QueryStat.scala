/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.nio.charset.StandardCharsets
import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Class for capturing query-related stats
 */
case class QueryStat(typeName: String,
                     date:     Long,
                     user:     String,
                     filter:   String,
                     hints:    String,
                     planTime: Long,
                     scanTime: Long,
                     hits:     Long,
                     deleted:  Boolean = false) extends UsageStat

/**
 * Maps query stats to accumulo
 */
object QueryStatTransform extends UsageStatTransform[QueryStat] {

  private [usage] val CQ_USER     = new Text("user")
  private [usage] val CQ_FILTER   = new Text("queryFilter")
  private [usage] val CQ_HINTS    = new Text("queryHints")
  private [usage] val CQ_PLANTIME = new Text("timePlanning")
  private [usage] val CQ_SCANTIME = new Text("timeScanning")
  private [usage] val CQ_TIME     = new Text("timeTotal")
  private [usage] val CQ_HITS     = new Text("hits")
  private [usage] val CQ_DELETED  = new Text("deleted")

  override def statToMutation(stat: QueryStat): Mutation = {
    val mutation = createMutation(stat)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_USER,     new Value(stat.user.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_FILTER,   new Value(stat.filter.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_HINTS,    new Value(stat.hints.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_PLANTIME, new Value(s"${stat.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_SCANTIME, new Value(s"${stat.scanTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_TIME,     new Value(s"${stat.scanTime + stat.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_HITS,     new Value(stat.hits.toString.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_DELETED,  new Value(stat.deleted.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  override def rowToStat(entries: Iterable[Entry[Key, Value]]): QueryStat = {
    if (entries.isEmpty) {
      return null
    }

    val (featureName, date) = typeNameAndDate(entries.head.getKey)
    val values = collection.mutable.Map.empty[Text, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier match {
        case CQ_USER     => values.put(CQ_USER, e.getValue.toString)
        case CQ_FILTER   => values.put(CQ_FILTER, e.getValue.toString)
        case CQ_HINTS    => values.put(CQ_HINTS, e.getValue.toString)
        case CQ_PLANTIME => values.put(CQ_PLANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_SCANTIME => values.put(CQ_SCANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_HITS     => values.put(CQ_HITS, e.getValue.toString.toInt)
        case CQ_DELETED  => values.put(CQ_DELETED, e.getValue.toString.toBoolean)
        case CQ_TIME     => // time is an aggregate, doesn't need to map back to anything
        case _           => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    val user = values.getOrElse(CQ_USER, "unknown").asInstanceOf[String]
    val queryHints = values.getOrElse(CQ_HINTS, "").asInstanceOf[String]
    val queryFilter = values.getOrElse(CQ_FILTER, "").asInstanceOf[String]
    val planTime = values.getOrElse(CQ_PLANTIME, 0L).asInstanceOf[Long]
    val scanTime = values.getOrElse(CQ_SCANTIME, 0L).asInstanceOf[Long]
    val hits = values.getOrElse(CQ_HITS, 0).asInstanceOf[Int]
    val deleted = values.getOrElse(CQ_DELETED, false).asInstanceOf[Boolean]

    QueryStat(featureName, date, user, queryFilter, queryHints, planTime, scanTime, hits, deleted)
  }

  // list of query hints we want to persist
  val QUERY_HINTS = List[Hints.Key](TRANSFORMS,
                                    TRANSFORM_SCHEMA,
                                    DENSITY_BBOX_KEY,
                                    WIDTH_KEY,
                                    HEIGHT_KEY,
                                    BIN_TRACK_KEY,
                                    STATS_KEY)

  /**
   * Converts a query hints object to a string for persisting
   *
   * @param hints
   * @return
   */
  def hintsToString(hints: Hints): String =
    QUERY_HINTS.flatMap(k => Option(hints.get(k)).map(v => hintToString(k, v))).sorted.mkString(",")

  /**
   * Converts a single hint to a string
   */
  private def hintToString(key: Hints.Key, value: AnyRef): String =
    s"${keyToString(key)}=${valueToString(value)}"

  /**
   * Maps a query hint to a string. We need this since the classes themselves don't really have a
   * decent toString representation.
   *
   * @param key
   * @return
   */
  private def keyToString(key: Hints.Key): String =
    key match {
      case TRANSFORMS         => "TRANSFORMS"
      case TRANSFORM_SCHEMA   => "TRANSFORM_SCHEMA"
      case BIN_TRACK_KEY      => "BIN_TRACK_KEY"
      case STATS_KEY          => "STATS_STRING_KEY"
      case RETURN_ENCODED_KEY => "RETURN_ENCODED"
      case DENSITY_BBOX_KEY   => "DENSITY_BBOX_KEY"
      case WIDTH_KEY          => "WIDTH_KEY"
      case HEIGHT_KEY         => "HEIGHT_KEY"
      case _                  => "unknown_hint"
    }

  /**
   * Encodes a value into a decent readable string
   */
  private def valueToString(value: AnyRef): String = value match {
    case null => "null"
    case sft: SimpleFeatureType => s"[${sft.getTypeName}]${SimpleFeatureTypes.encodeType(sft)}"
    case v => v.toString
  }
}

case class SerializedQueryStat(typeName: String,
                               date:     Long,
                               deleted:  Boolean,
                               entries:  Map[(Text, Text), Value]) extends UsageStat {
  lazy val user = entries.find(_._1._2 == QueryStatTransform.CQ_USER).map(_._2.toString).getOrElse("unknown")
}

object SerializedQueryStatTransform extends UsageStatTransform[SerializedQueryStat] {

  import QueryStatTransform.CQ_DELETED

  override def statToMutation(stat: SerializedQueryStat): Mutation = {
    val mutation = createMutation(stat)
    stat.entries.foreach { case ((cf, cq), v) => mutation.put(cf, cq, v) }
    mutation.put(stat.entries.head._1._1, CQ_DELETED, new Value(stat.deleted.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  override def rowToStat(entries: Iterable[Entry[Key, Value]]): SerializedQueryStat = {
    val (typeName, date) = typeNameAndDate(entries.head.getKey)
    val kvs = entries.map(e => (e.getKey.getColumnFamily, e.getKey.getColumnQualifier) -> e.getValue)
    val (delete, others) = kvs.partition(_._1._2 == CQ_DELETED)
    // noinspection MapGetOrElseBoolean
    val deleted = delete.headOption.map(_._2.toString.toBoolean).getOrElse(false)
    SerializedQueryStat(typeName, date, deleted, others.toMap)
  }
}