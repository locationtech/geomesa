/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{Connector, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.apache.accumulo.core.iterators.{Combiner, IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.SingleRowAccumuloMetadata
import org.locationtech.geomesa.accumulo.data.stats.AccumuloGeoMesaStats.CombinerName
import org.locationtech.geomesa.index.metadata.CachedLazyBinaryMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
  * Combiner for serialized stats. Should be one instance configured per catalog table. Simple feature
  * types and columns with stats should be set in the configuration.
  */
class StatsCombiner extends Combiner with LazyLogging {

  import StatsCombiner.{SeparatorOption, SftOption}

  private var serializers: Map[String, StatSerializer] = _
  private var separator: Char = '~'

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    serializers = options.toMap.collect {
      case (k, v) if k.startsWith(SftOption) =>
        val typeName = k.substring(SftOption.length)
        (typeName, StatSerializer(SimpleFeatureTypes.createType(typeName, v)))
    }
    separator = Option(options.get(SeparatorOption)).map(_.charAt(0)).getOrElse('~')
  }

  override def reduce(key: Key, iter: java.util.Iterator[Value]): Value = {
    val head = iter.next()
    if (!iter.hasNext) { head } else {
      val sftName = try {
        CachedLazyBinaryMetadata.decodeRow(key.getRow.getBytes, separator)._1
      } catch {
        // back compatible check
        case NonFatal(_) => SingleRowAccumuloMetadata.getTypeName(key.getRow)
      }
      val serializer = serializers(sftName)

      var stat = deserialize(head, serializer)

      while (stat == null) {
        if (iter.hasNext) {
          stat = deserialize(iter.next, serializer)
        } else {
          return head // couldn't parse anything... return head value and let client deal with it
        }
      }

      iter.foreach { s =>
        try { stat += serializer.deserialize(s.get) } catch {
          case NonFatal(e) => logger.error("Error combining stats:", e)
        }
      }

      new Value(serializer.serialize(stat))
    }
  }

  private def deserialize(value: Value, serializer: StatSerializer): Stat = {
    try { serializer.deserialize(value.get) } catch {
      case NonFatal(e) => logger.error("Error deserializing stat:", e); null
    }
  }
}

object StatsCombiner {

  val SftOption = "sft-"
  val SeparatorOption = "sep"

  def configure(sft: SimpleFeatureType, connector: Connector, table: String, separator: String): Unit = {
    AccumuloVersion.createTableIfNeeded(connector, table)

    val sftKey = getSftKey(sft)
    val sftOpt = SimpleFeatureTypes.encodeType(sft)

    getExisting(connector, table) match {
      case None => attach(connector, table, options(separator) + (sftKey -> sftOpt))
      case Some(existing) =>
        val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
        if (!existingSfts.get(sftKey).contains(sftOpt)) {
          connector.tableOperations().removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
          attach(connector, table, existingSfts.toMap ++ options(separator) + (sftKey -> sftOpt))
        }
    }
  }

  def remove(sft: SimpleFeatureType, connector: Connector, table: String, separator: String): Unit = {
    getExisting(connector, table).foreach { existing =>
      val sftKey = getSftKey(sft)
      val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
      if (existingSfts.containsKey(sftKey)) {
        connector.tableOperations().removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
        if (existingSfts.size > 1) {
          attach(connector, table, (existingSfts.toMap - sftKey) ++ options(separator))
        }
      }
    }
  }

  def list(connector: Connector, table: String): scala.collection.Map[String, String] = {
    getExisting(connector, table) match {
      case None => Map.empty
      case Some(existing) =>
        existing.getOptions.collect {
          case (k, v) if k.startsWith(SftOption) => (k.substring(SftOption.length), v)
        }
    }
  }

  private def getExisting(connector: Connector, table: String): Option[IteratorSetting] = {
    if (!connector.tableOperations().exists(table)) { None } else {
      Option(connector.tableOperations().getIteratorSetting(table, CombinerName, IteratorScope.scan))
    }
  }

  private def options(separator: String): Map[String, String] =
    Map(StatsCombiner.SeparatorOption -> separator, "all" -> "true")

  private def getSftKey(sft: SimpleFeatureType): String = s"$SftOption${sft.getTypeName}"

  private def attach(connector: Connector, table: String, options: Map[String, String]): Unit = {
    // priority needs to be less than the versioning iterator at 20
    val is = new IteratorSetting(10, CombinerName, classOf[StatsCombiner])
    options.foreach { case (k, v) => is.addOption(k, v) }
    connector.tableOperations().attachIterator(table, is)
  }
}