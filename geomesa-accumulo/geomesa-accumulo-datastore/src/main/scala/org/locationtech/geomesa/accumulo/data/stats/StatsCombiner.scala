/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Combiner, IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.data.SingleRowAccumuloMetadata
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.StatSerializer

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
  * Combiner for serialized stats. Should be one instance configured per catalog table. Simple feature
  * types and columns with stats should be set in the configuration.
  */
class StatsCombiner extends Combiner with LazyLogging {

  import StatsCombiner.{SeparatorOption, SftOption}

  private var serializers: Map[String, StatSerializer] = null
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
    if (!iter.hasNext) {
      head
    } else {
      val sftName = try {
        CachedLazyMetadata.decodeRow(key.getRow.getBytes, separator)._1
      } catch {
        // back compatible check
        case NonFatal(e) => SingleRowAccumuloMetadata.getTypeName(key.getRow)
      }
      val serializer = serializers(sftName)
      val stat = serializer.deserialize(head.get)
      iter.foreach { s =>
        try {
          stat += serializer.deserialize(s.get)
        } catch {
          case e: Exception => logger.error("Error combining stats:", e)
        }
      }
      new Value(serializer.serialize(stat))
    }
  }
}

object StatsCombiner {
  val SftOption = "sft-"
  val SeparatorOption = "sep"
}