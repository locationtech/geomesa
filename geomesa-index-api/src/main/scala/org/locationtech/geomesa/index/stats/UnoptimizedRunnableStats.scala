/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.stats.{SeqStat, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Allows for running of stats in a non-distributed fashion, and doesn't store any stats results
  *
  * @param ds datastore
  */
class UnoptimizedRunnableStats[DS <: GeoMesaDataStore[_, _, _]](val ds: DS) extends MetadataBackedStats[DS] {

  override protected val generateStats: Boolean = false

  override private [geomesa] val metadata: GeoMesaMetadata[Stat] = new NoOpMetadata[Stat]

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val stat = Stat(sft, stats)
    val query = new Query(sft.getTypeName, filter)
    try {
      val reader = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val result = try {
        reader.foreach(stat.observe)
      } finally {
        reader.close()
      }
      stat match {
        case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
        case s => Seq(s).asInstanceOf[Seq[T]]
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(filter)}'", e)
        Seq.empty
    }
  }
}

class NoOpMetadata[T] extends GeoMesaMetadata[T] {

  override def getFeatureTypes: Array[String] = Array.empty

  override def insert(typeName: String, key: String, value: T): Unit = {}

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {}

  override def remove(typeName: String, key: String): Unit = {}

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = None

  override def invalidateCache(typeName: String, key: String): Unit = {}

  override def delete(typeName: String): Unit = {}

  override def close(): Unit = {}
}
