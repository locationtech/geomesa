/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import org.geotools.data.{DataStore, Query, Transaction}
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, NoOpMetadata}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{SeqStat, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class DistributedRunnableStats(val ds: DataStore with HasGeoMesaMetadata[String]) extends MetadataBackedStats {

  override protected val generateStats: Boolean = false

  override private [geomesa] val metadata: GeoMesaMetadata[Stat] = new NoOpMetadata[Stat]

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_STRING, stats)
    query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

    try {
      WithClose(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) { reader =>
        // stats should always return exactly one result, even if there are no features in the table
        val result = if (reader.hasNext) { reader.next.getAttribute(0).asInstanceOf[String] } else {
          throw new IllegalStateException("Stats scan didn't return any rows")
        }
        StatsScan.decodeStat(sft)(result) match {
          case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
          case s => Seq(s).asInstanceOf[Seq[T]]
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(filter)}'", e)
        Seq.empty
    }
  }
}
