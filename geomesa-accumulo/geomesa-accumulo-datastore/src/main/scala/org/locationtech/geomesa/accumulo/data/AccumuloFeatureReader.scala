/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats._
import org.locationtech.geomesa.utils.stats.{MethodProfiling, NoOpTimings, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(queryPlanner: QueryPlanner, query: Query, dataStore: AccumuloDataStore)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  private val writeStats = dataStore.isInstanceOf[StatWriter]

  implicit val timings = if (writeStats) new TimingsImpl else NoOpTimings

  private val iter = profile(queryPlanner.runQuery(query), "planning")

  override def getFeatureType = queryPlanner.sft

  override def next() = profile(iter.next(), "next")

  override def hasNext = profile(iter.hasNext, "hasNext")

  override def close() = {
    iter.close()
    if (writeStats) {
      val stat = QueryStat(queryPlanner.sft.getTypeName,
          System.currentTimeMillis(),
          QueryStatTransform.filterToString(query.getFilter),
          QueryStatTransform.hintsToString(query.getHints),
          timings.time("planning"),
          timings.time("next") + timings.time("hasNext"),
          timings.occurrences("next").toInt)
      dataStore.asInstanceOf[StatWriter].writeStat(stat, dataStore.getQueriesTableName(queryPlanner.sft))
    }
  }
}
