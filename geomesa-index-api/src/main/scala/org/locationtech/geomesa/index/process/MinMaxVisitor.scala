/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, SimpleFeatureSource}
import org.geotools.api.feature.Feature
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.stats.Stat

/**
 * Feature visitor for calculating min/max values
 *
 * @param features features
 * @param attribute attribute to calculate
 * @param cached use cached stats, if available, vs calculating on the data
 */
class MinMaxVisitor(features: SimpleFeatureCollection, attribute: String, cached: Boolean)
  extends GeoMesaProcessVisitor with LazyLogging {

  private lazy val stat: Stat = Stat(features.getSchema, Stat.MinMax(attribute))

  private var resultCalc: FeatureResult = _

  // non-optimized visit
  override def visit(feature: Feature): Unit = stat.observe(feature.asInstanceOf[SimpleFeature])

  override def getResult: FeatureResult = {
    if (resultCalc != null) {
      resultCalc
    } else {
      createResult(stat.toJson)
    }
  }

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Running Geomesa min/max process on source type ${source.getClass.getName}")

    source.getDataStore match {
      case ds: HasGeoMesaStats =>
        resultCalc = ds.stats.getMinMax[Any](source.getSchema, attribute, query.getFilter, !cached) match {
          case None     => createResult("{}")
          case Some(mm) => createResult(mm.toJson)
        }

      case ds =>
        logger.warn(s"Running unoptimized min/max query on ${ds.getClass.getName}")
        SelfClosingIterator(features.features).foreach(visit)
    }
  }

  private def createResult(stat: String): FeatureResult = {
    val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "", Array(stat, GeometryUtils.zeroPoint))
    FeatureResult(new ListFeatureCollection(StatsScan.StatsSft, sf))
  }
}
