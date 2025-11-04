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
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.geotools.Transform.Transforms

/**
 * Visitor for calculating stats
 *
 * @param features features
 * @param statString stats to calculate
 * @param encode encode the result as binary, vs return as json
 * @param properties transform properties, or null for no transform
 */
class StatsVisitor(features: SimpleFeatureCollection, statString: String, encode: Boolean, properties: Array[String])
  extends GeoMesaProcessVisitor with LazyLogging {

  private val origSft = features.getSchema

  private lazy val transformDefinitions = Transforms(origSft, properties)
  private lazy val transformSft = Transforms.schema(origSft, transformDefinitions)
  private lazy val transformSf = TransformSimpleFeature(transformSft, transformDefinitions)

  private lazy val statSft = if (properties == null) { origSft } else { transformSft }

  private lazy val stat: Stat = Stat(statSft, statString)

  private var resultCalc: FeatureResult = _

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (properties != null) {
      // There are transforms!
      transformSf.setFeature(sf)
      stat.observe(transformSf)
    } else {
      stat.observe(sf)
    }
  }

  override def getResult: FeatureResult = {
    if (resultCalc != null) {
      resultCalc
    } else {
      val stats = if (encode) {
        StatsScan.encodeStat(statSft)(stat)
      } else {
        stat.toJson
      }

      val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "", Array(stats, GeometryUtils.zeroPoint))
      val manualVisitResults = new ListFeatureCollection(StatsScan.StatsSft)
      manualVisitResults.add(sf)
      FeatureResult(manualVisitResults)
    }
  }

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Running Geomesa stats iterator process on source type ${source.getClass.getName}")

    if (properties != null) {
      if (query.getProperties != Query.ALL_PROPERTIES) {
        logger.warn(s"Overriding inner query's properties (${query.getProperties}) " +
          s"with properties/transforms ${properties.mkString(",")}.")
      }
      query.setPropertyNames(properties: _*)
    }
    query.getHints.put(QueryHints.STATS_STRING, statString)
    query.getHints.put(QueryHints.ENCODE_STATS, Boolean.box(encode))
    resultCalc = FeatureResult(source.getFeatures(query))
  }
}
