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
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.FeatureSampler

/**
 * Visitor for sampling a percent of features
 *
 * @param features features to sample
 * @param percent percent of features to retain
 * @param threading threading key
 */
class SamplingVisitor(features: SimpleFeatureCollection, percent: Float, threading: Option[String])
  extends GeoMesaProcessVisitor with LazyLogging {

  private val manualVisitResults = new ListFeatureCollection(features.getSchema)

  private var resultCalc = FeatureResult(manualVisitResults)

  private val nth = (1f / percent).toInt
  private val thread = threading.map(features.getSchema.indexOf).filter(_ != -1)

  private val sampling = FeatureSampler.sample(nth, thread)

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (sampling(sf)) {
      manualVisitResults.add(sf)
    }
  }

  override def getResult: FeatureResult = resultCalc

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Running Geomesa sampling process on source type ${source.getClass.getName}")
    query.getHints.put(QueryHints.SAMPLING, percent)
    threading.foreach(query.getHints.put(QueryHints.SAMPLE_BY, _))
    resultCalc = FeatureResult(source.getFeatures(query))
  }
}
