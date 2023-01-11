/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.lambda.stream.stats.TransientStats
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import java.io.Closeable

trait TransientStore extends HasGeoMesaStats with Closeable {

  override val stats: GeoMesaStats = new TransientStats(this)

  def sft: SimpleFeatureType

  def createSchema(): Unit

  def removeSchema(): Unit

  def read(
      filter: Option[Filter] = None,
      transforms: Option[Array[String]] = None,
      hints: Option[Hints] = None,
      explain: Explainer = new ExplainLogging): QueryResult

  def write(feature: SimpleFeature): Unit

  def delete(feature: SimpleFeature): Unit

  def persist(): Unit
}
