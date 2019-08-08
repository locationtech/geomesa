/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Stats Process",
  description = "Gathers statistics for a data set"
)
class StatsProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
                 @DescribeParameter(
                   name = "features",
                   description = "The feature set on which to query")
                 features: SimpleFeatureCollection,

                 @DescribeParameter(
                   name = "statString",
                   description = "The string indicating what stats to instantiate")
                 statString: String,

                 @DescribeParameter(
                   name = "encode",
                   min = 0,
                   description = "Return the values encoded or as json")
                 encode: java.lang.Boolean = null,

                 @DescribeParameter(
                   name = "properties",
                   min = 0,
                   max = 128,
                   collectionType = classOf[String],
                   description = "The properties / transforms to apply before gathering stats")
                 properties: java.util.List[String] = null

             ): SimpleFeatureCollection = {

    logger.debug(s"Attempting stats iterator process on type ${features.getClass.getName}")

    val propsArray = Option(properties).map(_.toArray(Array.empty[String])).filter(_.length > 0).orNull

    val visitor = new StatsVisitor(features, statString, Option(encode).exists(_.booleanValue()), propsArray)
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}

class StatsVisitor(features: SimpleFeatureCollection, statString: String, encode: Boolean, properties: Array[String])
    extends GeoMesaProcessVisitor with LazyLogging {

  private val origSft = features.getSchema

  private lazy val (transforms, transformSFT) = QueryPlanner.buildTransformSFT(origSft, properties)
  private lazy val transformSF: TransformSimpleFeature = TransformSimpleFeature(origSft, transformSFT, transforms)

  private lazy val statSft = if (properties == null) { origSft } else { transformSFT }

  private lazy val stat: Stat = Stat(statSft, statString)

  private var resultCalc: FeatureResult = _

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (properties != null) {
      // There are transforms!
      transformSF.setFeature(sf)
      stat.observe(transformSF)
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
      query.setPropertyNames(properties)
    }
    query.getHints.put(QueryHints.STATS_STRING, statString)
    query.getHints.put(QueryHints.ENCODE_STATS, new java.lang.Boolean(encode))
    resultCalc = FeatureResult(source.getFeatures(query))
  }
}
