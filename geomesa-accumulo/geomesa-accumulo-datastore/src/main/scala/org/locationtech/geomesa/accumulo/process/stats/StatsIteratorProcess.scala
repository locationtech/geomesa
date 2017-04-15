/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.stats

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.api.QueryPlanner
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Stats Iterator Process",
  description = "Returns stats based upon the passed in stats string"
)
class StatsIteratorProcess extends LazyLogging {

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
                 encode: Boolean = false,

                 @DescribeParameter(
                   name = "properties",
                   min = 0,
                   description = "The properties / transforms to apply before gathering stats")
                 properties: String = null

             ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa stats iterator process on type " + features.getClass.getName)

    if (features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val arrayString = Option(properties).map(_.split(";")).orNull

    val visitor = new StatsVisitor(features, statString, encode, arrayString)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[StatsIteratorResult].results
  }
}

class StatsVisitor(features: SimpleFeatureCollection, statString: String, encode: Boolean, properties: Array[String] = null)
    extends FeatureCalc with LazyLogging {

  import scala.collection.JavaConversions._
  val origSft = features.getSchema

  lazy val (transforms, transformSFT) = QueryPlanner.buildTransformSFT(origSft, properties)
  lazy val transformSF: TransformSimpleFeature = TransformSimpleFeature(origSft, transformSFT, transforms)

  lazy val statSft = if (properties == null) {
    origSft
  } else {
    transformSFT
  }

  lazy val stat: Stat = Stat(statSft, statString)

  val returnSft = KryoLazyStatsIterator.StatsSft
  val manualVisitResults: DefaultFeatureCollection = new DefaultFeatureCollection(null, returnSft)
  var resultCalc: StatsIteratorResult = null

  //  Called for non AccumuloFeatureCollections
  def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]

    if (properties != null) {
      // There are transforms!
      transformSF.setFeature(sf)
      stat.observe(transformSF)
    } else {
      stat.observe(sf)
    }
  }

  override def getResult: CalcResult = {
    if (resultCalc == null) {
      val stats = if (encode) {
        KryoLazyStatsIterator.encodeStat(stat, statSft)
      } else {
        stat.toJson
      }

      val sf = new ScalaSimpleFeature("", returnSft, Array(stats, GeometryUtils.zeroPoint))
      manualVisitResults.add(sf)
      StatsIteratorResult(manualVisitResults)
    } else {
      resultCalc
    }
  }

  def setValue(r: SimpleFeatureCollection) = resultCalc = StatsIteratorResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.debug("Running Geomesa stats iterator process on source type " + source.getClass.getName)

    if (properties != null) {
      if (query.getProperties != Query.ALL_PROPERTIES) {
        logger.warn(s"Overriding inner query's properties (${query.getProperties}) with properties / transforms $properties.")
      }
      query.setPropertyNames(properties)
    }
    query.getHints.put(QueryHints.STATS_STRING, statString)
    query.getHints.put(QueryHints.ENCODE_STATS, new java.lang.Boolean(encode))
    source.getFeatures(query)
  }
}

case class StatsIteratorResult(results: SimpleFeatureCollection) extends AbstractCalcResult
