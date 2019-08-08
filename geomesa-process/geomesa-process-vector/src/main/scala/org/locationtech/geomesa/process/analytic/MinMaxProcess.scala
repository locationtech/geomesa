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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.process.analytic.MinMaxProcess.MinMaxVisitor
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Min/Max Process",
  description = "Gets attribute bounds for a data set"
)
class MinMaxProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
                 @DescribeParameter(
                   name = "features",
                   description = "The feature set on which to query")
                 features: SimpleFeatureCollection,

                 @DescribeParameter(
                   name = "attribute",
                   description = "The attribute to gather bounds for")
                 attribute: String,

                 @DescribeParameter(
                   name = "cached",
                   description = "Return cached values, if available",
                   min = 0, max = 1)
                 cached: java.lang.Boolean = null

             ): SimpleFeatureCollection = {

    require(attribute != null, "Attribute is a required field")

    logger.debug(s"Attempting min/max process on type ${features.getClass.getName}")

    val visitor = new MinMaxVisitor(features, attribute, Option(cached).forall(_.booleanValue()))
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}

object MinMaxProcess {

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
          resultCalc = ds.stats.getAttributeBounds[Any](source.getSchema, attribute, query.getFilter, !cached) match {
            case None     => createResult("{}")
            case Some(mm) => createResult(mm.toJson)
          }

        case ds =>
          logger.warn(s"Running unoptimized min/max query on ${ds.getClass.getName}")
          SelfClosingIterator(features.features).foreach(visit)
      }
    }
  }

  private def createResult(stat: String): FeatureResult = {
    val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "", Array(stat, GeometryUtils.zeroPoint))
    FeatureResult(new ListFeatureCollection(StatsScan.StatsSft, Array[SimpleFeature](sf)))
  }
}
