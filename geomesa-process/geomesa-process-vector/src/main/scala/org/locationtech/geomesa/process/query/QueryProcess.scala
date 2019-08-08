/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess, GeoMesaProcessVisitor}
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Geomesa Query",
  description = "Performs a Geomesa optimized query using spatiotemporal indexes"
)
class QueryProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the feature collection")
               filter: Filter,

               @DescribeParameter(
                 name = "properties",
                 min = 0,
                 max = 128,
                 collectionType = classOf[String],
                 description = "The properties/transforms to apply to the feature collection")
               properties: java.util.List[String] = null

             ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa query on type " + features.getClass.getName)

    val propsArray = Option(properties).map(_.toArray(Array.empty[String])).filter(_.length > 0).orNull

    val visitor = new QueryVisitor(features, Option(filter).getOrElse(Filter.INCLUDE), propsArray)
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}

class QueryVisitor(features: SimpleFeatureCollection, filter: Filter, properties: Array[String])
    extends GeoMesaProcessVisitor with LazyLogging {

  private val (sft, transformFeature) = if (properties == null) { (features.getSchema, null) } else {
    val original = features.getSchema
    val (transforms, transformSft) = QueryPlanner.buildTransformSFT(original, properties)
    val transformSf = TransformSimpleFeature(original, transformSft, transforms)
    (transformSft, transformSf)
  }

  private val retype: (SimpleFeature) => SimpleFeature =
    if (transformFeature == null) {
      (sf) => sf
    } else {
      (sf) => {
        transformFeature.setFeature(sf)
        ScalaSimpleFeature.create(transformFeature.getFeatureType, transformFeature)
      }
    }

  // normally handled in our query planner, but we are going to use the filter directly here
  private lazy val manualFilter = FastFilterFactory.optimize(features.getSchema, filter)
  private val manualVisitResults = new ListFeatureCollection(sft)
  private var resultCalc = FeatureResult(manualVisitResults)

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (manualFilter.evaluate(sf)) {
      manualVisitResults.add(retype(sf))
    }
  }

  override def getResult: FeatureResult = resultCalc

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Running Geomesa query on source type ${source.getClass.getName}")
    query.setFilter(org.locationtech.geomesa.filter.mergeFilters(query.getFilter, filter))
    if (properties != null && properties.length > 0) {
      if (query.getProperties != Query.ALL_PROPERTIES) {
        logger.warn(s"Overriding inner query's properties (${query.getProperties}) " +
            s"with properties/transforms ${properties.mkString(",")}.")
      }
      query.setPropertyNames(properties)
    }
    resultCalc = FeatureResult(source.getFeatures(query))
  }
}
