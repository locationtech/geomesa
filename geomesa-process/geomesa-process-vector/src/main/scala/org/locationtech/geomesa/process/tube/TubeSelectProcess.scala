/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.EmptyFeatureCollection
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.iterators.DeduplicatingSimpleFeatureIterator
import org.locationtech.jts.geom._
import org.opengis.feature.Feature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

@DescribeProcess(
  title = "Tube Select",
  description = "Performs a tube select on a Geomesa feature collection based on another feature collection"
)
class TubeSelectProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "tubeFeatures",
                 description = "Input feature collection (must have geometry and datetime)")
               tubeFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "featureCollection",
                 description = "The data set to query for matching features")
               featureCollection: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the featureCollection")
               filter: Filter,

               @DescribeParameter(
                 name = "maxSpeed",
                 min = 0,
                 description = "Max speed of the object in m/s for nofill & line gapfill methods")
               maxSpeed: java.lang.Long,

               @DescribeParameter(
                 name = "maxTime",
                 min = 0,
                 description = "Time as seconds for nofill & line gapfill methods")
               maxTime: java.lang.Long,

               @DescribeParameter(
                 name = "bufferSize",
                 min = 0,
                 description = "Buffer size in meters to use instead of maxSpeed/maxTime calculation")
               bufferSize: java.lang.Double,

               @DescribeParameter(
                 name = "maxBins",
                 min = 0,
                 description = "Number of bins to use for breaking up query into individual queries")
               maxBins: java.lang.Integer,

               @DescribeParameter(
                 name = "gapFill",
                 min = 0,
                 description = "Method of filling gap (nofill, line)")
               gapFill: String

               ): SimpleFeatureCollection = {

    logger.debug("Tube selecting on collection type "+featureCollection.getClass.getName)

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(
                                      tubeFeatures,
                                      featureCollection,
                                      Option(filter).getOrElse(Filter.INCLUDE),
                                      Option(maxSpeed).getOrElse(0L).asInstanceOf[Long],
                                      Option(maxTime).getOrElse(0L).asInstanceOf[Long],
                                      Option(bufferSize).getOrElse(0.0).asInstanceOf[Double],
                                      Option(maxBins).getOrElse(0).asInstanceOf[Int],
                                      Option(gapFill).map(GapFill.withName).getOrElse(GapFill.NOFILL))

    GeoMesaFeatureCollection.visit(featureCollection, tubeVisitor)

    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }
}

object GapFill extends Enumeration{
  type GapFill = Value
  val NOFILL: Value       = Value("nofill")
  val LINE: Value         = Value("line")
  val INTERPOLATED: Value = Value("interpolated")
}

class TubeVisitor(val tubeFeatures: SimpleFeatureCollection,
                  val featureCollection: SimpleFeatureCollection,
                  val filter: Filter = Filter.INCLUDE,
                  val maxSpeed: Long,
                  val maxTime: Long,
                  val bufferSize: Double,
                  val maxBins: Int,
                  val gapFill: GapFill.GapFill = GapFill.NOFILL)
    extends GeoMesaProcessVisitor with LazyLogging {

  var resultCalc: TubeResult = TubeResult(new EmptyFeatureCollection(featureCollection.getSchema))

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  private val bufferDistance = if (bufferSize > 0) { bufferSize } else { maxSpeed * maxTime }

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {

    import org.locationtech.geomesa.filter.ff

    logger.debug("Visiting source type: "+source.getClass.getName)

    val geomProperty = ff.property(source.getSchema.getGeometryDescriptor.getName)
    val dateProperty = ff.property(source.getSchema.getUserData.get(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY).asInstanceOf[String])

    logger.debug("Querying with date property: "+dateProperty)
    logger.debug("Querying with geometry property: "+geomProperty)

    // Create a time binned set of tube features with no gap filling

    val tubeBuilder = gapFill match {
      case GapFill.LINE => new LineGapFill(tubeFeatures, bufferDistance, maxBins)
      case GapFill.INTERPOLATED => new InterpolatedGapFill(tubeFeatures, bufferDistance, maxBins)
      case _ => new NoGapFill(tubeFeatures, bufferDistance, maxBins)
    }

    val tube = tubeBuilder.createTube

    val queryResults = CloseableIterator(tube).flatMap { sf =>
      val sfMin = tubeBuilder.getStartTime(sf).getTime
      val minDate = new Date(sfMin - maxTime*1000)

      val sfMax = tubeBuilder.getEndTime(sf).getTime
      val maxDate = new Date(sfMax + maxTime*1000)

      val dtg1 = ff.greater(dateProperty, ff.literal(minDate))
      val dtg2 = ff.less(dateProperty, ff.literal(maxDate))

      val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]

      // Eventually these can be combined into OR queries and the QueryPlanner can create multiple Accumulo Ranges
      // Buf for now we issue multiple queries
      val geoms = (0 until geom.getNumGeometries).toIterator.map(geom.getGeometryN)
      SelfClosingIterator(geoms).flatMap { g =>
        val geomFilter = ff.intersects(geomProperty, ff.literal(g))
        val combinedFilter = ff.and(List(query.getFilter, geomFilter, dtg1, dtg2, filter))
        SelfClosingIterator(source.getFeatures(combinedFilter).features)
      }
    }

    // Time slices may not be disjoint so we have to buffer results and dedupe for now
    val collection = new ListFeatureCollection(source.getSchema)
    WithClose(new DeduplicatingSimpleFeatureIterator(queryResults))(_.foreach(collection.add))

    resultCalc = TubeResult(collection)
  }
}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult