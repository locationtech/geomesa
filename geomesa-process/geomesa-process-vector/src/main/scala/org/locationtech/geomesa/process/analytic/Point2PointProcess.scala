/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(title = "Point2PointProcess", description = "Aggregates a collection of points into a collection of line segments")
class Point2PointProcess extends GeoMesaProcess {

  private val baseType = SchemaBuilder.builder().addLineString("geom", default = true).build("point2point")

  private val gf = JTSFactoryFinder.getGeometryFactory

  @DescribeResult(name = "result", description = "Aggregated feature collection")
  def execute(

               @DescribeParameter(name = "data", description = "Input feature collection")
               data: SimpleFeatureCollection,

               @DescribeParameter(name = "groupingField", description = "Field on which to group")
               groupingField: String,

               @DescribeParameter(name = "sortField", description = "Field on which to sort (must be Date type)")
               sortField: String,

               @DescribeParameter(name = "minimumNumberOfPoints", description = "Minimum number of points")
               minPoints: Int,

               @DescribeParameter(name = "breakOnDay", description = "Break connections on day marks")
               breakOnDay: Boolean,

               @DescribeParameter(name = "filterSingularPoints", description = "Filter out segments that fall on the same point", defaultValue = "true")
               filterSingularPoints: Boolean

               ): SimpleFeatureCollection = {

    import org.locationtech.geomesa.utils.geotools.Conversions._

    val queryType = data.getSchema
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(baseType)
    val groupingFieldIndex = data.getSchema.indexOf(groupingField)
    sftBuilder.add(queryType.getAttributeDescriptors.get(groupingFieldIndex))
    val sortFieldIndex = data.getSchema.indexOf(sortField)
    val sortDesc = queryType.getAttributeDescriptors.get(sortFieldIndex)
    val sortAttrName = sortDesc.getLocalName
    val sortType = sortDesc.getType.getBinding
    sftBuilder.add(s"${sortAttrName}_start", sortType)
    sftBuilder.add(s"${sortAttrName}_end", sortType)

    val sft = sftBuilder.buildFeatureType()
    val builder = new SimpleFeatureBuilder(sft)

    val lineFeatures =
      SelfClosingIterator(data.features()).toList
        .groupBy(f => String.valueOf(f.getAttribute(groupingFieldIndex)))
        .filter { case (_, coll) => coll.lengthCompare(minPoints) > 0 }
        .flatMap { case (_, coll) =>

          val globalSorted = coll.sortBy(_.get[java.util.Date](sortFieldIndex))

          val groups = if (!breakOnDay) { Array(globalSorted) } else {
            globalSorted
              .groupBy { f => getDayOfYear(sortFieldIndex, f) }
              .filter { case (_, g) => g.lengthCompare(2) >= 0 }  // need at least two points in a day to create a
              .map { case (_, g) => g }.toArray
          }

          val results = groups.flatMap { sorted =>
            sorted.sliding(2).zipWithIndex.map { case (ptLst, idx) =>
              import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
              val pts = ptLst.map(_.point.getCoordinate)
              val length = JTS.orthodromicDistance(pts.head, pts.last, DefaultGeographicCRS.WGS84)

              val group    = ptLst.head.getAttribute(groupingFieldIndex)
              val startDtg = ptLst.head.getAttribute(sortAttrName)
              val endDtg   = ptLst.last.getAttribute(sortAttrName)
              val attrs    = Array[AnyRef](gf.createLineString(pts.toArray), group, startDtg, endDtg)
              val sf = builder.buildFeature(s"$group-$idx", attrs)
              (length, sf)
            }
          }

          if (filterSingularPoints) {
            results.collect { case (length, sf) if length > 0.0 => sf }
          } else {
            results.map { case (_, sf) => sf }
          }
        }

    import scala.collection.JavaConversions._

    new ListFeatureCollection(sft, lineFeatures.toList)
  }

  def getDayOfYear(sortFieldIndex: Int, f: SimpleFeature): Int =
    ZonedDateTime.ofInstant(toInstant(f.getAttribute(sortFieldIndex).asInstanceOf[Date]), ZoneOffset.UTC).getDayOfYear
}
