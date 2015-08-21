package org.locationtech.geomesa.process

import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.joda.time.DateTime.Property
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.GeometryType
import org.opengis.feature.simple.SimpleFeature

import scala.util.Try

@DescribeProcess(title = "Point2PointProcess", description = "Aggregates a collection of points into a linestring.")
class Point2PointProcess extends VectorProcess {

  private val baseType = SimpleFeatureTypes.createType("geomesa", "point2point", "length:Double,*ls:LineString:srid=4326")
  private val gf = JTSFactoryFinder.getGeometryFactory

  @DescribeResult(name = "result", description = "Aggregated feature collection")
  def execute(

               @DescribeParameter(name = "data", description = "Input feature collection")
               data: SimpleFeatureCollection,

               @DescribeParameter(name = "groupingField", description = "Field on which to group")
               groupingField: String,

               @DescribeParameter(name = "sortField", description = "Field on which to sort")
               sortField: String,

               @DescribeParameter(name = "minimumNumberOfPoints", description = "Minimum number of points")
               minPoints: Int,

               @DescribeParameter(name = "breakOnDay", description = "Break connections on day marks")
               breakOnDay: Boolean

               ): SimpleFeatureCollection = {

    import org.locationtech.geomesa.utils.geotools.Conversions._

    import scala.collection.JavaConversions._

    val queryType = data.getSchema
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(baseType)
    queryType.getAttributeDescriptors
      .filterNot { _.getType.isInstanceOf[GeometryType] }
      .foreach { attr => sftBuilder.add(attr) }

    val sft = sftBuilder.buildFeatureType()

    val builder = new SimpleFeatureBuilder(sft)

    val groupingFieldIndex = data.getSchema.indexOf(groupingField)
    val sortFieldIndex = data.getSchema.indexOf(sortField)

    val lineFeatures =
      data.features().toList
        .groupBy(_.get(groupingFieldIndex).asInstanceOf[String])
        .filter { case (_, coll) => coll.size > minPoints }
        .flatMap { case (group, coll) =>

        val globalSorted = coll.sortBy(_.get(sortFieldIndex).asInstanceOf[java.util.Date])

        val groups =
          if(!breakOnDay) Array(globalSorted)
          else
            globalSorted
              .groupBy { f => getDayOfYear(sortFieldIndex, f) }
              .filter { case (_, g) => g.size >= 2 }  // need at least two points in a day to create a
              .map { case (_, g) => g }.toArray

        groups.flatMap { sorted =>
          Try {
            val pts = sorted.map(_.getDefaultGeometry.asInstanceOf[Point].getCoordinate)
            val ls = gf.createLineString(pts.toArray)
            val length = pts.sliding(2, 1).map { case List(s, e) => JTS.orthodromicDistance(s, e, DefaultGeographicCRS.WGS84) }.sum
            val sf = builder.buildFeature(group)
            sf.setAttributes(Array[AnyRef](Double.box(length), ls) ++ sorted.head.getAttributes)
            sf
          }.toOption
        }
      }

    DataUtilities.collection(lineFeatures.toArray)
  }

  def getDayOfYear(sortFieldIndex: Int, f: SimpleFeature): Property =
    new DateTime(f.getAttribute(sortFieldIndex).asInstanceOf[Date]).dayOfYear()
}
