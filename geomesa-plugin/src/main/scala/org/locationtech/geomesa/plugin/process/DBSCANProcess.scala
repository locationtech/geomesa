/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.process

import java.util.UUID

import org.apache.commons.math3.ml.clustering.{Clusterable, DBSCANClusterer}
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geometry.jts.{GeometryBuilder, JTSFactoryFinder}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

@DescribeProcess(
  title = "Density Based Spatial Clustering of Applications with Noise",
  description = "Clustering"
)
class DBSCANProcess {

  import org.locationtech.geomesa.utils.geotools.Conversions._
  private val outputSFT = SimpleFeatureTypes.createType("geomesa:dbscan", "radius:Double,geom:Polygon:srid=4326")
  private val builder = AvroSimpleFeatureFactory.featureBuilder(outputSFT)
  private val geomFactory = JTSFactoryFinder.getGeometryFactory
  private val geomBuilder = new GeometryBuilder(geomFactory)

  @DescribeResult(
    name = "layerName",
    `type` = classOf[SimpleFeatureCollection],
    description = "Name of the new featuretype, with workspace")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "Input feature collection")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "eps",
                 description = "Epsilon error")
               eps: Double,

               @DescribeParameter(
                 name = "minPts",
                 description = "The minimum number of points to form a cluster")
               minPts: Int

               ): SimpleFeatureCollection = {

    val calc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)

    val clusterer = new DBSCANClusterer[FeatureClusterable](eps, minPts, new DistanceMeasure {
      override def compute(p1: Array[Double], p2: Array[Double]): Double = {
        calc.setStartingGeographicPoint(p1(0), p1(1))
        calc.setDestinationGeographicPoint(p2(0), p2(1))
        calc.getOrthodromicDistance
      }
    })

    val clusters = clusterer.cluster(features.features().toList.map(new FeatureClusterable(_)))
    val resultFeatures = clusters.flatMap { c =>
      val points = c.getPoints.map(_.sf.point)
      val geom = geomFactory.buildGeometry(points)
      val pt = geom.getCentroid
      val radius = points.map { p => p.distance(pt) }.max
      if(radius > 0.0) {
        val circle = geomBuilder.circle(pt.getX, pt.getY, radius, 20)
        builder.reset()
        builder.add(radius)
        builder.add(circle)
        val feature = builder.buildFeature(UUID.randomUUID().toString)
        Some(feature)
      } else None
    }
    DataUtilities.collection(resultFeatures)
  }

  class FeatureClusterable(val sf: SimpleFeature) extends Clusterable {
    override def getPoint: Array[Double] = {
      val coord = sf.point.getCoordinate
      Array(coord.x, coord.y)
    }
  }

}
