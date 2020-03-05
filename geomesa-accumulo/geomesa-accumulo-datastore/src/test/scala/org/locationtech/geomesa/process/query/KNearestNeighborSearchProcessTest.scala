/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geohash.VincentyModel
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class KNearestNeighborSearchProcessTest extends TestWithFeatureType {

  sequential

  override val spec: String = "label:String,*geom:Point:srid=4326"

  val knn = new KNearestNeighborSearchProcess()

  val rng = new Random(0)

  val centerLat = 38.149894
  val centerLon = -79.073639
  val width = 0.30

  val uvaLawn = Seq(
    ScalaSimpleFeature.create(sft, "rotunda", "cville", "POINT( -78.503547 38.035475 )"),
    ScalaSimpleFeature.create(sft, "pavilion I", "cville", "POINT( -78.503923 38.035536 )"),
    ScalaSimpleFeature.create(sft, "pavilion II", "cville", "POINT( -78.503109 38.035278 )"),
    ScalaSimpleFeature.create(sft, "pavilion III", "cville", "POINT( -78.504059 38.035308 )"),
    ScalaSimpleFeature.create(sft, "pavilion IV", "cville", "POINT( -78.503180 38.035039 )"),
    ScalaSimpleFeature.create(sft, "pavilion V", "cville", "POINT( -78.504276 38.034971 )"),
    ScalaSimpleFeature.create(sft, "pavilion VI", "cville", "POINT( -78.503424 38.034721 )"),
    ScalaSimpleFeature.create(sft, "pavilion VII", "cville", "POINT( -78.504424 38.034628 )"),
    ScalaSimpleFeature.create(sft, "pavilion VIII", "cville", "POINT( -78.503601 38.034343 )"),
    ScalaSimpleFeature.create(sft, "pavilion IX", "cville", "POINT( -78.504617 38.034208 )"),
    ScalaSimpleFeature.create(sft, "pavilion X", "cville", "POINT( -78.503833 38.033938 )"),
    ScalaSimpleFeature.create(sft, "cabell", "cville", "POINT( -78.505152 38.032704 )"),
    ScalaSimpleFeature.create(sft, "beams", "cville", "POINT( -78.510295 38.034283 )"),
    ScalaSimpleFeature.create(sft, "mccormick", "cville", "POINT( -78.522288 38.032844 )"),
    ScalaSimpleFeature.create(sft, "hep", "cville", "POINT( -78.520019 38.034511 )")
  )

  // random points about a central point
  // note that these points will be uniform in cartesian space only
  val distributedPoints = Seq.tabulate(1000) { i =>
    val lon = centerLon + width * (rng.nextDouble() - 0.5)
    val lat = centerLat + width * (rng.nextDouble() - 0.5)
    ScalaSimpleFeature.create(sft, (100000 + i).toString, "cville", s"POINT($lon $lat)")
  }

  val diagonalFeatures = Seq.tabulate[SimpleFeature](90) { lat =>
    ScalaSimpleFeature.create(sft, s"$lat", "diagonal", f"POINT($lat%d $lat%d)")
  }

  val polarFeatures = Seq.range(-180, 181).map { lon =>
    ScalaSimpleFeature.create(sft, s"$lon", "polar", f"POINT($lon%d 89.9)")
  }

  step {
    addFeatures(uvaLawn)
    addFeatures(distributedPoints)
    addFeatures(diagonalFeatures)
    addFeatures(polarFeatures)
  }

  def collection(features: SimpleFeature*): SimpleFeatureCollection = {
    val fc = new DefaultFeatureCollection()
    features.foreach(fc.add)
    fc
  }

  // generates a single SimpleFeature
  def queryFeature(label: String, lon: Double, lat: Double): SimpleFeature =
    ScalaSimpleFeature.create(sft, label, label, f"POINT($lon $lat)")

  // generates a very loose query
  def wideQuery: Query = {
    val lat = 38.0
    val lon = -78.50
    val siteSize = 5.0
    val filter = s"BBOX(geom, ${lon - siteSize}, ${lat - siteSize}, ${lon + siteSize}, ${lat + siteSize})"
    new Query(sftName, ECQL.toFilter(filter))
  }

  "GeoMesaKNearestNeighborSearch" should {

    "handle an empty query point collection" in {
      val inputFeatures = collection()
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 100, 500d, 5000d).features()).toList
      res must beEmpty
    }

    "handle non-point geometries in inputFeatures by ignoring them" in {
      val sft = SimpleFeatureTypes.createType("lineStringKnn", "geom:LineString:srid=4326")
      val inputFeatures = collection(ScalaSimpleFeature.create(sft, "route 29", "LINESTRING(-78.491 38.062, -78.474 38.082)"))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 100, 500d, 5000d)).toList
      res must beEmpty
    }

    "find nothing within 10km of a single query point " in {
      val inputFeatures = collection(queryFeature("fan mountain", -78.692649, 37.878219))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 5, 1000d, 10000d)).toList
      res must beEmpty
    }

    "find 11 points within 400m of a point when k is set to 15 " in {
      val inputFeatures = collection(queryFeature("madison", -78.502720, 38.036871))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 15, 100d, 400d)).toList
      res must containTheSameElementsAs(uvaLawn.take(11))
    }

    "find nearest features around Charlottesville" in {
      val inputFeatures = collection(queryFeature("madison", -78.502720, 38.036871))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures(wideQuery)
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 15, 500d, 2500d)).toList
      res must containTheSameElementsAs(uvaLawn)
    }

    "find nearest features around Staunton" in {
      val k = 10
      val referenceFeature = queryFeature("blackfriars", -79.070569, 38.149185)
      val inputFeatures = collection(referenceFeature)
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures(wideQuery)
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 5000d, 50000d)).toList
      val directFeatures = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures().features).toList.sortBy { f =>
        VincentyModel.getDistanceBetweenTwoPoints(referenceFeature.point, f.point).getDistanceInMeters
      }
      res must containTheSameElementsAs(directFeatures.take(k))
    }

    "handle three query points, one of which will return nothing" in {
      val inputFeatures = collection(
        queryFeature("madison", -78.502720, 38.036871),
        queryFeature("fan mountain", -78.692649, 37.878219),
        queryFeature("blackfriars", -79.070569, 38.149185)
      )
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, 5, 500d, 5000d)).toList
      res must haveLength(10)
      res must containAllOf(uvaLawn.take(5))

      val directFeatures = collection(uvaLawn ++ distributedPoints: _*)
      val direct = SelfClosingIterator(knn.execute(inputFeatures, directFeatures, 5, 500d, 5000d)).toList
      res must containTheSameElementsAs(direct)
    }

    "find features close to the equator" in {
      val k = 10
      val inputFeatures = collection(queryFeature("", 0.1, 0.2))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 1000000d, 2000000d)).toList
      res.map(_.getID) must containTheSameElementsAs((0 until k).map(_.toString))
    }

    "find features close to Southwest Russia" in {
      val k = 10
      val inputFeatures = collection(queryFeature("", 45.1, 45.1))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 500000d, 1000000d)).toList
      res.map(_.getID) must containTheSameElementsAs((41 to 50).map(_.toString))
    }

    "find features close to the North Pole" in {
      val k = 10
      val inputFeatures = collection(queryFeature("", 89.9, 89.9))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures(new Query(sftName, ECQL.toFilter("label = 'diagonal'")))
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 700000d, 2000000d)).toList
      res.map(_.getID) must containTheSameElementsAs((80 to 89).map(_.toString))
    }

    "find northern features close to the North Pole" in {
      val k = 10
      val inputFeatures = collection(queryFeature("", 89.9, 89.9))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures(new Query(sftName, ECQL.toFilter("label = 'polar'")))
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 100000d, 1000000d)).toList
      res.map(_.getID) must containTheSameElementsAs((85 to 94).map(_.toString))
    }

    "find more things near the north polar region" in {
      val k = 10
      val inputFeatures = collection(queryFeature("", 0.0001, 89.9))
      val dataFeatures = ds.getFeatureSource(sftName).getFeatures()
      val res = SelfClosingIterator(knn.execute(inputFeatures, dataFeatures, k, 100000d, 1000000d)).toList
      res.map(_.getID) must containTheSameElementsAs((-4 to 5).map(_.toString))
    }
  }
}
