/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import java.util.Collections

import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.referencing.GeodeticCalculator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.spatial.BBOX
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KnnProcessTest extends Specification {

  import scala.collection.JavaConverters._

  val process = new KNearestNeighborSearchProcess

  val sft = SimpleFeatureTypes.createType("knn", "*geom:Point:srid=4326")
  val geom = org.locationtech.geomesa.filter.ff.property("geom")

  val features = Seq.tabulate[SimpleFeature](10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"POINT(45 5$i)")
  }
  val featureCollection = new ListFeatureCollection(sft, features.asJava)

  val diagonalFeatures = Seq.tabulate[SimpleFeature](90) { lat =>
    ScalaSimpleFeature.create(sft, s"$lat", f"POINT($lat%d $lat%d)")
  }

  val polarFeatures = Seq.range(-180, 181).map { lon =>
    ScalaSimpleFeature.create(sft, s"$lon", f"POINT($lon%d 89.9)")
  }

  val cville = WKTUtils.read("POINT(-78.4953560 38.0752150)").asInstanceOf[Point]

  "KnnProcess" should {

    "manually visit a feature collection" in {
      val input = Collections.singletonList[SimpleFeature](ScalaSimpleFeature.create(sft, "", "POINT (45 55)"))
      val query = new ListFeatureCollection(sft, input)
      val result = SelfClosingIterator(process.execute(query, featureCollection, 3, 0, Double.MaxValue).features).toSeq
      result must containTheSameElementsAs(features.slice(4, 7))
    }

    "manually visit a feature collection smaller than k" in {
      val input = Collections.singletonList[SimpleFeature](ScalaSimpleFeature.create(sft, "", "POINT (45 55)"))
      val query = new ListFeatureCollection(sft, input)
      val result = SelfClosingIterator(process.execute(query, featureCollection, 20, 0, Double.MaxValue).features).toSeq
      result must containTheSameElementsAs(features)
    }

    "inject a small BBOX into a larger query" in {
      val filter = ECQL.toFilter("prop like 'foo' AND bbox(geom, -80, 30, -70, 40)")
      val base = new Query("", filter)

      val p = GeoHash("dqb0tg").getCentroid
      val window = new KnnWindow(base, geom, p, 10, 1000, 10000)

      // generate a new query
      val updated = window.next(None)

      // verify that original is untouched
      base.getFilter mustEqual filter

      val decomposed = org.locationtech.geomesa.filter.decomposeAnd(updated.getFilter)

      decomposed must haveLength(3)
      val bounds = decomposed.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      bounds must haveLength(2)

      // verify that the original filter is present
      bounds must contain((-80d, -70d, 30d, 40d))
      // verify the window filter
      val expected = Envelope(p, 1000, new GeodeticCalculator)
      bounds must contain((expected.xmin, expected.xmax, expected.ymin, expected.ymax))
    }

    "calculate features close to the equator" in {
      val k = 10
      val p = WKTUtils.read("POINT(0.1 0.2)").asInstanceOf[Point]
      val nearest = Array.ofDim[FeatureWithDistance](k)
      val calculator = new KnnCalculator(p, k, Double.MaxValue, nearest)
      diagonalFeatures.foreach(calculator.visit)
      nearest.map(_.sf.getID).toSeq must containTheSameElementsAs((0 until k).map(_.toString))
    }

    "calculate features close to Southwest Russia" in {
      val k = 10
      val p = WKTUtils.read("POINT(45.1 45.1)").asInstanceOf[Point]
      val nearest = Array.ofDim[FeatureWithDistance](k)
      val calculator = new KnnCalculator(p, k, Double.MaxValue, nearest)
      diagonalFeatures.foreach(calculator.visit)
      nearest.map(_.sf.getID).toSeq must containTheSameElementsAs((41 to 50).map(_.toString))
    }

    "calculate features close to the North Pole" in {
      val k = 10
      val p = WKTUtils.read("POINT(89.9 89.9)").asInstanceOf[Point]
      val nearest = Array.ofDim[FeatureWithDistance](k)
      val calculator = new KnnCalculator(p, k, Double.MaxValue, nearest)
      diagonalFeatures.foreach(calculator.visit)
      nearest.map(_.sf.getID).toSeq must containTheSameElementsAs((80 to 89).map(_.toString))
    }

    "calculate northern features close to the North Pole" in {
      val k = 10
      val p = WKTUtils.read("POINT(89.9 89.9)").asInstanceOf[Point]
      val nearest = Array.ofDim[FeatureWithDistance](k)
      val calculator = new KnnCalculator(p, k, Double.MaxValue, nearest)
      polarFeatures.foreach(calculator.visit)

      nearest.map(_.sf.getID).toSeq must containTheSameElementsAs((85 to 94).map(_.toString))
    }

    "calculate more things near the north polar region" in {
      val k = 10
      val p = WKTUtils.read("POINT(0.0001 89.9)").asInstanceOf[Point]
      val nearest = Array.ofDim[FeatureWithDistance](k)
      val calculator = new KnnCalculator(p, k, Double.MaxValue, nearest)
      polarFeatures.foreach(calculator.visit)
      diagonalFeatures.foreach(calculator.visit)

      nearest.map(_.sf.getID).toSeq must containTheSameElementsAs((-4 to 5).map(_.toString))
    }

    "double the window when no features are found" in {
      val window = new KnnWindow(new Query(""), geom, cville, 10, 500d, 5000d)
      // note: compare envelope -> referenced envelope, otherwise comparison doesn't work
      Envelope(cville, 500d, new GeodeticCalculator).toJts mustEqual
          BoundsFilterVisitor.visit(window.next(None).getFilter)
      Envelope(cville, 1000d, new GeodeticCalculator).toJts mustEqual
          BoundsFilterVisitor.visit(window.next(Some(0)).getFilter)
    }

    "expand the window correctly around Charlottesville" in {
      val window = new KnnWindow(new Query(""), geom, cville, 10, 500d, 5000d)

      val initial = window.next(None).getFilter
      window.radius mustEqual 500d
      initial must beAnInstanceOf[BBOX]
      val inner = Envelope(cville, window.radius, new GeodeticCalculator)
      // note: compare envelope -> referenced envelope, otherwise comparison doesn't work
      inner.toJts mustEqual initial.asInstanceOf[BBOX].getBounds

      // can get a multi-polygon and visualize these queries using:
      // expanded.map(_.toString.drop(20).dropRight(2)).mkString("MULTIPOLYGON(", ",", ")")
      val expanded = org.locationtech.geomesa.filter.decomposeOr(window.next(Some(5)).getFilter)

      window.radius must beCloseTo(797.88, 0.01) // math.sqrt(10 / (math.Pi * (5 / (4 * 500d * 500d))))
      expanded must haveLength(4)
      val bounds = expanded.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      bounds must haveLength(4)
      bounds must containTheSameElementsAs {
        Seq(
          (-78.50444946042865, -78.50105448126463, 38.07071041202179, 38.07971958797821),
          (-78.48965751873537, -78.48626253957136, 38.07071041202179, 38.07971958797821),
          (-78.50444946042865, -78.48626253957136, 38.07971958797821, 38.08240328075565),
          (-78.50444946042865, -78.48626253957136, 38.06802671924435, 38.07071041202179)
        )
      }
    }

    "expand the window correctly around the anti-meridian near Suva, Fiji" in {
      val p = WKTUtils.read("POINT (178.440 -18.140)").asInstanceOf[Point]
      val window = new KnnWindow(new Query(""), geom, p, 10, 250000d, 750000d)

      val initial = org.locationtech.geomesa.filter.decomposeOr(window.next(None).getFilter)

      window.radius mustEqual 250000d
      initial must haveLength(2)
      val initialBounds = initial.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      initialBounds must haveLength(2)
      initialBounds must containTheSameElementsAs(
        Seq(
          (176.07760736902137, 180.0, -20.39844934356917, -15.881550656430832),
          (-180.0, -179.19760736902137, -20.39844934356917, -15.881550656430832)
        )
      )

      // can get a multi-polygon and visualize these queries using:
      // expanded.map(_.toString.drop(20).dropRight(2)).mkString("MULTIPOLYGON(", ",", ")")
      val expanded = org.locationtech.geomesa.filter.decomposeOr(window.next(Some(7)).getFilter)

      window.radius must beCloseTo(337167.76, 0.01) // math.sqrt(10 / (math.Pi * (7 / (4 * 250000d * 250000d))))
      expanded must haveLength(6)
      val expandedBounds = expanded.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      expandedBounds must haveLength(6)
      expandedBounds must containTheSameElementsAs(
        Seq(
          (175.2540034973006, 180.0,-15.881550656430832, -15.0942293591859),
          (175.2540034973006, 180.0,-21.185770640814102, -20.39844934356917),
          (175.2540034973006, 176.07760736902137, -20.39844934356917, -15.881550656430832),
          (-180.0, -178.3740034973006, -15.881550656430832, -15.0942293591859),
          (-180.0, -178.3740034973006, -21.185770640814102, -20.39844934356917),
          (-179.19760736902137, -178.3740034973006, -20.39844934356917, -15.881550656430832)
        )
      )
    }

    "expand the window correctly around the polar region near McMurdo Station" in {
      val p = WKTUtils.read("POINT (166.68360 -77.842)").asInstanceOf[Point]
      val window = new KnnWindow(new Query(""), geom, p, 10, 250000d, 750000d)

      val initial = org.locationtech.geomesa.filter.decomposeOr(window.next(None).getFilter)

      window.radius mustEqual 250000d
      initial must haveLength(1)
      val initialBounds = initial.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      initialBounds must haveLength(1)
      initialBounds mustEqual Seq((156.16673876448903, 177.200461235511, -80.08109073174205, -75.60290926825795))

      // can get a multi-polygon and visualize these queries using:
      // expanded.map(_.toString.drop(20).dropRight(2)).mkString("MULTIPOLYGON(", ",", ")")
      val expanded = org.locationtech.geomesa.filter.decomposeOr(window.next(Some(7)).getFilter)

      window.radius must beCloseTo(337167.76, 0.01) // math.sqrt(10 / (math.Pi * (7 / (4 * 250000d * 250000d))))
      expanded must haveLength(5)
      val expandedBounds = expanded.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      expandedBounds must haveLength(5)
      expandedBounds must containTheSameElementsAs(
        Seq(
          (152.61949285269546,180.0,-75.60290926825795,-74.82227697359694),
          (152.61949285269546,180.0,-80.86172302640306,-80.08109073174205),
          (152.61949285269546,156.16673876448903,-80.08109073174205,-75.60290926825795),
          (177.200461235511,180.0,-80.08109073174205,-75.60290926825795),
          (-180.0,-179.25229285269543,-80.86172302640306,-74.82227697359694)
        )
      )

      val double = org.locationtech.geomesa.filter.decomposeOr(window.next(Some(9)).getFilter)

      window.radius must beCloseTo(401032.76, 0.01) // math.sqrt(10 / (math.Pi * (9 / (4 * 337167.76 * 337167.76))))
      double must haveLength(6)
      val doubleBounds = double.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      doubleBounds must haveLength(6)
      doubleBounds must containTheSameElementsAs(
        Seq(
          (150.08274179277637, 180.0, -74.82227697359694, -74.2503545564565),
          (150.08274179277637, 180.0, -81.4336454435435, -80.86172302640306),
          (150.08274179277637, 152.61949285269546, -80.86172302640306, -74.82227697359694),
          (-180.0, -176.71554179277635, -74.82227697359694, -74.2503545564565),
          (-180.0, -176.71554179277635, -81.4336454435435, -80.86172302640306),
          (-179.25229285269543, -176.71554179277635, -80.86172302640306, -74.82227697359694)
        )
      )
    }

    "expand the window correctly around the north pole" in {
      val p = WKTUtils.read("POINT(89.9 89.9)").asInstanceOf[Point]
      val window = new KnnWindow(new Query(""), geom, p, 10, 250000d, 750000d)

      val initial = org.locationtech.geomesa.filter.decomposeOr(window.next(None).getFilter)

      window.radius mustEqual 250000d
      initial must haveLength(1)
      val initialBounds = initial.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      initialBounds must haveLength(1)
      initialBounds mustEqual Seq((-180.0, 180.0, 87.66172837475138, 90.0))

      // can get a multi-polygon and visualize these queries using:
      // expanded.map(_.toString.drop(20).dropRight(2)).mkString("MULTIPOLYGON(", ",", ")")
      val expanded = org.locationtech.geomesa.filter.decomposeOr(window.next(Some(7)).getFilter)

      window.radius must beCloseTo(337167.76, 0.01) // math.sqrt(10 / (math.Pi * (7 / (4 * 250000d * 250000d))))
      expanded must haveLength(1)
      val expandedBounds = expanded.collect {
        case b: BBOX => (b.getBounds.getMinX, b.getBounds.getMaxX, b.getBounds.getMinY, b.getBounds.getMaxY)
      }
      expandedBounds must haveLength(1)
      expandedBounds mustEqual Seq((-180.0, 180.0, 86.88129440297135, 87.66172837475138))
    }

    "stop after reaching the threshold" in {
      val window = new KnnWindow(new Query(""), geom, cville, 10, 500d, 800d)
      window.hasNext must beTrue
      window.next(None)
      window.hasNext must beTrue
      window.next(Some(5))
      window.hasNext must beTrue
      val max = window.next(Some(5)).getFilter
      window.hasNext must beFalse
      // note: compare envelope -> referenced envelope, otherwise comparison doesn't work
      Envelope(cville, 800d, new GeodeticCalculator).toJts mustEqual BoundsFilterVisitor.visit(max)
    }

    "convert cartesian envelopes to world bounds" in {
      // simple in-bounds case
      Envelope(-10, 10, -5, 5).toWorld mustEqual Seq(Envelope(-10, 10, -5, 5))

      // anti-meridian left
      Envelope(-200, -140, -45, 45).toWorld must containTheSameElementsAs(
        Seq(Envelope(160, 180, -45, 45), Envelope(-180, -140, -45, 45))
      )
      // anti-meridian right
      Envelope(165, 185, -45, 45).toWorld must containTheSameElementsAs(
        Seq(Envelope(165, 180, -45, 45), Envelope(-180, -175, -45, 45))
      )

      // pole-crossing south
      Envelope(60, 80, -95, -75).toWorld mustEqual Seq(Envelope(-180, 180, -90, -75))
      // pole-crossing north
      Envelope(60, 80, 60, 110).toWorld mustEqual Seq(Envelope(-180, 180, 60, 90))

      // wider than world bounds
      Envelope(-200, 200, -100, 100).toWorld mustEqual Seq(Envelope(-180, 180, -90, 90))
      Envelope(-200, 200, -45, 45).toWorld mustEqual Seq(Envelope(-180, 180, -45, 45))
      Envelope(-5, 5, -100, 100).toWorld mustEqual Seq(Envelope(-180, 180, -90, 90))
    }

    "calculate diffs between query envelopes" in {
      // contains the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 15, -7.5, 7.5)) must beEmpty
      // contained within the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(-5, 5, -2.5, 2.5)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 2.5, 5), Envelope(-10, 10, -5, -2.5), Envelope(-10, -5, -2.5, 2.5), Envelope(5, 10, -2.5, 2.5))
      )
      // disjoint
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, -10, -7.5, 7.5)) mustEqual Seq(Envelope(-10, 10, -5, 5))
      Envelope(10, 15, -15, -10).minus(Envelope(0, 5, -20, -15)) mustEqual Seq(Envelope(10, 15, -15, -10))
      Envelope(10, 15, -15, -10).minus(Envelope(0, 5, -5, 0)) mustEqual Seq(Envelope(10, 15, -15, -10))
      // side-laps the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, -5, -2.5, 2.5)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 2.5, 5), Envelope(-10, 10, -5, -2.5), Envelope(-5, 10, -2.5, 2.5))
      )
      Envelope(-10, 10, -5, 5).minus(Envelope(5, 15, -2.5, 2.5)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 2.5, 5), Envelope(-10, 10, -5, -2.5), Envelope(-10, 5, -2.5, 2.5))
      )
      // top/bottom-laps the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(-5, 5, -2.5, 10)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, -5, -2.5), Envelope(-10, -5, -2.5, 5), Envelope(5, 10, -2.5, 5))
      )
      Envelope(-10, 10, -5, 5).minus(Envelope(-5, 5, -10, 2.5)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 2.5, 5), Envelope(-10, -5, -5, 2.5), Envelope(5, 10, -5, 2.5))
      )
      // corner-laps the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(0, 15, 0, 10)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, -5, 0), Envelope(-10, 0, 0, 5))
      )
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 0, -10, 0)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 0, 5), Envelope(0, 10, -5, 0))
      )
      Envelope(-10, 10, -5, 5).minus(Envelope(0, 15, -10, 0)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, 0, 5), Envelope(-10, 0, -5, 0))
      )
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 0, 0, 10)) must containTheSameElementsAs(
        Seq(Envelope(-10, 10, -5, 0), Envelope(0, 10, 0, 5))
      )
      // half-laps the other envelope
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 15, 0, 10)) mustEqual Seq(Envelope(-10, 10, -5, 0))
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 15, -10, 0)) mustEqual Seq(Envelope(-10, 10, 0, 5))
      Envelope(-10, 10, -5, 5).minus(Envelope(-15, 0, -10, 10)) mustEqual Seq(Envelope(0, 10, -5, 5))
      Envelope(-10, 10, -5, 5).minus(Envelope(0, 15, -10, 10)) mustEqual Seq(Envelope(-10, 0, -5, 5))
    }

    "calculate non-overlapping query windows" in {
      // initial query window
      QueryEnvelope(Envelope(149, 151, -86, -84), None).query mustEqual Seq(Envelope(149, 151, -86, -84))
      // expands but still within world bounds
      QueryEnvelope(Envelope(145, 155, -87.5, -82.5), Some(Envelope(149, 151, -86, -84))).query must containTheSameElementsAs(
        Seq(Envelope(145, 155, -87.5, -86), Envelope(145, 155, -84, -82.5), Envelope(145, 149, -86, -84), Envelope(151, 155, -86, -84))
      )
      // expands past the poles
      QueryEnvelope(Envelope(130, 170, -95, -75), Some(Envelope(145, 155, -87.5, -82.5))).query must containTheSameElementsAs(
        Seq(Envelope(-180, 180, -90, -87.5), Envelope(-180, 180, -82.5, -75), Envelope(-180, 145, -87.5, -82.5), Envelope(155, 180, -87.5, -82.5))
      )
    }
  }
}
