/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreStatsTest extends Specification with TestWithMultipleSfts {

  sequential

  // note: attributes that are not indexed but still collect stats only store bounds and topK
  val spec = "name:String:index=join,age:Int:keep-stats=true,height:Int,dtg:Date,*geom:Point:srid=4326"
  val sft  = createNewSchema(spec)
  val sftName = sft.getTypeName

  val baseMillis = {
    val sf = new ScalaSimpleFeature(sft, "")
    sf.setAttribute(3, "2016-01-04T00:00:00.000Z")
    sf.getAttribute(3).asInstanceOf[Date].getTime
  }

  val dayInMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(baseMillis), ZoneOffset.UTC).plusDays(1).toInstant.toEpochMilli - baseMillis

  "AccumuloDataStore" should {
    "track stats for ingested features" >> {

      "initially have global stats" >> {
        ds.stats.getCount(sft) must beNone
        ds.stats.getBounds(sft, Filter.INCLUDE, exact = false) mustEqual wholeWorldEnvelope
        ds.stats.getMinMax[String](sft, "name") must beNone
        ds.stats.getMinMax[Int](sft, "age") must beNone
        ds.stats.getMinMax[Date](sft, "dtg") must beNone
        ds.stats.getTopK[String](sft, "name") must beNone
        ds.stats.getTopK[Int](sft, "age") must beNone
        ds.stats.getTopK[Date](sft, "dtg") must beNone
        ds.stats.getFrequency[String](sft, "name", 0) must beNone
        ds.stats.getFrequency[Int](sft, "age", 0) must beNone
        ds.stats.getFrequency[Date](sft, "dtg", 0) must beNone
        ds.stats.getHistogram[String](sft, "name", 0, null, null) must beNone
        ds.stats.getHistogram[Int](sft, "age", 0, 0, 0) must beNone
        ds.stats.getHistogram[Date](sft, "dtg", 0, null, null) must beNone
        ds.stats.getHistogram[Geometry](sft, "geom", 0, null, null) must beNone
      }

      "through feature writer append" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(0, "alpha")
        sf.setAttribute(1, 10)
        sf.setAttribute(2, 10)
        sf.setAttribute(3, "2016-01-04T00:00:00.000Z")
        sf.setAttribute(4, "POINT (0 0)")
        writer.write()
        writer.flush()

        ds.stats.getCount(sft) must beSome(1L)

        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 0, 0, 0, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome(("alpha", "alpha", 1L))
        ds.stats.getMinMax[Int](sft, "age").map(_.tuple) must beSome((10, 10, 1L))
        ds.stats.getMinMax[String](sft, "height") must beNone
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must beSome((new Date(baseMillis), new Date(baseMillis), 1L))

        ds.stats.getTopK[String](sft, "name").map(_.topK(10).toSeq) must beSome(Seq(("alpha", 1L)))
        ds.stats.getTopK[Int](sft, "age").map(_.topK(10).toSeq) must beSome(Seq((10, 1L)))
        ds.stats.getTopK[Int](sft, "height") must beNone
        ds.stats.getTopK[Date](sft, "dtg") must beNone

        ds.stats.getFrequency[String](sft, "name", 0) must beSome
        ds.stats.getFrequency[Int](sft, "age", 0) must beNone
        ds.stats.getFrequency[Int](sft, "height", 0) must beNone
        ds.stats.getFrequency[Date](sft, "dtg", 0) must beNone

        ds.stats.getHistogram[String](sft, "name", 0, null, null) must beSome
        ds.stats.getHistogram[Int](sft, "age", 0, 0, 0) must beNone
        ds.stats.getHistogram[Date](sft, "dtg", 0, null, null) must beSome
        ds.stats.getHistogram[Geometry](sft, "geom", 0, null, null) must beSome

        val sf2 = writer.next()
        sf2.setAttribute(0, "cappa")
        sf2.setAttribute(1, 12)
        sf2.setAttribute(2, 12)
        sf2.setAttribute(3, "2016-01-04T12:00:00.000Z")
        sf2.setAttribute(4, "POINT (10 10)")
        writer.write()
        writer.close()

        ds.stats.getCount(sft) must beSome(2L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 10, 0, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome(("alpha", "cappa", 2L))
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must
            beSome((new Date(baseMillis), new Date(baseMillis + dayInMillis / 2), 2L))
      }

      "through feature source add features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature(sft, "collection1")
        sf.setAttribute(0, "gamma")
        sf.setAttribute(1, Int.box(15))
        sf.setAttribute(2, Int.box(15))
        sf.setAttribute(3, "2016-01-05T00:00:00.000Z")
        sf.setAttribute(4, "POINT (-10 -10)")

        val features = new DefaultFeatureCollection()
        features.add(sf)
        fs.addFeatures(features)

        ds.stats.getCount(sft) must beSome(3L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome (("alpha", "gamma", 3L))
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must
            beSome((new Date(baseMillis), new Date(baseMillis + dayInMillis), 3L))
      }

      "not expand bounds when not necessary" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(0, "beta")
        sf.setAttribute(1, 11)
        sf.setAttribute(2, 11)
        sf.setAttribute(3, "2016-01-04T00:00:00.000Z")
        sf.setAttribute(4, "POINT (0 0)")
        writer.write()
        writer.close()

        ds.stats.getCount(sft) must beSome(4L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome(("alpha", "gamma", 4L))
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must
            beSome((new Date(baseMillis), new Date(baseMillis + dayInMillis), 3L))
      }

      "through feature source set features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature(sft, "")
        sf.setAttribute(0, "0")
        sf.setAttribute(1, Int.box(10))
        sf.setAttribute(2, Int.box(10))
        sf.setAttribute(3, "2016-01-03T00:00:00.000Z")
        sf.setAttribute(4, "POINT (15 0)")

        val features: SimpleFeatureReader = new SimpleFeatureReader() {
          private val iter = Iterator.single(sf)
          override def next(): SimpleFeature = iter.next()
          override def hasNext: Boolean = iter.hasNext
          override def getFeatureType: SimpleFeatureType = sft
          override def close(): Unit = {}
        }

        fs.setFeatures(features)

        ds.stats.getCount(sft) must beSome(1L)
        // note - with setFeatures, stats get reset
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(15.0, 15.0, 0.0, 0.0, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must
            beSome((new Date(baseMillis - dayInMillis), new Date(baseMillis - dayInMillis), 1L))
      }

      "update all stats" >> {
        val deleter = ds.getFeatureWriter(sftName, Transaction.AUTO_COMMIT)
        while (deleter.hasNext) {
          deleter.next()
          deleter.remove()
        }
        deleter.close()

        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        (0 until 10).foreach { i =>
          val sf = writer.next()
          sf.setAttribute(0, s"$i")
          if (i < 3) {
            sf.setAttribute(1, Int.box(1))
          } else {
            sf.setAttribute(1, Int.box(2))
          }
          sf.setAttribute(2, Int.box(i))
          sf.setAttribute(3, f"2016-01-${i + 1}%02dT00:00:00.000Z")
          sf.setAttribute(4, s"POINT (${i * 3} $i)")
          writer.write()
        }

        writer.close()

        // our expected values
        val minDate = new Date(baseMillis - 3 * dayInMillis)
        val maxDate = new Date(baseMillis + 6 * dayInMillis)
        val minGeom = WKTUtils.read("POINT (0 0)")
        val maxGeom = WKTUtils.read("POINT (27 9)")

        // execute a stat update so we have the latest values
        ds.stats.writer.analyze(sft)
        // run it twice so that all our bounds are exact for histograms
        ds.stats.writer.analyze(sft)

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 27, 0, 9, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome(("0", "9", 10L))
        ds.stats.getMinMax[Int](sft, "age").map(_.tuple) must beSome((1, 2, 2L))
        ds.stats.getMinMax[Int](sft, "height") must beNone
        ds.stats.getMinMax[Date](sft, "dtg").map(_.tuple) must beSome((minDate, maxDate, 10L))

        val nameTopK = ds.stats.getTopK[String](sft, "name")
        nameTopK must beSome
        nameTopK.get.topK(10).toSeq must containTheSameElementsAs((0 until 10).map(i => (s"$i", 1)))

        val ageTopK = ds.stats.getTopK[Int](sft, "age")
        ageTopK must beSome
        ageTopK.get.topK(10).toSeq mustEqual Seq((2, 7), (1, 3))

        ds.stats.getTopK[Int](sft, "height") must beNone
        ds.stats.getTopK[Date](sft, "dtg") must beNone

        val nameFrequency = ds.stats.getFrequency[String](sft, "name", 0)
        nameFrequency must beSome
        forall(0 until 10)(i => nameFrequency.get.count(i.toString) mustEqual 1)

        ds.stats.getFrequency[Int](sft, "age", 0) must beNone
        ds.stats.getFrequency[Int](sft, "height", 0) must beNone

        val nameHistogram = ds.stats.getHistogram[String](sft, "name", 0, null, null)
        nameHistogram must beSome
        nameHistogram.get.bounds mustEqual ("0", "9")
        nameHistogram.get.length mustEqual 1000
        forall(0 until 10)(i => nameHistogram.get.count(nameHistogram.head.indexOf(i.toString)) mustEqual 1)
        (0 until 1000).map(nameHistogram.get.count).sum mustEqual 10

        ds.stats.getHistogram[Int](sft, "age", 0, 0, 0) must beNone
        ds.stats.getHistogram[Int](sft, "height", 0, 0, 0) must beNone

        val dateHistogram = ds.stats.getHistogram[Date](sft, "dtg", 0, null, null)
        dateHistogram must beSome
        dateHistogram.get.bounds mustEqual (minDate, maxDate)
        dateHistogram.get.length mustEqual 1000
        dateHistogram.get.count(0) mustEqual 1
        dateHistogram.get.count(999) mustEqual 1
        (0 until 1000).map(dateHistogram.get.count).sum mustEqual 10

        val geomHistogram = ds.stats.getHistogram[Geometry](sft, "geom", 0, null, null)
        geomHistogram must beSome
        geomHistogram.get.bounds mustEqual (minGeom, maxGeom)
        val geoms = (0 until 10).map(i => WKTUtils.read(s"POINT (${i * 3} $i)"))
        forall(geoms)(g => geomHistogram.get.count(geomHistogram.get.indexOf(g)) mustEqual 1)

        val z3Histogram = ds.stats.getZ3Histogram(sft, "geom", "dtg", null, 0)
        z3Histogram must beSome
        val dates = (0 until 10).map(i => new Date(minDate.getTime + i * dayInMillis))
        forall(geoms.zip(dates)) { z =>
          val index = z3Histogram.get.indexOf(z)
          index._2 must not(beEqualTo(-1))
          z3Histogram.get.count(index._1, index._2) must beBetween(1L, 2L)
        }
      }

      "calculate exact counts" >> {
        // mock accumulo seems to really struggle with setting up the stat iterator
        // the call to batchScanner.iterator can take several seconds
        // this doesn't seem to happen with regular accumulo
        // so only test one of each query type here
        val filters = Seq("bbox(geom,0,0,10,5)", "name < '7'",
          "bbox(geom,0,0,10,5) AND dtg during 2016-01-02T23:00:00.000Z/2016-01-03T01:00:00.000Z")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length.toLong
          exact must beGreaterThan(0L)
          val calculated = ds.stats.getCount(sft, filter, exact = true)
          calculated must beSome(exact)
        }
      }

      "handle empty queries with exact stats" >> {
        val filter = "dtg > '2019-01-01T00:00:00.000Z' AND dtg < '2019-01-02T00:00:00.000Z' AND dtg > currentDate('-P1D')"
        val calculated = ds.stats.getCount(sft, ECQL.toFilter(filter), exact = true)
        calculated must beSome(0L)
      }

      "estimate counts for spatial queries" >> {
        val filters = Seq("bbox(geom,0,0,10,5)", "bbox(geom,10,5,30,10)", "bbox(geom,0,0,30,10)",
          "bbox(geom,-30,-10,-5,0)", "dwithin(geom, POLYGON((0 0, 0 5, 5 7, 6 3, 0 0)), 100, meters)")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val estimated = ds.stats.getCount(sft, filter, exact = false)
          estimated must beSome(beCloseTo(exact, 3L))
        }
      }

      "estimate counts for temporal queries" >> {
        val filters = Seq("dtg during 2016-01-03T00:00:00.000Z/2016-01-05T00:00:00.000Z",
          "dtg during 2015-01-03T00:00:00.000Z/2015-01-05T00:00:00.000Z",
          "dtg during 2016-01-08T00:00:00.000Z/2016-01-10T00:00:00.000Z")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val estimated = ds.stats.getCount(sft, filter, exact = false)
          estimated must beSome(beCloseTo(exact, 3L))
        }
      }

      "estimate counts for spatio-temporal queries" >> {
        val filters = Seq("bbox(geom,0,0,10,5) AND dtg during 2016-01-03T00:00:00.000Z/2016-01-05T00:00:00.000Z",
          "bbox(geom,10,5,30,10) AND dtg during 2015-01-03T00:00:00.000Z/2015-01-05T00:00:00.000Z",
          "bbox(geom,0,0,30,10) AND dtg during 2016-01-08T00:00:00.000Z/2016-01-10T00:00:00.000Z",
          "bbox(geom,0,0,30,10) AND dtg during 2015-12-30T00:00:00.000Z/2016-01-01T00:00:00.000Z")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val estimated = ds.stats.getCount(sft, filter, exact = false)
          estimated must beSome(beCloseTo(exact, 3L))
        }
      }

      "estimate counts for attribute queries" >> {
        val filters = Seq("name = '5'", "name < '7'", "name > 'foo'", "NOT name = '3'")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val estimated = ds.stats.getCount(sft, filter, exact = false)
          estimated must beSome(beCloseTo(exact, 1L))
        }
      }

      "estimate counts for schemas without a date" >> {
        val sft = createNewSchema("name:String:index=join,*geom:Point:srid=4326")
        val reader = ds.getFeatureReader(new Query(AccumuloDataStoreStatsTest.this.sftName), Transaction.AUTO_COMMIT)
        val features = SelfClosingIterator(reader).map { f =>
          ScalaSimpleFeature.create(sft, f.getID, f.getAttribute("name"), f.getAttribute("geom"))
        }
        addFeatures(features.toSeq)
        val filters = Seq("name = '5'", "name < '7'", "name > 'foo'", "NOT name = '3'")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val estimated = ds.stats.getCount(sft, filter, exact = false)
          estimated must beSome(beCloseTo(exact, 1L))
        }
      }

      "not calculate stats when collection is disabled" >> {
        import scala.collection.JavaConversions._
        val params = dsParams ++ Map(AccumuloDataStoreParams.GenerateStatsParam.key -> false)
        val dsNoStats =  DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

        val fs = dsNoStats.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature(sft, "collection1")
        sf.setAttribute(0, "zed")
        sf.setAttribute(1, Int.box(100))
        sf.setAttribute(2, Int.box(100))
        sf.setAttribute(3, "2016-01-05T00:00:00.000Z")
        sf.setAttribute(4, "POINT (-100 -90)")

        val features = new DefaultFeatureCollection()
        features.add(sf)
        fs.addFeatures(features)

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 27, 0, 9, CRS_EPSG_4326)
        ds.stats.getMinMax[String](sft, "name").map(_.tuple) must beSome(("0", "9", 10L))
        ds.stats.getMinMax[Int](sft, "age").map(_.tuple) must beSome((1, 2, 2L))
        ds.stats.getMinMax[Int](sft, "height") must beNone
      }
    }
  }
}
