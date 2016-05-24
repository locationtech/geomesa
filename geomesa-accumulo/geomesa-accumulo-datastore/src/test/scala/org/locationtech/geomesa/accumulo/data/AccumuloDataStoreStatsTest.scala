/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.stats.AttributeBounds
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}
import org.locationtech.geomesa.utils.stats.{Frequency, RangeHistogram, Z3RangeHistogram}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreStatsTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326"

  val baseMillis = {
    val sf = new ScalaSimpleFeature("", sft)
    sf.setAttribute(1, "2016-01-04T00:00:00.000Z")
    sf.getAttribute(1).asInstanceOf[Date].getTime
  }

  val dayInMillis = new DateTime(baseMillis, DateTimeZone.UTC).plusDays(1).getMillis - baseMillis

  "AccumuloDataStore" should {
    "track stats for ingested features" >> {

      "initially have global stats" >> {
        ds.stats.getCount(sft) must beNone
        ds.stats.getBounds(sft) mustEqual wholeWorldEnvelope
        ds.stats.getAttributeBounds[String](sft, "name") must beNone
        ds.stats.getAttributeBounds[Date](sft, "dtg") must beNone
      }

      "through feature writer append" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(0, "alpha")
        sf.setAttribute(1, "2016-01-04T00:00:00.000Z")
        sf.setAttribute(2, "POINT (0 0)")
        writer.write()
        writer.flush()

        ds.stats.getCount(sft) must beSome(1)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 0, 0, 0, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[String](sft, "name") must beSome(AttributeBounds("alpha", "alpha", 1))
        ds.stats.getAttributeBounds[Date](sft, "dtg") must beSome(AttributeBounds(new Date(baseMillis), new Date(baseMillis), 1))

        val sf2 = writer.next()
        sf2.setAttribute(0, "cappa")
        sf2.setAttribute(1, "2016-01-04T12:00:00.000Z")
        sf2.setAttribute(2, "POINT (10 10)")
        writer.write()
        writer.close()

        ds.stats.getCount(sft) must beSome(2)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 10, 0, 10, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[String](sft, "name") must beSome(AttributeBounds("alpha", "cappa", 2))
        ds.stats.getAttributeBounds[Date](sft, "dtg") must
            beSome(AttributeBounds(new Date(baseMillis), new Date(baseMillis + dayInMillis / 2), 2))
      }

      "through feature source add features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature("collection1", sft)
        sf.setAttribute(0, "gamma")
        sf.setAttribute(1, "2016-01-05T00:00:00.000Z")
        sf.setAttribute(2, "POINT (-10 -10)")

        val features = new DefaultFeatureCollection()
        features.add(sf)
        fs.addFeatures(features)

        ds.stats.getCount(sft) must beSome(3)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[String](sft, "name") must beSome (AttributeBounds("alpha", "gamma", 3))
        ds.stats.getAttributeBounds[Date](sft, "dtg") must
            beSome(AttributeBounds(new Date(baseMillis), new Date(baseMillis + dayInMillis), 3))
      }

      "not expand bounds when not necessary" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(0, "beta")
        sf.setAttribute(1, "2016-01-04T00:00:00.000Z")
        sf.setAttribute(2, "POINT (0 0)")
        writer.write()
        writer.close()

        ds.stats.getCount(sft) must beSome(4)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[String](sft, "name") must beSome(AttributeBounds("alpha", "gamma", 4))
        ds.stats.getAttributeBounds[Date](sft, "dtg") must
            beSome(AttributeBounds(new Date(baseMillis), new Date(baseMillis + dayInMillis), 3))
      }

      "through feature source set features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature("", sft)
        sf.setAttribute(0, "0")
        sf.setAttribute(1, "2016-01-03T00:00:00.000Z")
        sf.setAttribute(2, "POINT (15 0)")

        val features = new SimpleFeatureReader() {
          val iter = Iterator(sf)
          override def next(): SimpleFeature = iter.next()
          override def hasNext: Boolean = iter.hasNext
          override def getFeatureType: SimpleFeatureType = sft
          override def close(): Unit = {}
        }

        fs.setFeatures(features)

        ds.stats.getCount(sft) must beSome(1)
        // note - we don't reduce bounds during delete...
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-10, 15, -10, 10, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[Date](sft, "dtg") must
            beSome(AttributeBounds(new Date(baseMillis - dayInMillis), new Date(baseMillis + dayInMillis), 4))
      }

      "update all stats" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        (0 until 10).foreach { i =>
          val sf = writer.next()
          sf.setAttribute(0, s"$i")
          sf.setAttribute(1, f"2016-01-${i + 1}%02dT00:00:00.000Z")
          sf.setAttribute(2, s"POINT (${i * 3} $i)")
          writer.write()
        }

        writer.close()

        // our expected values
        val minDate = new Date(baseMillis - 3 * dayInMillis)
        val maxDate = new Date(baseMillis + 6 * dayInMillis)
        val minGeom = WKTUtils.read("POINT (0 0)")
        val maxGeom = WKTUtils.read("POINT (27 9)")

        // execute a stat update so we have the latest values
        ds.stats.generateStats(sft)
        // run it twice so that all our bounds are exact for histograms
        ds.stats.generateStats(sft)

        ds.stats.getCount(sft) must beSome(11)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(0, 27, 0, 9, CRS_EPSG_4326)
        ds.stats.getAttributeBounds[String](sft, "name") must beSome(AttributeBounds("0", "9", 10))
        ds.stats.getAttributeBounds[Date](sft, "dtg") must beSome(AttributeBounds(minDate, maxDate, 10))

        val nameHistogram = ds.stats.getStats[RangeHistogram[String]](sft, Seq("name"))
        nameHistogram must haveLength(1)
        nameHistogram.head.bounds mustEqual ("0", "9")
        nameHistogram.head.length mustEqual 1000
        nameHistogram.head.count(nameHistogram.head.indexOf("0")) mustEqual 2
        forall(1 until 10)(i => nameHistogram.head.count(nameHistogram.head.indexOf(i.toString)) mustEqual 1)
        (0 until 1000).map(nameHistogram.head.count).sum mustEqual 11

        val nameFrequency = ds.stats.getStats[Frequency[String]](sft, Seq("name"))
        nameFrequency must haveLength(1)
        nameFrequency.head.count("0") mustEqual 2
        forall(1 until 10)(i => nameFrequency.head.count(i.toString) mustEqual 1)

        val dateHistogram = ds.stats.getStats[RangeHistogram[Date]](sft, Seq("dtg"))
        dateHistogram must haveLength(1)
        dateHistogram.head.bounds mustEqual (minDate, maxDate)
        dateHistogram.head.length mustEqual 1000
        dateHistogram.head.count(0) mustEqual 1
        dateHistogram.head.count(999) mustEqual 1
        (0 until 1000).map(dateHistogram.head.count).sum mustEqual 11

        val geomHistogram = ds.stats.getStats[RangeHistogram[Geometry]](sft, Seq("geom"))
        geomHistogram must haveLength(1)
        geomHistogram.head.bounds mustEqual (minGeom, maxGeom)
        val geoms = (0 until 10).map(i => WKTUtils.read(s"POINT (${i * 3} $i)"))
        forall(geoms)(g => geomHistogram.head.count(geomHistogram.head.indexOf(g)) mustEqual 1)

        val z3Histogram = ds.stats.getStats[Z3RangeHistogram](sft, Seq("geom", "dtg"))
        z3Histogram must haveLength(1)
        val dates = (0 until 10).map(i => new Date(minDate.getTime + i * dayInMillis))
        forall(geoms.zip(dates)) { z =>
          val index = z3Histogram.head.indexOf(z)
          index._2 must not(beEqualTo(-1))
          z3Histogram.head.count(index._1, index._2) must beBetween(1L, 2L)
        }

        success
      }

      "calculate exact counts" >> {
        // mock accumulo seems to really struggle with setting up the stat iterator
        // the call to batchScanner.iterator can take several seconds
        // this doesn't seem to happen with regular accumulo
        // so only test one of each query type here
        val filters = Seq("bbox(geom,0,0,10,5)", "name < '7'",
          "bbox(geom,0,0,10,5) AND dtg during 2016-01-03T00:00:00.000Z/2016-01-03T04:00:00.000Z")
        forall(filters.map(ECQL.toFilter)) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          val exact = SelfClosingIterator(reader).length
          val calculated = ds.stats.getCount(sft, filter, exact = true)
          exact must beGreaterThan(0)
          calculated must beSome(exact)
        }
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
          "bbox(geom,0,0,30,10) AND dtg during 2016-01-08T00:00:00.000Z/2016-01-10T00:00:00.000Z")
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
    }
  }
}
