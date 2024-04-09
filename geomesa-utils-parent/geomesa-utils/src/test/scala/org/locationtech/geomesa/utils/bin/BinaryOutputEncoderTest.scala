/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.bin

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{LineString, Point}
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/bin/BinaryOutputEncoderTest.scala
import org.opengis.feature.simple.SimpleFeature
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/bin/BinaryOutputEncoderTest.scala
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat

@RunWith(classOf[JUnitRunner])
class BinaryOutputEncoderTest extends Specification {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getDate(d: String): java.util.Date = synchronized { dateFormat.parse(d) }

  "BinaryViewerOutputFormat" should {

    "encode a point feature collection" in {
      val sft = SimpleFeatureTypes.createType("bintest",
        "track:String,label:Long,lat:Double,lon:Double,dtg:Date,geom:Point:srid=4326")
      val baseDtg = getDate("2014-01-01 08:09:00").getTime

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 4).foreach { i =>
        val point = WKTUtils.read(s"POINT (45 5$i)")
        val date = s"2014-01-01T08:0${9-i}:00.000Z"
        builder.addAll(s"1234-$i", Long.box(10 + i), Double.box(45 + i), Double.box(50), date, point)
        fc.add(builder.buildFeature(s"$i"))
      }

      "with label field" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(4), Some(0), Some(1)))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 24, (i + 1) * 24))
          decoded.dtg mustEqual baseDtg - 60 * 1000 * i
          decoded.lat mustEqual 50 + i
          decoded.lon mustEqual 45
          decoded.trackId mustEqual s"1234-$i".hashCode
          decoded.label mustEqual BinaryOutputEncoder.convertToLabel(Long.box(10 + i))
        }
      }

      "without label field" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(4), Some(0), None))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual baseDtg - 60 * 1000 * i
          decoded.lat mustEqual 50 + i
          decoded.lon mustEqual 45
          decoded.trackId mustEqual s"1234-$i".hashCode
          decoded.label mustEqual -1L
        }
      }

      "with id field" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(4), None, None))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual baseDtg - 60 * 1000 * i
          decoded.lat mustEqual 50 + i
          decoded.lon mustEqual 45
          decoded.trackId mustEqual s"$i".hashCode
          decoded.label mustEqual -1L
        }
      }
    }

    "encode a line feature collection" in {
      val sft = SimpleFeatureTypes.createType("binlinetest",
        "track:String,label:Long,dtg:Date,dates:List[Date],geom:LineString:srid=4326")
      val line = WKTUtils.read("LINESTRING(45 50, 46 51, 47 52, 50 55)")
      val date = getDate("2014-01-01 08:00:00")
      val dates = (0 until 4).map(i => getDate(s"2014-01-01 08:00:0${9-i}"))

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 1).foreach { i =>
        builder.addAll(s"1234-$i", java.lang.Long.valueOf(10 + i), date, dates, line)
        fc.add(builder.buildFeature(s"$i"))
      }

      "with label field" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(3), Some(0), Some(1)))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 24, (i + 1) * 24))
          decoded.dtg mustEqual dates(i).getTime
          decoded.lat mustEqual line.getCoordinates()(i).y.toFloat
          decoded.lon mustEqual line.getCoordinates()(i).x.toFloat
          decoded.trackId mustEqual "1234-0".hashCode
          decoded.label mustEqual BinaryOutputEncoder.convertToLabel(Long.box(10))
        }
      }

      "without label field" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(3), Some(0), None))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual dates(i).getTime
          decoded.lat mustEqual line.getCoordinates()(i).y.toFloat
          decoded.lon mustEqual line.getCoordinates()(i).x.toFloat
          decoded.trackId mustEqual "1234-0".hashCode
          decoded.label mustEqual -1L
        }
      }

      "with sorting" >> {
        val out = new ByteArrayOutputStream()
        val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(3), Some(0), None))
        encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out, sort = true)
        val encoded = out.toByteArray
        forall(0 until 4) { i =>
          val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual dates(3 - i).getTime
          decoded.lat mustEqual line.getCoordinates()(3 - i).y.toFloat
          decoded.lon mustEqual line.getCoordinates()(3 - i).x.toFloat
          decoded.trackId mustEqual "1234-0".hashCode
          decoded.label mustEqual -1L
        }
      }
    }

    "encode timestamps" in {
      val sft = {
        // use a builder to create the Timestamp binding, we bind it to classOf[Date] in SimpleFeatureTypes
        val builder = new SimpleFeatureTypeBuilder
        builder.setName("bintest")
        builder.add("track", classOf[String])
        builder.add("dtg", classOf[java.sql.Timestamp])
        builder.add("geom", classOf[Point], org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
        builder.buildFeatureType()
      }
      val baseDtg = getDate("2014-01-01 08:09:00").getTime

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 4).foreach { i =>
        val point = WKTUtils.read(s"POINT (45 5$i)")
        val date = s"2014-01-01T08:0${9-i}:00.000Z"
        builder.addAll(s"1234-$i", date, point)
        fc.add(builder.buildFeature(s"$i"))
      }

      val out = new ByteArrayOutputStream()
      val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(1), Some(0), None))
      encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
      val encoded = out.toByteArray
      forall(0 until 4) { i =>
        val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
        decoded.dtg mustEqual baseDtg - 60 * 1000 * i
        decoded.lat mustEqual 50 + i
        decoded.lon mustEqual 45
        decoded.trackId mustEqual s"1234-$i".hashCode
        decoded.label mustEqual -1L
      }
    }

    "encode lists of timestamps" in {
      val sft = {
        // use a builder to create the Timestamp binding, we bind it to classOf[Date] in SimpleFeatureTypes
        val builder = new SimpleFeatureTypeBuilder
        builder.setName("binlinetest")
        builder.add("track", classOf[String])
        builder.add("dtg", classOf[java.sql.Timestamp])
        builder.userData(SimpleFeatureTypes.AttributeConfigs.UserDataListType, classOf[java.sql.Timestamp].getName)
        builder.add("dates", classOf[java.util.List[java.sql.Timestamp]])
        builder.add("geom", classOf[LineString], org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
        builder.buildFeatureType()
      }
      val line = WKTUtils.read("LINESTRING(45 50, 46 51, 47 52, 50 55)")
      val date = getDate("2014-01-01 08:00:00")
      val dates = (0 until 4).map(i => getDate(s"2014-01-01 08:00:0${9-i}"))

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 1).foreach { i =>
        builder.addAll(s"1234-$i", date, dates, line)
        fc.add(builder.buildFeature(s"$i"))
      }

      val out = new ByteArrayOutputStream()
      val encoder = BinaryOutputEncoder(fc.getSchema, EncodingOptions(None, Some(2), Some(0), None))
      encoder.encode(CloseableIterator(fc.features).asInstanceOf[Iterator[SimpleFeature]], out)
      val encoded = out.toByteArray
      forall(0 until 4) { i =>
        val decoded = BinaryOutputEncoder.decode(encoded.slice(i * 16, (i + 1) * 16))
        decoded.dtg mustEqual dates(i).getTime
        decoded.lat mustEqual line.getCoordinates()(i).y.toFloat
        decoded.lon mustEqual line.getCoordinates()(i).x.toFloat
        decoded.trackId mustEqual "1234-0".hashCode
        decoded.label mustEqual -1L
      }
    }
  }
}