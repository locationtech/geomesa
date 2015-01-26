/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.filter.function

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryOutputEncoderTest extends Specification {

  "BinaryViewerOutputFormat" should {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    "encode a point feature collection" in {
      val sft = SimpleFeatureTypes.createType("bintest",
        "track:String,label:String,lat:Double,lon:Double,dtg:Date,geom:Point:srid=4326")
      val date = dateFormat.parse("2014-01-01 08:00:00")

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 4).foreach { i =>
        val point = WKTUtils.read(s"POINT (45 5$i)")
        builder.addAll(Array(s"1234-$i", s"label-$i", 45 + i, 50, date, point).asInstanceOf[Array[AnyRef]])
        fc.add(builder.buildFeature(s"$i"))
      }

      "with label field" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dtg", Some("track"), Some("label"), None, AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 24, (i + 1) * 24))
          decoded.dtg mustEqual date.getTime
          decoded.lat mustEqual 45
          decoded.lon mustEqual 50 + i
          decoded.trackId mustEqual Some(s"1234-$i").map(_.hashCode.toString)
          decoded.asInstanceOf[ExtendedValues].label mustEqual Some(s"label-$i")
        }
        success
      }

      "without label field" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dtg", Some("track"), None, None, AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual date.getTime
          decoded.lat mustEqual 45
          decoded.lon mustEqual 50 + i
          decoded.trackId mustEqual Some(s"1234-$i").map(_.hashCode.toString)
          decoded must beAnInstanceOf[BasicValues]
        }
        success
      }

      "with id field" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dtg", Some("id"), None, None, AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual date.getTime
          decoded.lat mustEqual 45
          decoded.lon mustEqual 50 + i
          decoded.trackId mustEqual Some(s"$i").map(_.hashCode.toString)
          decoded must beAnInstanceOf[BasicValues]
        }
        success
      }

      "without custom lat/lon" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dtg", Some("track"), None, Some(("lat", "lon")), AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual date.getTime
          decoded.lat mustEqual 45 + i
          decoded.lon mustEqual 50
          decoded.trackId mustEqual Some(s"1234-$i").map(_.hashCode.toString)
          decoded must beAnInstanceOf[BasicValues]
        }
        success
      }
    }

    "encode a line feature collection" in {
      val sft = SimpleFeatureTypes.createType("binlinetest",
        "track:String,label:String,dtg:Date,dates:List[Date],geom:LineString:srid=4326")
      val line = WKTUtils.read("LINESTRING(45 50, 46 51, 47 52, 50 55)")
      val date = dateFormat.parse("2014-01-01 08:00:00")
      val dates = (0 until 4).map(i => dateFormat.parse(s"2014-01-01 08:00:0$i"))

      val fc = new ListFeatureCollection(sft)
      val builder = new SimpleFeatureBuilder(sft)
      (0 until 1).foreach { i =>
        builder.addAll(Array[AnyRef](s"1234-$i", s"label-$i", date, dates, line))
        fc.add(builder.buildFeature(s"$i"))
      }

      "with label field" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dates", Some("track"), Some("label"), None, AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 24, (i + 1) * 24))
          decoded.dtg mustEqual dates(i).getTime
          decoded.lat mustEqual line.getCoordinates()(i).x.toFloat
          decoded.lon mustEqual line.getCoordinates()(i).y.toFloat
          decoded.trackId mustEqual Some("1234-0").map(_.hashCode.toString)
          decoded.asInstanceOf[ExtendedValues].label mustEqual Some("label-0")
        }
        success
      }

      "without label field" >> {
        val out = new ByteArrayOutputStream()
        BinaryOutputEncoder
            .encodeFeatureCollection(fc, out, "dates", Some("track"), None, None, AxisOrder.LatLon, false)
        val encoded = out.toByteArray
        (0 until 4).foreach { i =>
          val decoded = Convert2ViewerFunction.decode(encoded.slice(i * 16, (i + 1) * 16))
          decoded.dtg mustEqual dates(i).getTime
          decoded.lat mustEqual line.getCoordinates()(i).x.toFloat
          decoded.lon mustEqual line.getCoordinates()(i).y.toFloat
          decoded.trackId mustEqual Some("1234-0").map(_.hashCode.toString)
          decoded must beAnInstanceOf[BasicValues]
        }
        success
      }
    }
  }
}