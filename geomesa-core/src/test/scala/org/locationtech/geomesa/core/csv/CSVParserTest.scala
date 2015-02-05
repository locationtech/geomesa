/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.csv

import java.lang.{Integer => jInt, Double => jDouble}

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.csv.DMS.North
import org.locationtech.geomesa.core.csv.CSVParser._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class CSVParserTest extends Specification {

  val i = 1
  val d = 1.0
  val dms = DMS(38,4,31.17,North)
  val time = new DateTime
  val pointStr = "POINT(0.0 0.0)"
  val dmsPtStr = "38:04:31.17N -78:29:42.32E"
  val dmsPtX = -78.495089
  val dmsPtY =  38.075325
  val s = "argle"

  val eps = 0.000001

  "CSVParser" should {
    "parse ints" in {
      IntParser.parse(i.toString) must beASuccessfulTry(new jInt(i))
    }
    "parse doubles" in {
      DoubleParser.parse(d.toString) must beASuccessfulTry(new jDouble(d))
    }
    "parse doubles in DMS" in {
      DoubleParser.parse(dms.toString) must beASuccessfulTry(new jDouble(dms.toDouble))
    }
    "parse times"   in {
      TimeParser.timeFormats.forall(f =>
        TimeParser.parse(f.print(time)).map(_.getTime / 1000) must beASuccessfulTry(time.getMillis / 1000)
      )
    }
    "parse points"  in {
      PointParser.parse(pointStr) must beASuccessfulTry(WKTUtils.read(pointStr))
    }
    "parse points in DMS" in {
      val Success(resultPt) = PointParser.parse(dmsPtStr)
      math.abs(resultPt.getX - dmsPtX) must beLessThan(eps)
      math.abs(resultPt.getY - dmsPtY) must beLessThan(eps)
    }
    "parse LineStrings"  in {
      val wkt = "LINESTRING(0 2, 2 0, 8 6)"
      val geom = WKTUtils.read(wkt)
      LineStringParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse Polygons"  in {
      val wkt = "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))"
      val geom = WKTUtils.read(wkt)
      PolygonParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse MultiLineStrings"  in {
      val wkt = "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))"
      val geom = WKTUtils.read(wkt)
      MultiLineStringParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse MultiPoints"  in {
      val wkt = "MULTIPOINT(0 0, 2 2)"
      val geom = WKTUtils.read(wkt)
      MultiPointParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse MultiPolygons"  in {
      val wkt = "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))"
      val geom = WKTUtils.read(wkt)
      MultiPolygonParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse Geometries"  in {
      val wkt = "POINT(1 1)"
      val geom = WKTUtils.read(wkt)
      GeometryParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "parse strings" in {
      StringParser.parse(s) must beASuccessfulTry(s)
    }
  }
}
