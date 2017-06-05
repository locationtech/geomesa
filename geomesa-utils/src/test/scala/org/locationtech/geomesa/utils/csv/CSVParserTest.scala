/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.csv

import java.lang.{Double => jDouble, Integer => jInt}

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.csv.CSVParser._
import org.locationtech.geomesa.utils.csv.DMS.North
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class CSVParserTest extends Specification {

  "CSVParser" should {
    "parse ints" in {
      val i = 1
      IntParser.parse(i.toString) must beASuccessfulTry(new jInt(i))
    }
    "fail to parse invalid ints" in {
      IntParser.parse("not valid") must beAFailedTry
    }
    "parse doubles" in {
      val d = 1.0
      DoubleParser.parse(d.toString) must beASuccessfulTry(new jDouble(d))
    }
    "parse doubles in DMS" in {
      val dms = DMS(38,4,31.17,North)
      DoubleParser.parse(dms.toString) must beASuccessfulTry(new jDouble(dms.toDouble))
    }
    "fail to parse invalid doubles" in {
      DoubleParser.parse("not valid") must beAFailedTry
    }
    "parse times" in {
      val time = new DateTime
      TimeParser.timeFormats.forall(f =>
        TimeParser.parse(f.print(time)).map(_.getTime / 1000) must beASuccessfulTry(time.getMillis / 1000)
      )
    }
    "fail to parse invalid times" in {
      TimeParser.parse("not valid") must beAFailedTry
    }
    "parse points"  in {
      val pointStr = "POINT(0.0 0.0)"
      PointParser.parse(pointStr) must beASuccessfulTry(WKTUtils.read(pointStr))
    }
    "parse points in DMS" in {
      val dmsPtStr = "38:04:31.17N -78:29:42.32E"
      val dmsPtX = -78.495089
      val dmsPtY =  38.075325
      val eps = 0.000001
      val Success(resultPt) = PointParser.parse(dmsPtStr)
      math.abs(resultPt.getX - dmsPtX) must beLessThan(eps)
      math.abs(resultPt.getY - dmsPtY) must beLessThan(eps)
    }
    "fail to parse invalid points" in {
      PointParser.parse("not valid") must beAFailedTry
    }
    "parse LineStrings"  in {
      val wkt = "LINESTRING(0 2, 2 0, 8 6)"
      val geom = WKTUtils.read(wkt)
      LineStringParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "fail to parse invalid LineStrings" in {
      LineStringParser.parse("POINT(0.0 0.0)") must beAFailedTry
    }
    "parse Polygons"  in {
      val wkt = "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))"
      val geom = WKTUtils.read(wkt)
      PolygonParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "fail to parse invalid Polygons" in {
      PolygonParser.parse("POINT(0.0 0.0)") must beAFailedTry
    }
    "parse MultiLineStrings"  in {
      val wkt = "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))"
      val geom = WKTUtils.read(wkt)
      MultiLineStringParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "fail to parse invalid MultiLineStrings" in {
      MultiLineStringParser.parse("POINT(0.0 0.0)") must beAFailedTry
    }
    "parse MultiPoints"  in {
      val wkt = "MULTIPOINT(0 0, 2 2)"
      val geom = WKTUtils.read(wkt)
      MultiPointParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "fail to parse invalid MultiPoints" in {
      MultiPointParser.parse("POINT(0.0 0.0)") must beAFailedTry
    }
    "parse MultiPolygons"  in {
      val wkt = "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))"
      val geom = WKTUtils.read(wkt)
      MultiPolygonParser.parse(wkt) must beASuccessfulTry(geom)
    }
    "fail to parse invalid MultiPolygons" in {
      MultiPolygonParser.parse("POINT(0.0 0.0)") must beAFailedTry
    }
    "parse Geometries"  in {
      val ptWkt = "POINT(1 1)"
      val pt = WKTUtils.read(ptWkt)
      GeometryParser.parse(ptWkt) must beASuccessfulTry(pt)
      val lineWkt = "LINESTRING(0 2, 2 0, 8 6)"
      val line = WKTUtils.read(lineWkt)
      GeometryParser.parse(lineWkt) must beASuccessfulTry(line)
    }
    "fail to parse invalid Geometries" in {
      GeometryParser.parse("not valid") must beAFailedTry
    }
    "parse strings" in {
      val s = "argle"
      StringParser.parse(s) must beASuccessfulTry(s)
    }
  }
}
