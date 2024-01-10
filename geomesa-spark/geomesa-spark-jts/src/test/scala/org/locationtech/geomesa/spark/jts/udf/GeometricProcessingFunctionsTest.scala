/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.{Geometry, Polygon}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometricProcessingFunctionsTest extends Specification with TestEnvironment {

  "sql geometry constructors" should {
    sequential

    // before
    step {
      // Trigger initialization of spark session
      val _ = spark
    }
    "st_bufferPoint" >> {

      "should handle nulls" >> {
        sc.sql("select st_bufferPoint(null, null)").collect.head(0) must beNull
        dfBlank.select(st_bufferPoint(lit(null), lit(null))).first must beNull
      }

      "should return a point buffered in meters" >> {
        val buf = sc.sql("select st_bufferPoint(st_makePoint(0, 0), 10)").collect().head.get(0)
        val dfBuf = dfBlank.select(st_bufferPoint(st_makePoint(lit(0), lit(0)), lit(10))).first

        val bufferedPoly = WKTUtils.read(
          """
          |POLYGON ((0.0000899320367762 0, 0.0000897545764446 0.0000056468793115, 0.0000892228958048 0.0000112714729702, 0.0000883390931573 0.0000168515832745, 0.0000871066564674 0.0000223651880784, 0.0000855304495997 0.0000277905277026, 0.0000836166931225 0.0000331061908102, 0.0000813729397584 0.0000382911989076, 0.0000788080445769 0.0000433250891364, 0.0000759321300474 0.0000481879950317, 0.0000727565460907 0.0000528607249257, 0.0000692938252858 0.0000573248376881, 0.0000655576334099 0.0000615627155054, 0.0000615627155054 0.0000655576334099, 0.0000573248376881 0.0000692938252858, 0.0000528607249257 0.0000727565460907, 0.0000481879950317 0.0000759321300474, 0.0000433250891364 0.0000788080445769, 0.0000382911989076 0.0000813729397584, 0.0000331061908102 0.0000836166931225, 0.0000277905277026 0.0000855304495997, 0.0000223651880784 0.0000871066564674, 0.0000168515832745 0.0000883390931573, 0.0000112714729702 0.0000892228958048, 0.0000056468793115 0.0000897545764446, -0 0.0000899320367762, -0.0000056468793115 0.0000897545764446, -0.0000112714729702 0.0000892228958048, -0.0000168515832745 0.0000883390931573, -0.0000223651880784 0.0000871066564674, -0.0000277905277026 0.0000855304495997, -0.0000331061908102 0.0000836166931225, -0.0000382911989076 0.0000813729397584, -0.0000433250891364 0.0000788080445769, -0.0000481879950317 0.0000759321300474, -0.0000528607249257 0.0000727565460907, -0.0000573248376881 0.0000692938252858, -0.0000615627155054 0.0000655576334099, -0.0000655576334099 0.0000615627155054, -0.0000692938252858 0.0000573248376881, -0.0000727565460907 0.0000528607249257, -0.0000759321300474 0.0000481879950317, -0.0000788080445769 0.0000433250891364, -0.0000813729397584 0.0000382911989076, -0.0000836166931225 0.0000331061908102, -0.0000855304495997 0.0000277905277026, -0.0000871066564674 0.0000223651880784, -0.0000883390931573 0.0000168515832745, -0.0000892228958048 0.0000112714729702, -0.0000897545764446 0.0000056468793115, -0.0000899320367762 -0, -0.0000897545764446 -0.0000056468793115, -0.0000892228958048 -0.0000112714729702, -0.0000883390931573 -0.0000168515832745, -0.0000871066564674 -0.0000223651880784, -0.0000855304495997 -0.0000277905277026, -0.0000836166931225 -0.0000331061908102, -0.0000813729397584 -0.0000382911989076, -0.0000788080445769 -0.0000433250891364, -0.0000759321300474 -0.0000481879950317, -0.0000727565460907 -0.0000528607249257, -0.0000692938252858 -0.0000573248376881, -0.0000655576334099 -0.0000615627155054, -0.0000615627155054 -0.0000655576334099, -0.0000573248376881 -0.0000692938252858, -0.0000528607249257 -0.0000727565460907, -0.0000481879950317 -0.0000759321300474, -0.0000433250891364 -0.0000788080445769, -0.0000382911989076 -0.0000813729397584, -0.0000331061908102 -0.0000836166931225, -0.0000277905277026 -0.0000855304495997, -0.0000223651880784 -0.0000871066564674, -0.0000168515832745 -0.0000883390931573, -0.0000112714729702 -0.0000892228958048, -0.0000056468793115 -0.0000897545764446, -0 -0.0000899320367762, 0.0000056468793115 -0.0000897545764446, 0.0000112714729702 -0.0000892228958048, 0.0000168515832745 -0.0000883390931573, 0.0000223651880784 -0.0000871066564674, 0.0000277905277026 -0.0000855304495997, 0.0000331061908102 -0.0000836166931225, 0.0000382911989076 -0.0000813729397584, 0.0000433250891364 -0.0000788080445769, 0.0000481879950317 -0.0000759321300474, 0.0000528607249257 -0.0000727565460907, 0.0000573248376881 -0.0000692938252858, 0.0000615627155054 -0.0000655576334099, 0.0000655576334099 -0.0000615627155054, 0.0000692938252858 -0.0000573248376881, 0.0000727565460907 -0.0000528607249257, 0.0000759321300474 -0.0000481879950317, 0.0000788080445769 -0.0000433250891364, 0.0000813729397584 -0.0000382911989076, 0.0000836166931225 -0.0000331061908102, 0.0000855304495997 -0.0000277905277026, 0.0000871066564674 -0.0000223651880784, 0.0000883390931573 -0.0000168515832745, 0.0000892228958048 -0.0000112714729702, 0.0000897545764446 -0.0000056468793115, 0.0000899320367762 0))
        """.stripMargin)

        buf.asInstanceOf[Polygon].equalsExact(bufferedPoly, 0.000001) must beTrue
        dfBuf.asInstanceOf[Polygon].equalsExact(bufferedPoly, 0.000001) must beTrue
      }

      "should handle antimeridian" >> {
        val buf = sc.sql("select st_bufferPoint(st_makePoint(-180, 50), 100000)").first.getAs[Geometry](0)
        val dfBuf = dfBlank.select(st_bufferPoint(st_makePoint(lit(-180), lit(50)), lit(100000))).first
        // check points on both sides of antimeridian
        buf.contains(WKTUtils.read("POINT(-179.9 50)")) mustEqual true
        buf.contains(WKTUtils.read("POINT(179.9 50)")) mustEqual true

        dfBuf.contains(WKTUtils.read("POINT(-179.9 50)")) mustEqual true
        dfBuf.contains(WKTUtils.read("POINT(179.9 50)")) mustEqual true
      }
    }

    "st_antimeridianSafeGeom" >> {
      "should handle nulls" >> {
        sc.sql("select st_antimeridianSafeGeom(null)").collect.head(0) must beNull
        dfBlank.select(st_antimeridianSafeGeom(lit(null))).first must beNull
      }

      "should split a geom that spans the antimeridian" >> {

        val wkt = "POLYGON((-190 50, -190 60, -170 60, -170 50, -190 50))"
        val geom = s"st_geomFromWKT('$wkt')"
        val decomposed = sc.sql(s"select st_antimeridianSafeGeom($geom)").first.getAs[Geometry](0)
        val dfDecomposed = dfBlank.select(st_antimeridianSafeGeom(st_geomFromWKT(lit(wkt)))).first

        val expected = WKTUtils.read(
          "MULTIPOLYGON (((-180 50, -180 60, -170 60, -170 50, -180 50)), ((180 60, 180 50, 170 50, 170 60, 180 60)))")
        decomposed mustEqual expected
        dfDecomposed mustEqual expected
      }
    }

    "st_makeValid" >> {

      "should handle nulls" >> {
        sc.sql("select st_makeValid(null)").collect.head(0) must beNull
        dfBlank.select(st_makeValid(lit(null))).first must beNull
      }

      "should return valid geometry" >> {

        val wkt = "POLYGON((0 0, 0 1, 2 1, 2 2, 1 2, 1 0, 0 0))"
        val geom = s"st_geomFromWKT('$wkt')"
        val decomposed = sc.sql(s"select st_isValid($geom), st_isValid(st_makeValid($geom))").first
        val dfDecomposed = dfBlank.select(st_isValid(st_geomFromWKT(lit(wkt))), st_isValid(st_makeValid(st_geomFromWKT(lit(wkt))))).first

        val expected = (false, true)

        (decomposed.getBoolean(0), decomposed.getBoolean(1)) mustEqual expected
        dfDecomposed mustEqual expected

      }

      "should repair geometry" >> {

        // these polygons are taken from the PostGIS docs: https://postgis.net/docs/ST_MakeValid.html
        val input_wkt = Seq("POLYGON ((0 0, 0 1, 2 1, 2 2, 1 2, 1 0, 0 0))", 
          "MULTIPOLYGON (((186 194, 187 194, 188 195, 189 195, 190 195, 191 195, 192 195, 193 194, 194 194, 194 193, 195 192, 195 191, 195 190, 195 189, 195 188, 194 187, 194 186, 14 6, 13 6, 12 5, 11 5, 10 5, 9 5, 8 5, 7 6, 6 6, 6 7, 5 8, 5 9, 5 10, 5 11, 5 12, 6 13, 6 14, 186 194)), ((150 90, 149 80, 146 71, 142 62, 135 55, 128 48, 119 44, 110 41, 100 40, 90 41, 81 44, 72 48, 65 55, 58 62, 54 71, 51 80, 50 90, 51 100, 54 109, 58 118, 65 125, 72 132, 81 136, 90 139, 100 140, 110 139, 119 136, 128 132, 135 125, 142 118, 146 109, 149 100, 150 90)))", 
          "MULTIPOLYGON (((91 50, 79 22, 51 10, 23 22, 11 50, 23 78, 51 90, 79 78, 91 50)), ((91 100, 79 72, 51 60, 23 72, 11 100, 23 128, 51 140, 79 128, 91 100)), ((91 150, 79 122, 51 110, 23 122, 11 150, 23 178, 51 190, 79 178, 91 150)), ((141 50, 129 22, 101 10, 73 22, 61 50, 73 78, 101 90, 129 78, 141 50)), ((141 100, 129 72, 101 60, 73 72, 61 100, 73 128, 101 140, 129 128, 141 100)), ((141 150, 129 122, 101 110, 73 122, 61 150, 73 178, 101 190, 129 178, 141 150)))", 
          "LINESTRING (0 0, 0 0)")
        val expected_wkt = Seq("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))",
          "MULTIPOLYGON (((149 80, 146 71, 142 62, 135 55, 128 48, 119 44, 110 41, 100 40, 90 41, 81 44, 72 48, 65 55, 64 56, 14 6, 13 6, 12 5, 11 5, 10 5, 9 5, 8 5, 7 6, 6 6, 6 7, 5 8, 5 9, 5 10, 5 11, 5 12, 6 13, 6 14, 56.76923076923077 64.76923076923077, 54 71, 51 80, 50 90, 51 100, 54 109, 58 118, 65 125, 72 132, 81 136, 90 139, 100 140, 110 139, 119 136, 125.23076923076923 133.23076923076923, 186 194, 187 194, 188 195, 189 195, 190 195, 191 195, 192 195, 193 194, 194 194, 194 193, 195 192, 195 191, 195 190, 195 189, 195 188, 194 187, 194 186, 134 126, 135 125, 142 118, 146 109, 149 100, 150 90, 149 80)))",
          "MULTIPOLYGON (((51 10, 23 22, 11 50, 21.714285714285715 75, 11 100, 21.714285714285715 125, 11 150, 23 178, 51 190, 76 179.28571428571428, 101 190, 129 178, 141 150, 130.28571428571428 125, 141 100, 130.28571428571428 75, 141 50, 129 22, 101 10, 76 20.714285714285715, 51 10)))",
          "LINESTRING EMPTY")

        input_wkt.zip(expected_wkt).foreach { case (wkt, expected) =>
          val geom = s"st_geomFromWKT('$wkt')"
          val decomposed = sc.sql(s"select st_asText(st_makeValid($geom))").first.getAs[String](0)
          val dfDecomposed = dfBlank.select(st_asText(st_makeValid(st_geomFromWKT(lit(wkt))))).first

          decomposed mustEqual expected
          dfDecomposed mustEqual expected
        }
        true mustEqual true // happy ending for JUnitRunner
      }
    }

  }
}
