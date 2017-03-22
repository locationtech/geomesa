/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometryProcessingFunctionsTest extends Specification with LazyLogging {

  "sql geometry processing functions" should {
    import scala.collection.JavaConversions._

    sequential
    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")

    val ds = DataStoreFinder.getDataStore(dsParams)
    val spark = SparkSQLTestUtils.createSparkSession()
    val sc = spark.sqlContext

    SparkSQLTestUtils.ingestChicago(ds)

    val df = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    logger.info(df.schema.treeString)
    df.createOrReplaceTempView("chicago")


    "st_antimeridianSafeGeom" >> {
      "should handle nulls" >> {
        sc.sql("select st_antimeridianSafeGeom(null)").collect.head(0) must beNull
      }

      "should split a geom that spans the antimeridian" >> {
        val geom = "st_geomFromWKT('POLYGON((-190 50, -190 60, -170 60, -170 50, -190 50))')";
        val decomposed = sc.sql(s"select st_antimeridianSafeGeom($geom)").collect.head.get(0).asInstanceOf[Geometry]
        decomposed mustEqual WKTUtils.read(
          "MULTIPOLYGON (((-180 50, -180 60, -170 60, -170 50, -180 50)), ((180 60, 180 50, 170 50, 170 60, 180 60)))")
      }
    }

    "st_bufferPoint" >> {
      "should handle nulls" >> {
        sc.sql("select st_bufferPoint(null, null)").collect.head(0) must beNull
      }

      "should return a point buffered in meters" >> {
        val buf = sc.sql("select st_bufferPoint(st_makePoint(0,0), 10)").collect().head.get(0)
        val bufferedPoly = WKTUtils.read(
          """
            |POLYGON ((0.0000899320367762 0, 0.0000897545764446 0.0000056468793115, 0.0000892228958048 0.0000112714729702, 0.0000883390931573 0.0000168515832745, 0.0000871066564674 0.0000223651880784, 0.0000855304495997 0.0000277905277026, 0.0000836166931225 0.0000331061908102, 0.0000813729397584 0.0000382911989076, 0.0000788080445769 0.0000433250891364, 0.0000759321300474 0.0000481879950317, 0.0000727565460907 0.0000528607249257, 0.0000692938252858 0.0000573248376881, 0.0000655576334099 0.0000615627155054, 0.0000615627155054 0.0000655576334099, 0.0000573248376881 0.0000692938252858, 0.0000528607249257 0.0000727565460907, 0.0000481879950317 0.0000759321300474, 0.0000433250891364 0.0000788080445769, 0.0000382911989076 0.0000813729397584, 0.0000331061908102 0.0000836166931225, 0.0000277905277026 0.0000855304495997, 0.0000223651880784 0.0000871066564674, 0.0000168515832745 0.0000883390931573, 0.0000112714729702 0.0000892228958048, 0.0000056468793115 0.0000897545764446, -0 0.0000899320367762, -0.0000056468793115 0.0000897545764446, -0.0000112714729702 0.0000892228958048, -0.0000168515832745 0.0000883390931573, -0.0000223651880784 0.0000871066564674, -0.0000277905277026 0.0000855304495997, -0.0000331061908102 0.0000836166931225, -0.0000382911989076 0.0000813729397584, -0.0000433250891364 0.0000788080445769, -0.0000481879950317 0.0000759321300474, -0.0000528607249257 0.0000727565460907, -0.0000573248376881 0.0000692938252858, -0.0000615627155054 0.0000655576334099, -0.0000655576334099 0.0000615627155054, -0.0000692938252858 0.0000573248376881, -0.0000727565460907 0.0000528607249257, -0.0000759321300474 0.0000481879950317, -0.0000788080445769 0.0000433250891364, -0.0000813729397584 0.0000382911989076, -0.0000836166931225 0.0000331061908102, -0.0000855304495997 0.0000277905277026, -0.0000871066564674 0.0000223651880784, -0.0000883390931573 0.0000168515832745, -0.0000892228958048 0.0000112714729702, -0.0000897545764446 0.0000056468793115, -0.0000899320367762 -0, -0.0000897545764446 -0.0000056468793115, -0.0000892228958048 -0.0000112714729702, -0.0000883390931573 -0.0000168515832745, -0.0000871066564674 -0.0000223651880784, -0.0000855304495997 -0.0000277905277026, -0.0000836166931225 -0.0000331061908102, -0.0000813729397584 -0.0000382911989076, -0.0000788080445769 -0.0000433250891364, -0.0000759321300474 -0.0000481879950317, -0.0000727565460907 -0.0000528607249257, -0.0000692938252858 -0.0000573248376881, -0.0000655576334099 -0.0000615627155054, -0.0000615627155054 -0.0000655576334099, -0.0000573248376881 -0.0000692938252858, -0.0000528607249257 -0.0000727565460907, -0.0000481879950317 -0.0000759321300474, -0.0000433250891364 -0.0000788080445769, -0.0000382911989076 -0.0000813729397584, -0.0000331061908102 -0.0000836166931225, -0.0000277905277026 -0.0000855304495997, -0.0000223651880784 -0.0000871066564674, -0.0000168515832745 -0.0000883390931573, -0.0000112714729702 -0.0000892228958048, -0.0000056468793115 -0.0000897545764446, -0 -0.0000899320367762, 0.0000056468793115 -0.0000897545764446, 0.0000112714729702 -0.0000892228958048, 0.0000168515832745 -0.0000883390931573, 0.0000223651880784 -0.0000871066564674, 0.0000277905277026 -0.0000855304495997, 0.0000331061908102 -0.0000836166931225, 0.0000382911989076 -0.0000813729397584, 0.0000433250891364 -0.0000788080445769, 0.0000481879950317 -0.0000759321300474, 0.0000528607249257 -0.0000727565460907, 0.0000573248376881 -0.0000692938252858, 0.0000615627155054 -0.0000655576334099, 0.0000655576334099 -0.0000615627155054, 0.0000692938252858 -0.0000573248376881, 0.0000727565460907 -0.0000528607249257, 0.0000759321300474 -0.0000481879950317, 0.0000788080445769 -0.0000433250891364, 0.0000813729397584 -0.0000382911989076, 0.0000836166931225 -0.0000331061908102, 0.0000855304495997 -0.0000277905277026, 0.0000871066564674 -0.0000223651880784, 0.0000883390931573 -0.0000168515832745, 0.0000892228958048 -0.0000112714729702, 0.0000897545764446 -0.0000056468793115, 0.0000899320367762 0))
          """.stripMargin)
        buf.asInstanceOf[Polygon].equalsExact(bufferedPoly, 0.000001) must beTrue
      }

      "should handle antimeridian" >> {
        val buf = sc.sql("select st_bufferPoint(st_makePoint(-180, 50), 100000)").collect().head.get(0).asInstanceOf[Geometry]
        // check points on both sides of antimeridian
        buf.contains(WKTUtils.read("POINT(-179.9 50)")) mustEqual true
        buf.contains(WKTUtils.read("POINT(179.9 50)")) mustEqual true
      }
    }

    "st_aggregateDistanceSpheroid" >> {
      "should work with window functions" >> {
        val res = sc.sql(
          """
          |select
          |   case_number,dtg,st_aggregateDistanceSpheroid(l)
          |from (
          |  select
          |      case_number,
          |      dtg,
          |      collect_list(geom) OVER (PARTITION BY true ORDER BY dtg asc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as l
          |  from chicago
          |)
          |where
          |   size(l) > 1
        """.stripMargin).
          collect().map(_.getDouble(2))
        Array(70681.00230533161, 141178.0595870766) must beEqualTo(res)
      }
    }

    "st_lengthSpheroid" >> {
      "should handle null" >> {
        sc.sql("select st_lengthSpheroid(null)").collect.head(0) must beNull
      }

      "should get great circle length of a linestring" >> {
        val res = sc.sql(
          """
          |select
          |  case_number,st_lengthSpheroid(st_makeLine(l))
          |from (
          |   select
          |      case_number,
          |      dtg,
          |      collect_list(geom) OVER (PARTITION BY true ORDER BY dtg asc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as l
          |   from chicago
          |)
          |where
          |   size(l) > 1
        """.stripMargin).
          collect().map(_.getDouble(1))
        Array(70681.00230533161, 141178.0595870766) must beEqualTo(res)
      }
    }

  }

}
