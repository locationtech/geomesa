/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.spark.sql.SQLTypes
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTS
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricConstructorsTest extends Specification with LazyLogging {

  "sql geometry constructors" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")

    val ds = DataStoreFinder.getDataStore(dsParams)
    val spark = SparkSQLTestUtils.createSparkSession()
    val sc = spark.sqlContext
    SQLTypes.init(sc)

    "st_box2DFromGeoHash" >> {
      sc.sql("select st_box2DFromGeoHash(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        s"""
           |select st_box2DFromGeoHash('ezs42', 25)
          """.stripMargin
      )

      val boxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = boxCoords(0)
      val ur = boxCoords(2)
      boxCoords.length mustEqual 5
      ll.x must beCloseTo(-5.625, .022) // lon
      ll.y must beCloseTo(42.583, .022) // lat
      ur.x must beCloseTo(-5.581, .022) // lon
      ur.y must beCloseTo(42.627, .022) // lat
    }

    "st_geomFromGeoHash" >> {
      sc.sql("select st_geomFromGeoHash(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        s"""
           |select st_geomFromGeoHash('ezs42', 25)
          """.stripMargin
      )

      val geomboxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = geomboxCoords(0)
      val ur = geomboxCoords(2)
      geomboxCoords.length mustEqual 5
      ll.x must beCloseTo(-5.625, .022) // lon
      ll.y must beCloseTo(42.583, .022) // lat
      ur.x must beCloseTo(-5.581, .022) // lon
      ur.y must beCloseTo(42.627, .022) // lat
    }

    "st_makeBBOX" >> {
      sc.sql("select st_makeBBOX(null, null, null, null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_makeBBOX(0.0, 0.0, 2.0, 2.0)
        """.stripMargin
      )
      r.collect().head.getAs[Geometry](0) mustEqual JTS.toGeometry(BoundingBox(0, 2, 0, 2))
    }

    "st_makeBox2D" >> {
      sc.sql("select st_makeBox2D(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }


    "st_pointFromGeoHash" >> {
      sc.sql("select st_pointFromGeoHash(null, null)").collect.head(0) must beNull
      val r = sc.sql(
        s"""
           |select st_pointFromGeoHash('ezs42', 25)
        """.stripMargin
      )

      val point = r.collect().head.getAs[Point](0)
      point.getX must beCloseTo(-5.603, .022)
      point.getY must beCloseTo(42.605, .022)
    }


    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
