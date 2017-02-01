/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometryAccessorsTest extends org.specs2.mutable.Spec with LazyLogging {

  "sql geometry accessors" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")
    var ds: DataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null
    var df: DataFrame = null

    // before
    step {
      ds = DataStoreFinder.getDataStore(dsParams)
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext

      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      logger.info(df.schema.treeString)
      df.createOrReplaceTempView("chicago")

      df.collect().length mustEqual 3
    }

    "st_boundary" >> {
      val result = sc.sql(
        """
          |select st_boundary(st_geomFromWKT('LINESTRING(1 1, 0 0, -1 1)'))
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("MULTIPOINT(1 1, -1 1)")
    }

    "st_coordDim" >> {
      val result = sc.sql(
        """
          |select st_coordDim(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2
    }

    "st_dimension" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 0
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('LINESTRING(1 1, 0 0, -1 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "polygon" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 2
      }

      "geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }
    }

    "st_envelope" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0,0 3,1 3,1 0,0 0))")
      }

      "polygon" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))")
      }
    }

    "st_exteriorRing" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_exteriorRing(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
      }

      "polygon without an interior ring" >> {
        val result = sc.sql(
          """
            |select st_exteriorRing(st_geomFromWKT('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(30 10, 40 40, 20 40, 10 20, 30 10)")
      }

      "polygon with an interior ring" >> {
        val result = sc.sql(
          """
            |select st_exteriorRing(st_geomFromWKT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
            |(20 30, 35 35, 30 20, 20 30))'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(35 10, 45 45, 15 40, 10 20, 35 10)")
      }
    }

    "st_geometryN" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('POINT(0 0)'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "multilinestring" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(10 10, 20 20, 10 40)")
      }

      "geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(1 1,0 0)")
      }
    }

    "st_geometryType" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_geometryType(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "Point"
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_geometryType(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "LineString"
      }

      "geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_geometryType(st_geomFromWKT('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "GeometryCollection"
      }

    }

    "st_interiorRingN" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_interiorRingN(st_geomFromWKT('POINT(0 0)'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
      }

      "polygon with a valid int" >> {
        val result = sc.sql(
          """
            |select st_interiorRingN(st_geomFromWKT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
            |(20 30, 35 35, 30 20, 20 30))'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(20 30, 35 35, 30 20, 20 30)")
      }

      "polygon with an invalid int" >> {
        val result = sc.sql(
          """
            |select st_interiorRingN(st_geomFromWKT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
            |(20 30, 35 35, 30 20, 20 30))'), 5)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
      }
    }

    "st_isClosed" >> {
      "open linestring" >> {
        val result = sc.sql(
          """
            |select st_isClosed(st_geomFromWKT('LINESTRING(0 0, 1 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }

      "closed linestring" >> {
        val result = sc.sql(
          """
            |select st_isClosed(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "open multilinestring" >> {
        val result = sc.sql(
          """
            |select st_isClosed(st_geomFromWKT('MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }

      "closed multilinestring" >> {
        val result = sc.sql(
          """
            |select st_isClosed(st_geomFromWKT('MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1, 0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }
    }

    "st_isCollection" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_isCollection(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }

      "multipoint" >> {
        val result = sc.sql(
          """
            |select st_isCollection(st_geomFromWKT('MULTIPOINT((0 0), (42 42))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_isCollection(st_geomFromWKT('GEOMETRYCOLLECTION(POINT(0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }
    }

    "st_isEmpty" >> {
      "empty geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_isEmpty(st_geomFromWKT('GEOMETRYCOLLECTION EMPTY'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "non-empty point" >> {
        val result = sc.sql(
          """
            |select st_isEmpty(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }
    }

    "st_isRing" >> {
      "closed and simple linestring" >> {
        val result = sc.sql(
          """
            |select st_isRing(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "closed and non-simple linestring" >> {
        val result = sc.sql(
          """
            |select st_isRing(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }
    }

    "st_isSimple" >> {
      "simple point" >> {
        val result = sc.sql(
          """
            |select st_isSimple(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "simple linestring" >> {
        val result = sc.sql(
          """
            |select st_isSimple(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "non-simple linestring" >> {
        val result = sc.sql(
          """
            |select st_isSimple(st_geomFromWKT('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }

      "non-simple polygon" >> {
        val result = sc.sql(
          """
            |select st_isSimple(st_geomFromWKT('POLYGON((1 2, 3 4, 5 6, 1 2))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }
    }

    "st_isValid" >> {
      "valid linestring" >> {
        val result = sc.sql(
          """
            |select st_isValid(st_geomFromWKT('LINESTRING(0 0, 1 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
      }

      "invalid polygon" >> {
        val result = sc.sql(
          """
            |select st_isValid(st_geomFromWKT('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
      }
    }

    "st_numGeometries" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_numGeometries(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_numGeometries(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_numGeometries(st_geomFromWKT('GEOMETRYCOLLECTION(MULTIPOINT(-2 3,-2 2),
            |LINESTRING(5 5,10 10),
            |POLYGON((-7 4.2,-7.1 5,-7.1 4.3,-7 4.2)))'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 3
      }
    }

    "st_numPoints" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_numPoints(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "multipoint" >> {
        val result = sc.sql(
          """
            |select st_numPoints(st_geomFromWKT('MULTIPOINT(-2 3,-2 2)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 2
      }

      "multipoint" >> {
        val result = sc.sql(
          """
            |select st_numPoints(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 5
      }
    }

    "st_pointN" >> {
      "first point" >> {
        val result = sc.sql(
          """
            |select st_pointN(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "last point" >> {
        val result = sc.sql(
          """
            |select st_pointN(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)'), 5)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 2)")
      }

      "first point using a negative index" >> {
        val result = sc.sql(
          """
            |select st_pointN(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)'), -5)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "last point using a negative index" >> {
        val result = sc.sql(
          """
            |select st_pointN(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)'), -1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 2)")
      }
    }

    "st_x" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_x(st_geomFromWKT('POINT(0 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) mustEqual 0f
      }

      "non-point" >> {
        val result = sc.sql(
          """
            |select st_x(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) must beNull
      }
    }

    "st_y" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_y(st_geomFromWKT('POINT(0 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) mustEqual 1f
      }

      "non-point" >> {
        val result = sc.sql(
          """
            |select st_y(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) must beNull
      }
    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
