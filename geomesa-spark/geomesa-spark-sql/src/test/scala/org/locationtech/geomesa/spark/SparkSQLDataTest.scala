/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLDataTest extends Specification with LazyLogging {
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  "sql data tests" should {
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
    }

    "ingest chicago" >> {
      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      logger.info(df.schema.treeString)
      df.createOrReplaceTempView("chicago")

      df.collect.length mustEqual 3
    }

    "basic sql 1" >> {
      val r = sc.sql("select * from chicago where st_equals(geom, st_geomFromWKT('POINT(-76.5 38.5)'))")
      val d = r.collect

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }

    "basic sql 4" >> {
      val r = sc.sql("select 1 + 1 > 4")
      val d = r.collect

      d.length mustEqual 1
    }

    "basic sql 5" >> {
      val r = sc.sql("select * from chicago where case_number = 1 and st_intersects(geom, st_makeBox2d(st_point(-77, 38), st_point(-76, 39)))")
      val d = r.collect

      d.length mustEqual 1
    }

    "basic sql 6" >> {
      val r = sc.sql("select st_intersects(st_makeBox2d(st_point(-77, 38), st_point(-76, 39)), st_makeBox2d(st_point(-77, 38), st_point(-76, 39)))")
      val d = r.collect

      d.length mustEqual 1
    }

    "st_translate" >> {
      "null" >> {
        sc.sql("select st_translate(null, null, null)").collect.head(0) must beNull
      }

      "point" >> {
        val r = sc.sql(
          """
          |select st_translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
        """.stripMargin)

        r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 12)")
      }
    }

    "where __fid__ equals" >> {
      val r = sc.sql("select * from chicago where __fid__ = '1'")
      val d = r.collect()

      d.length mustEqual 1
      d.head.getAs[Int]("case_number") mustEqual 2
    }

    "where attr equals" >> {
      val r = sc.sql("select * from chicago where case_number = 2")
      val d = r.collect()

      d.length mustEqual 1
      d.head.getAs[Int]("case_number") mustEqual 2
    }

    "where __fid__ in" >> {
      val r = sc.sql("select * from chicago where __fid__ in ('1', '2')")
      val d = r.collect()

      d.length mustEqual 2
      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(2, 3))
    }

    "where attr in" >> {
      val r = sc.sql("select * from chicago where case_number in (2, 3)")
      val d = r.collect()

      d.length mustEqual 2
      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(2, 3))
    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
