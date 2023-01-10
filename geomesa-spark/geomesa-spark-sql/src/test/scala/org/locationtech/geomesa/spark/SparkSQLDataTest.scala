/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
=======
<<<<<<< HEAD
import org.locationtech.geomesa.spark.isUsingSedona
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb0bd279638 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import org.geotools.api.data.{DataStore, DataStoreFinder}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.sql.SQLTypes
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.{Map => JMap}
import java.{util => ju}

@RunWith(classOf[JUnitRunner])
class SparkSQLDataTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true").asJava
  var ds: DataStore = _
  var spark: SparkSession = _
  var sc: SQLContext = _

  var df: DataFrame = _
  var dfIndexed: DataFrame = _
  var dfPartitioned: DataFrame = _

  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  // before
  step {
    ds = DataStoreFinder.getDataStore(dsParams)
    spark = SparkSQLTestUtils.createSparkSession()
    sc = spark.sqlContext
    SQLTypes.init(sc)
  }

  "sql data tests" should {

    "not using sedona" >> {
      isUsingSedona must beFalse
    }

    "ingest chicago" >> {
      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      logger.debug(df.schema.treeString)

      df.createOrReplaceTempView("chicago")

      df.collect.length mustEqual 3
    }

    "create indexed relation" >> {
      dfIndexed = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .load()
      logger.debug(df.schema.treeString)

      dfIndexed.createOrReplaceTempView("chicagoIndexed")

      dfIndexed.collect.length mustEqual 3
    }
    
    
    "create spatially partitioned relation with date query option" >> {
      dfPartitioned = spark.read
          .format("geomesa")
          .options(dsParams)
          .option("geomesa.feature", "chicago")
          .option("spatial", "true")
          .option("query", "dtg AFTER 2016-01-01T10:00:00.000Z")
          .load()
      logger.debug(df.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedWithQuery")

      spark.sql("select * from chicagoPartitionedWithQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("2", "3")
    }

    "create spatially partitioned relation with attribute query option" >> {
      dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("spatial", "true")
        .option("query", "case_number < 3")
        .load()
      logger.debug(df.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedWithQuery")

      spark.sql("select * from chicagoPartitionedWithQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("1", "2")
    }

    "create spatially partitioned relation with spatial query option" >> {
      dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("spatial", "true")
        .option("query", "BBOX(geom, -76.7, 38.2, -76.2, 38.7)")
        .load()
      logger.debug(df.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedWithQuery")

      spark.sql("select * from chicagoPartitionedWithQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("1")
    }

    "create spatially partitioned relation" >> {
      dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .option("spatial","true")
        .option("strategy", "RTREE")
        .load()
      logger.debug(df.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitioned")

      // Filter if features belonged to multiple partition envelopes
      // TODO: Better way
      val hashSet = new ju.HashSet[String]()
      dfPartitioned.collect.foreach{ row =>
        hashSet.add(row.getAs[String]("__fid__"))
      }
      hashSet.size() mustEqual 3
    }

    "handle projections on in-memory store" >> {
      val r = sc.sql("select geom from chicagoIndexed where case_number = 1")
      val d = r.collect
      d.length mustEqual 1

      val row = d(0)
      row.schema.fieldNames.length mustEqual 1
      row.fieldIndex("geom") mustEqual 0
    }

    "basic sql indexed" >> {
      val r = sc.sql("select * from chicagoIndexed where st_equals(geom, st_geomFromWKT('POINT(-76.5 38.5)'))")
      val d = r.collect

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }

    "basic sql partitioned" >> {
      val r = sc.sql("select * from chicagoPartitioned where st_equals(geom, st_geomFromWKT('POINT(-77 38)'))")
      val d = r.collect

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-77, 38))
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

    "pushdown spatial predicates" >> {
      val pushdown = sc.sql("select geom from chicago where st_intersects(st_makeBox2d(st_point(-77, 38), st_point(-76, 39)), geom)")
      val pushdownPlan = pushdown.queryExecution.optimizedPlan

      val pushdownDF = df.where("st_intersects(st_makeBox2D(st_point(-77, 38), st_point(-76, 39)), geom)")
      val pushdownDFPlan = pushdownDF.queryExecution.optimizedPlan

      val noPushdown = sc.sql("select geom from chicago where __fid__ = 1")
      val noPushdownPlan = noPushdown.queryExecution.optimizedPlan

      pushdownPlan.children.head.isInstanceOf[LogicalRelation] mustEqual true // filter is pushed down
      pushdownDFPlan.isInstanceOf[LogicalRelation] mustEqual true // filter is pushed down
      noPushdownPlan.children.head.isInstanceOf[Filter] mustEqual true // filter remains at top level

    }

    "pushdown attribute filters" >> {
      val pushdown = sc.sql("select geom from chicago where case_number = 1")
      val pushdownPlan = pushdown.queryExecution.optimizedPlan

      val pushdownDF = df.where("case_number = 1")
      val pushdownDFPlan = pushdownDF.queryExecution.optimizedPlan

      val noPushdown = sc.sql("select geom from chicago where __fid__ = 1")
      val noPushdownPlan = noPushdown.queryExecution.optimizedPlan

      pushdownPlan.children.head must beAnInstanceOf[LogicalRelation] // filter is pushed down
      pushdownDFPlan must beAnInstanceOf[LogicalRelation] // filter is pushed down
      noPushdownPlan.children.head must beAnInstanceOf[Filter] // filter remains at top level
    }

    "pushdown attribute comparison filters" >> {
      val pushdownLt = sc.sql("select case_number from chicago where case_number < 2")
      val pushdownLte = sc.sql("select case_number from chicago where case_number <= 2")
      val pushdownGt = sc.sql("select case_number from chicago where case_number > 2")
      val pushdownGte = sc.sql("select case_number from chicago where case_number >= 2")

      // ensure all 4 were pushed down
      val queries = Seq(pushdownLt, pushdownLte, pushdownGt, pushdownGte)
      val plans = queries.map{ q => q.queryExecution.optimizedPlan.children.head.getClass }.toArray
      plans mustEqual Array.fill(4)(classOf[LogicalRelation])

      // ensure correct results
      pushdownLt.first().get(0) mustEqual 1
      pushdownLte.collect().map{ r=> r.get(0) } mustEqual Array(1, 2)
      pushdownGt.first().get(0) mustEqual 3
      pushdownGte.collect().map{ r=> r.get(0) } mustEqual Array(2, 3)
    }

    "pushdown date attribute comparison filters" >> {
      val and = "select case_number from chicago where dtg > cast('2016-01-01T01:00:00Z' as timestamp) " +
          "and dtg < cast('2016-01-02T01:00:00Z' as timestamp)"
      val between = "select case_number from chicago where dtg between cast('2016-01-01T01:00:00Z' as timestamp) " +
          "and cast('2016-01-02T01:00:00Z' as timestamp)"

      foreach(Seq(and, between)) { select =>
        val df = sc.sql(select)
        df.queryExecution.optimizedPlan.children.head must beAnInstanceOf[LogicalRelation]
        df.collect().map(_.get(0)) mustEqual Array(2)
      }
    }

    "pushdown date attribute string filters" >> {
      val and = "select case_number from chicago where dtg > '2016-01-01T01:00:00Z' and dtg < '2016-01-02T01:00:00Z'"
      val between = "select case_number from chicago where dtg between '2016-01-01T01:00:00Z' and '2016-01-02T01:00:00Z'"

      foreach(Seq(and, between)) { select =>
        val df = sc.sql(select)
        df.queryExecution.optimizedPlan.children.head must beAnInstanceOf[LogicalRelation]
        df.collect().map(_.get(0)) mustEqual Array(2)
      }
    }

    "pushdown spatio-temporal filters" >> {
      val sql = "select case_number from chicago where " +
          "st_intersects(geom, st_makeBox2d(st_point(-77.5, 37.9), st_point(-76.5, 38.1))) and " +
          "dtg between cast('2016-01-01T01:00:00Z' as timestamp) and cast('2016-01-03T01:00:00Z' as timestamp)"

      val df = sc.sql(sql)
      df.queryExecution.optimizedPlan.children must haveLength(1)
      df.queryExecution.optimizedPlan.children.head must beAnInstanceOf[LogicalRelation]
      df.collect().map(_.get(0)) mustEqual Array(2)
    }

    "preserve feature ID through dataframe ops" >> {
      val sql = "select * from chicago where __fid__ = '1'"
      sc.sql(sql).collect().map(_.getAs[String]("__fid__")) mustEqual Array("1")
      sc.sql(sql).withColumn("label", new Column(Literal(1))).collect().map(_.getAs[String]("__fid__")) mustEqual Array("1")
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
      d.head.getAs[Int]("case_number") mustEqual 1
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
      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(1, 2))
    }

    "where attr in" >> {
      val r = sc.sql("select * from chicago where case_number in (2, 3)")
      val d = r.collect()

      d.length mustEqual 2
      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(2, 3))
    }

    "sweepline join" >> {

      val gf = new GeometryFactory

      val points = SparkSQLTestUtils.generatePoints(gf, 1000)
      SparkSQLTestUtils.ingestPoints(ds, "points", points)

      val polys = SparkSQLTestUtils.generatePolys(gf, 1000)
      SparkSQLTestUtils.ingestGeometries(ds, "polys", polys)

      val polysDf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "polys")
        .load()

      val pointsDf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "points")
        .load()

      val partitionedPolys = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "polys")
        .option("spatial","true")
        .option("strategy", "EARTH")
        .option("partitions","10")
        .load()

      val partitionedPoints = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "points")
        .option("spatial","true")
        .option("strategy", "EARTH")
        .option("partitions","10")
        .load()

      partitionedPolys.createOrReplaceTempView("polysSpatial")
      partitionedPoints.createOrReplaceTempView("pointsSpatial")
      pointsDf.createOrReplaceTempView("points")
      polysDf.createOrReplaceTempView("polys")

      var now = System.currentTimeMillis()
      val r1 = spark.sql("select * from polys join points on st_intersects(points.geom, polys.geom)")
      val count = r1.count()
      logger.debug(s"Regular join took ${System.currentTimeMillis() - now}ms")
      now = System.currentTimeMillis()
      val r2 = spark.sql("select * from polysSpatial join pointsSpatial on st_intersects(pointsSpatial.geom, polysSpatial.geom)")
      val sweeplineCount = r2.count()
      logger.debug(s"Sweepline join took ${System.currentTimeMillis() - now}ms")
      sweeplineCount mustEqual count
    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
