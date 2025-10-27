/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType}
import org.apache.spark.sql.{Column, Row}
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.jts.st_geomFromWKT
import org.locationtech.geomesa.spark.sql.DataFrameFunctions.SpatialRelations
import org.locationtech.geomesa.spark.sql.SparkUtils
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.specs2.execute.Result
import org.specs2.matcher.MatchResult

import java.util.{Collections, UUID, Map => JMap}
import java.{util => ju}
import scala.util.Random

class SparkSQLDataTest extends TestWithSpark {

  import scala.collection.JavaConverters._

  val dsParams: JMap[String, String] =
    Map("namespace" -> getClass.getSimpleName, "cqengine" -> "true", "geotools" -> "true").asJava

  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  override def beforeAll(): Unit = {
    super.beforeAll()
    WithClose(DataStoreFinder.getDataStore(dsParams)) { ds =>
      SparkSQLTestUtils.ingestChicago(ds)
    }
    val df = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    logger.debug(df.schema.treeString)

    df.createOrReplaceTempView("chicago")
  }

  "sql data tests" should {

    "not using sedona" >> {
      haveSedona must beFalse
      isUsingSedona must beFalse
    }

    "ingest chicago" >> {
      val df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      df.collect.length mustEqual 3
    }

    "work well without Apache Sedona" in {
      val resDf = spark.sql(
        """
          |SELECT '7' as __fid__, arrest, case_number, dtg, st_castToPoint(st_translate(geom, 70, -30)) AS geom
          | FROM chicago
          | WHERE st_contains(st_makeBBOX(-79, 35, -77.5, 40), geom) AND CONCAT(__fid__, 'id') = '3id'""".stripMargin)
      val resList = resDf.collect()
      resList.length mustEqual 1L
      resList(0).getAs[Int]("case_number") mustEqual 3
      val resGeom = resList(0).getAs[Point]("geom")
      resGeom.getX mustEqual -8d
      resGeom.getY mustEqual 9d
    }

    "create indexed relation" >> {
      val dfIndexed = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .load()
      logger.debug(dfIndexed.schema.treeString)

      dfIndexed.createOrReplaceTempView("chicagoIndexed")

      dfIndexed.collect.length mustEqual 3
    }

    "create spatially partitioned relation with date query option" >> {
      val dfPartitioned = spark.read
          .format("geomesa")
          .options(dsParams)
          .option("geomesa.feature", "chicago")
          .option("spatial", "true")
          .option("query", "dtg AFTER 2016-01-01T10:00:00.000Z")
          .load()
      logger.debug(dfPartitioned.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedWithQuery")

      spark.sql("select * from chicagoPartitionedWithQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("2", "3")
    }

    "create spatially partitioned relation with attribute query option" >> {
      val dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("spatial", "true")
        .option("query", "case_number < 3")
        .load()
      logger.debug(dfPartitioned.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedAttrQuery")

      spark.sql("select * from chicagoPartitionedAttrQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("1", "2")
    }

    "create spatially partitioned relation with spatial query option" >> {
      val dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("spatial", "true")
        .option("query", "BBOX(geom, -76.7, 38.2, -76.2, 38.7)")
        .load()
      logger.debug(dfPartitioned.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedGeomQuery")

      spark.sql("select * from chicagoPartitionedGeomQuery")
        .collect().map{ r=> r.get(0) } mustEqual Array("1")
    }

    "create spatially partitioned relation" >> {
      val dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .option("spatial","true")
        .option("strategy", "RTREE")
        .load()
      logger.debug(dfPartitioned.schema.treeString)

      // Filter if features belonged to multiple partition envelopes
      // TODO: Better way
      val hashSet = new ju.HashSet[String]()
      dfPartitioned.collect.foreach{ row =>
        hashSet.add(row.getAs[String]("__fid__"))
      }
      hashSet.size() mustEqual 3
    }

    "handle projections on in-memory store" >> {
      val dfIndexed = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .load()
      logger.debug(dfIndexed.schema.treeString)

      dfIndexed.createOrReplaceTempView("chicagoIndexedProj")

      val r = sc.sql("select geom from chicagoIndexedProj where case_number = 1")
      val d = r.collect
      d.length mustEqual 1

      val row = d(0)
      row.schema.fieldNames.length mustEqual 1
      row.fieldIndex("geom") mustEqual 0
    }

    "basic sql indexed" >> {
      val dfIndexed = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .load()
      logger.debug(dfIndexed.schema.treeString)

      dfIndexed.createOrReplaceTempView("chicagoIndexedBasic")

      val r = sc.sql("select * from chicagoIndexedBasic where st_equals(geom, st_geomFromWKT('POINT(-76.5 38.5)'))")
      val d = r.collect

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }

    "basic sql partitioned" >> {
      val dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .option("spatial","true")
        .option("strategy", "RTREE")
        .load()
      logger.debug(dfPartitioned.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitionedBasic")

      val r = sc.sql("select * from chicagoPartitionedBasic where st_equals(geom, st_geomFromWKT('POINT(-77 38)'))")
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

      val df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      val pushdownDF = df.where("st_intersects(st_makeBox2D(st_point(-77, 38), st_point(-76, 39)), geom)")
      val pushdownDFPlan = pushdownDF.queryExecution.optimizedPlan

      val noPushdown = sc.sql("select geom from chicago where __fid__ = 1")
      val noPushdownPlan = noPushdown.queryExecution.optimizedPlan

      pushdownPlan.children.head.isInstanceOf[LogicalRelation] must beTrue // filter is pushed down
      pushdownDFPlan.isInstanceOf[LogicalRelation] must beTrue // filter is pushed down
      noPushdownPlan.children.head.isInstanceOf[Filter] must beTrue // filter remains at top level
    }

    "pushdown attribute filters" >> {
      val pushdown = sc.sql("select geom from chicago where case_number = 1")
      val pushdownPlan = pushdown.queryExecution.optimizedPlan

      val df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
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

      WithClose(DataStoreFinder.getDataStore(dsParams)) { ds =>
        val points = SparkSQLTestUtils.generatePoints(gf, 1000)
        SparkSQLTestUtils.ingestPoints(ds, "points", points)

        val polys = SparkSQLTestUtils.generatePolys(gf, 1000)
        SparkSQLTestUtils.ingestGeometries(ds, "polys", polys)
      }

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
  }

  "GeoMesaSparkSQL" should {

    "map appropriate column types" in {

      val spec =
        """int:Integer,
          |long:Long,
          |float:Float,
          |double:Double,
          |uuid:UUID,
          |string:String,
          |boolean:Boolean,
          |dtg:Date,
          |time:Timestamp,
          |bytes:Bytes,
          |list:List[String],
          |map:Map[String,Integer],
          |line:LineString:srid=4326,
          |poly:Polygon:srid=4326,
          |points:MultiPoint:srid=4326,
          |lines:MultiLineString:srid=4326,
          |polys:MultiPolygon:srid=4326,
          |geoms:GeometryCollection:srid=4326,
          |*point:Point:srid=4326
    """.stripMargin

      val sft = SimpleFeatureTypes.createType("complex", spec)
      val sf = {
        val sf = new ScalaSimpleFeature(sft, "0")
        sf.setAttribute("int", "1")
        sf.setAttribute("long", "-100")
        sf.setAttribute("float", "1.0")
        sf.setAttribute("double", "5.37")
        sf.setAttribute("uuid", UUID.randomUUID())
        sf.setAttribute("string", "mystring")
        sf.setAttribute("boolean", "false")
        sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
        sf.setAttribute("time", "2013-01-02T00:00:00.000Z")
        sf.setAttribute("bytes", Array[Byte](0, 1))
        sf.setAttribute("list", Collections.singletonList("mylist"))
        sf.setAttribute("map", Collections.singletonMap("mykey", 1))
        sf.setAttribute("line", "LINESTRING(0 2, 2 0, 8 6)")
        sf.setAttribute("poly", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
        sf.setAttribute("points", "MULTIPOINT(0 0, 2 2)")
        sf.setAttribute("lines", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
        sf.setAttribute("polys", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))")
        sf.setAttribute("geoms", "GEOMETRYCOLLECTION(POINT(45.0 49.0),POINT(45.1 49.1))")
        sf.setAttribute("point", "POINT(45.0 49.0)")
        sf
      }

      // we turn off the geo-index on the CQEngine DataStore because
      // BucketIndex doesn't do polygon <-> polygon comparisons properly;
      // acceptable performance-wise because the test data set is small
      val dsParams = Map(
        "geotools" -> "true",
        "namespace" -> "SparkSQLColumnsTest",
        "cqengine" -> "true",
        "useGeoIndex" -> "false"
      )

      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds.createSchema(sft)

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, sf, useProvidedFid = true)
        }

        val df = spark.read
          .format("geomesa")
          .options(dsParams)
          .option("geomesa.feature", sft.getTypeName)
          .load()

        logger.debug(df.schema.treeString)
        df.createOrReplaceTempView(sft.getTypeName)

        val expected = Seq(
          "__fid__" -> DataTypes.StringType,
          "int"     -> DataTypes.IntegerType,
          "long"    -> DataTypes.LongType,
          "float"   -> DataTypes.FloatType,
          "double"  -> DataTypes.DoubleType,
          "string"  -> DataTypes.StringType,
          "boolean" -> DataTypes.BooleanType,
          "dtg"     -> DataTypes.TimestampType,
          "time"    -> DataTypes.TimestampType,
          "bytes"   -> DataTypes.BinaryType,
          "list"    -> ArrayType(DataTypes.StringType),
          "map"     -> MapType(DataTypes.StringType, DataTypes.IntegerType),
          "line"    -> JTSTypes.LineStringTypeInstance,
          "poly"    -> JTSTypes.PolygonTypeInstance,
          "points"  -> JTSTypes.MultiPointTypeInstance,
          "lines"   -> JTSTypes.MultiLineStringTypeInstance,
          "polys"   -> JTSTypes.MultipolygonTypeInstance,
          "geoms"   -> JTSTypes.GeometryCollectionTypeInstance,
          "point"   -> JTSTypes.PointTypeInstance
        )

        val sqlDf = sc.sql(s"select * from ${sft.getTypeName}")

        val schema = sqlDf.schema
        schema must haveLength(expected.length) // note: uuid was not supported
        schema.map(_.name) mustEqual expected.map(_._1)
        schema.map(_.dataType) mustEqual expected.map(_._2)

        val result = sqlDf.collect()
        result must haveLength(1)

        val row = result.head

        // note: have to compare backwards so that java.util.Date == java.sql.Timestamp
        sf.getID mustEqual row.get(0)
        Result.foreach(expected.drop(1)) { case (f, _) =>
          val attrType = sft.getDescriptor(f).getType
          if (attrType.getBinding == classOf[java.util.List[_]]) {
            sf.getAttribute(f).asInstanceOf[java.util.List[_]].toArray() mustEqual row.getAs[Seq[_]](f).toArray
          } else if (attrType.getBinding == classOf[java.util.Map[_, _]]) {
            sf.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala mustEqual row.getAs[Map[_, _]](f)
          } else {
            sf.getAttribute(f) mustEqual row.getAs[AnyRef](f)
          }
        }
      }
    }
  }

  "GeoJSONExtensions" should {
    "convert points" in {
      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import spark.implicits._

      val rows = Seq(("1", 1, WKTUtils.read("POINT(1 2)")), ("2", 2, WKTUtils.read("POINT(1 3)")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      val result = df.toGeoJSON.collect()
      result must haveLength(2)
      result.head mustEqual
        """{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"name":1}}"""
      result.last mustEqual
        """{"type":"Feature","id":"2","geometry":{"type":"Point","coordinates":[1,3]},"properties":{"name":2}}"""
    }

    "convert polygons" in {
      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import spark.implicits._

      val rows = Seq(("1", 1, WKTUtils.read("POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.head() mustEqual
        """{"type":"Feature","id":"1","geometry":{"type":"Polygon","coordinates":""" +
          """[[[1,1],[1,2],[2,2],[2,1],[1,1]]]},"properties":{"name":1}}"""
    }

    "handle multiple rows" in {
      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import spark.implicits._

      val rows = (1 to 10).map { i => (s"$i", i, WKTUtils.read(s"POINT($i $i)")) }

      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.collect mustEqual Array.range(1, 11).map { i =>
        s"""{"type":"Feature","id":"$i","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i}}"""
      }
    }

    "handle rows with nulls" in {
      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import spark.implicits._

      val rows = Seq(
        ("1", Int.box(1), WKTUtils.read("POINT(1 1)")),
        ("2", null, null),
        ("3", Int.box(3), WKTUtils.read("POINT(3 3)"))
      )

      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.collect mustEqual Array(1, 2, 3).map {
        case 2 => """{"type":"Feature","id":"2","geometry":null,"properties":{"name":null}}"""
        case i => s"""{"type":"Feature","id":"$i","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i}}"""
      }
    }
  }

  "DataFrame functions" should {
    "st_transform" in {
      val pointWGS84 = "POINT(-0.871722 52.023636)"
      val expectedOSGB36 = WKTUtils.read("POINT(477514.0081191745 236736.03179981868)")
      val fn = new SpatialRelations(){}
      val transf = dfBlank().select(fn.st_transform(st_geomFromWKT(pointWGS84), lit("EPSG:4326"), lit("EPSG:27700"))).first
      transf must not(throwAn[Exception])
      transf mustEqual expectedOSGB36
    }
  }

  "sql geometric distance functions" should {

    "st_aggregateDistanceSpheroid should work with window functions" in {
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
      Array(70681.00230533126, 141178.0595870745) must beEqualTo(res)
    }

    "st_lengthSpheroid should handle null" in {
      sc.sql("select st_lengthSpheroid(null)").collect.head(0) must beNull
    }

    "st_lengthSpheroid should get great circle length of a linestring" in {
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
      Array(70681.00230533126, 141178.0595870745) must beEqualTo(res)
    }

    "st_transform should handle null" in {
      sc.sql("select st_transform(null, null, null)").collect.head(0) must beNull
    }

    "st_transform should transform the coordinates of a point" in {
      val pointWGS84 = "POINT(-0.871722 52.023636)"
      val expectedOSGB36 = "POINT(477514.0081191745 236736.03179981868)"
      val r = sc.sql(
        s"select st_transform(st_geomFromWKT('$pointWGS84'), 'EPSG:4326', 'EPSG:27700')"
      ).collect()
      r.head.getAs[Point](0) mustEqual WKTUtils.read(expectedOSGB36)
    }
  }

  "SparkUtils" should {

    lazy val simpleTypeValueMap: Map[String, AnyRef] = Map(
      "Integer" -> Int.box(10),
      "Long" -> Long.box(42),
      "Float" -> Float.box(3.14f),
      "Double" -> Double.box(42.10),
      "String" -> "test_string",
      "Boolean" -> Boolean.box(true),
      "Date" -> new java.util.Date(System.currentTimeMillis()),
      "Bytes" -> Array.fill(10)((Random.nextInt(256) - 128).toByte)
    )

    lazy val geomTypeValueMap: Map[String, AnyRef] = Map(
      "Point" -> FastConverter.convert(
        "POINT(45.0 49.0)",
        classOf[org.locationtech.jts.geom.Point]),
      "LineString" -> FastConverter.convert(
        "LINESTRING(0 2, 2 0, 8 6)", classOf[org.locationtech.jts.geom.LineString]),
      "Polygon" -> FastConverter.convert(
        "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))", classOf[org.locationtech.jts.geom.Polygon]),
      "MultiPoint" -> FastConverter.convert(
        "MULTIPOINT(0 0, 2 2)", classOf[org.locationtech.jts.geom.MultiPoint]),
      "MultiLineString" -> FastConverter.convert(
        "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))", classOf[org.locationtech.jts.geom.MultiLineString]),
      "MultiPolygon" -> FastConverter.convert(
        "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))",
        classOf[org.locationtech.jts.geom.MultiPolygon]),
      "GeometryCollection" -> FastConverter.convert(
        "GEOMETRYCOLLECTION(POINT(45.0 49.0),POINT(45.1 49.1))",
        classOf[org.locationtech.jts.geom.GeometryCollection]),
      "Geometry" -> FastConverter.convert(
        "POINT(45.0 49.0)",
        classOf[org.locationtech.jts.geom.Point])
    )

    lazy val unsupportedTypeValueMap: Map[String, AnyRef] = Map(
      "UUID" -> UUID.randomUUID()
    )

    def validateDataTypeConversions(sf: SimpleFeature): MatchResult[_] = {
      val sft = sf.getFeatureType
      val featureNames = sft.getAttributeDescriptors.asScala.map(d => d.getLocalName)
      val schema = SparkUtils.createStructType(sft)
      val extractors = SparkUtils.getExtractors(schema.fieldNames, schema)
      val row = SparkUtils.sf2row(schema, sf, extractors)
      validateRow(sf, row)

      // create dataframe to validate correctness of schema
      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
      val row2 = df.collect()(0)
      validateRow(sf, row2)

      // simple feature should preserve its value when converted back from Row
      val sf2 = SparkUtils.rowsToFeatures(sft, schema).apply(row2)
      featureNames.foreach { f =>
        val attrType = sft.getDescriptor(f).getType
        if (attrType.getBinding == classOf[java.util.List[_]]) {
          sf.getAttribute(f).asInstanceOf[java.util.List[_]].toArray mustEqual
            sf2.getAttribute(f).asInstanceOf[java.util.List[_]].toArray
        } else if (attrType.getBinding == classOf[java.util.Map[_, _]]) {
          val sfMap = sf.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
          val sf2Map = sf2.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
          sfMap.size mustEqual sf2Map.size
          sfMap.keys.toArray mustEqual sf2Map.keys.toArray
          sfMap.values.toArray mustEqual sf2Map.values.toArray
        } else {
          sf.getAttribute(f) mustEqual sf2.getAttribute(f)
        }
      }

      // simple feature type should be generated from struct type correctly
      val sft2 = SparkUtils.createFeatureType("created_from_struct_type", schema)
      foreach(featureNames) { f =>
        val binding = sft.getDescriptor(f).getType.getBinding
        // We cannot check user data for basic or geometry types, since we cannot tell if it is a
        // default field, or what srid the geom field is. Here we just want to make sure that the
        // element type of list field or key/value types of map field is correctly populated.
        if (binding == classOf[java.util.List[_]] || binding == classOf[java.util.Map[_, _]]) {
          sft.getDescriptor(f).getUserData mustEqual sft2.getDescriptor(f).getUserData
        }
        binding mustEqual sft2.getDescriptor(f).getType.getBinding
      }
    }

    def validateRow(sf: SimpleFeature, row: Row): Unit = {
      sf.getID mustEqual(row.getAs[String]("__fid__"))
      val sft = sf.getFeatureType
      val featureNames = sft.getAttributeDescriptors.asScala.map(d => d.getLocalName)
      featureNames.foreach { f =>
        val attrType = sft.getDescriptor(f).getType
        if (attrType.getBinding == classOf[java.util.List[_]]) {
          sf.getAttribute(f).asInstanceOf[java.util.List[_]].toArray() mustEqual row.getAs[Seq[_]](f).toArray
        } else if (attrType.getBinding == classOf[java.util.Map[_, _]]) {
          val rowValue = row.getAs[Map[_, _]](f)
          val featureValue = sf.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
          featureValue.size mustEqual rowValue.size
          featureValue.keys.toArray mustEqual rowValue.keys.toArray
          featureValue.values.toArray mustEqual rowValue.values.toArray
        } else {
          sf.getAttribute(f) mustEqual row.getAs[AnyRef](f)
        }
      }
    }

    "convert simple type to SparkSQL data type correctly" in {
      val spec = simpleTypeValueMap.keys.map(t => s"f_$t:$t").mkString(",")
      val sft = SimpleFeatureTypes.createType("simple", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      simpleTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      validateDataTypeConversions(sf)
    }

    "convert geom type to SparkSQL UDT correctly" in {
      val spec = geomTypeValueMap.keys.map(t => s"f_$t:$t:srid=4326").mkString(",")
      val sft = SimpleFeatureTypes.createType("geom", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      geomTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      validateDataTypeConversions(sf)
    }

    "convert list type to SparkSQL Array type correctly" in {
      val spec = simpleTypeValueMap.keys.map(t => s"f_$t:List[$t]").mkString(",")
      val sft = SimpleFeatureTypes.createType("complex_list", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      simpleTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", List(value)) }
      validateDataTypeConversions(sf)
    }

    "convert map type to SparkSQL Map type correctly" in {
      // using byte[] as key of HashMap will cause weird problems
      val keyTypes = simpleTypeValueMap.keys.filter(_ != "Bytes")
      val valueTypes = simpleTypeValueMap.keys

      // make a cross join of keyTypes and valueTypes to generate all possible combinations
      val mapKeyValueTypes = keyTypes.flatMap(keyType => valueTypes.map(valueType => (keyType, valueType)))

      val spec = mapKeyValueTypes.map {
        case (keyType, valueType) => s"f_${keyType}_${valueType}:Map[$keyType, $valueType]"
      }.mkString(",")
      val sft = SimpleFeatureTypes.createType("complex_map", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      mapKeyValueTypes.foreach {
        case (keyType, valueType) =>
          val key = simpleTypeValueMap(keyType)
          val value = simpleTypeValueMap(valueType)
          sf.setAttribute(s"f_${keyType}_${valueType}", Map(key -> value))
      }
      validateDataTypeConversions(sf)
    }

    "ignore unsupported types" in {
      val spec = unsupportedTypeValueMap.keys.map(t => s"f_$t:$t").mkString(",")
      val sft = SimpleFeatureTypes.createType("unsupported", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      unsupportedTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      val schema = SparkUtils.createStructType(sft)
      val extractors = SparkUtils.getExtractors(schema.fieldNames, schema)
      val row = SparkUtils.sf2row(schema, sf, extractors)
      row.length mustEqual 1
      row.getAs[String](0) mustEqual sf.getID
    }

    "ignore complex types containing unsupported types" in {
      val spec = unsupportedTypeValueMap.keys.flatMap(
        t => Seq(s"list_$t:List[$t]", s"mapKey_$t:Map[$t, String]", s"mapValue_$t:Map[String, $t]")).mkString(",")
      val sft = SimpleFeatureTypes.createType("unsupported_complex", spec)
      val schema = SparkUtils.createStructType(sft)
      schema.fieldNames mustEqual Array("__fid__")
    }
  }
}
