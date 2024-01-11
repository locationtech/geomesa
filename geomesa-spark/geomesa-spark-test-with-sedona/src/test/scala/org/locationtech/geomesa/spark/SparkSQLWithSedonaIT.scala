/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{broadcast, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.geomesa.spark.jts.{geomLit, st_equals, st_intersects}
import org.locationtech.geomesa.spark.sql.SQLTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SparkSQLWithSedonaIT extends Specification with LazyLogging {
  // This test should run with geomesa.use.sedona = true, please refer to pom.xml of this project for details

  import scala.collection.JavaConverters._

  sequential

  val gf = new GeometryFactory
  val dsParams: Map[String, String] = Map("cqengine" -> "true", "geotools" -> "true")
  var ds: DataStore = _
  var spark: SparkSession = _
  var polysDf: DataFrame = _
  var pointsDf: DataFrame = _

  val random = new Random()
  random.setSeed(0)

  private def generatePoints(gf: GeometryFactory, numPoints: Int) = {
    val sft = SimpleFeatureTypes.createType("points", "name:String,*geom:Point:srid=4326")
    ds.createSchema(sft)
    val features = (1 to numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      val geom = gf.createPoint(new Coordinate(x, y))
      new ScalaSimpleFeature(sft, i.toString, Array(i.toString, geom)).asInstanceOf[SimpleFeature]
    }.toList
    val fs = ds.getFeatureSource("points").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features.asJava))
  }

  private def generatePolys(gf: GeometryFactory, numPoints: Int) = {
    val sft = SimpleFeatureTypes.createType("polys", "name:String,*geom:Polygon:srid=4326")
    ds.createSchema(sft)
    val features = (1 to numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      val width = (3 * random.nextDouble()) / 2.0
      val height = (1 * random.nextDouble()) / 2.0
      val (minX, maxX, minY, maxY) = (x - width, x + width, y - height, y + height)
      val coords = Array(new Coordinate(minX, minY), new Coordinate(minX, maxY),
        new Coordinate(maxX, minY), new Coordinate(maxX, maxY), new Coordinate(minX, minY))
      val geom = gf.createPolygon(coords)
      new ScalaSimpleFeature(sft, i.toString, Array(i.toString, geom)).asInstanceOf[SimpleFeature]
    }.toList
    val fs = ds.getFeatureSource("polys").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features.asJava))
  }

  step {
    ds = DataStoreFinder.getDataStore(dsParams.asJava)
    spark = SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", value = false)
      .master("local[*]")
      .getOrCreate()
    SQLTypes.init(spark.sqlContext)

    // ingest some randomly generated points and polygons
    val sft = SimpleFeatureTypes.createType("buffered_points", "name:String,*geom:Geometry:srid=4326")
    ds.createSchema(sft)
    generatePoints(gf, 1000)
    generatePolys(gf, 1000)
    polysDf = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "polys")
      .load()
    polysDf.createOrReplaceTempView("polys")
    pointsDf = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "points")
      .load()
    pointsDf.createOrReplaceTempView("points")
  }

  "be enabled" >> {
    haveSedona must beTrue
    isUsingSedona must beTrue
  }

  "geometry type interoperability" >> {
    val firstPointDf = pointsDf.filter(expr("name = '1'"))
    val firstPoint = firstPointDf.collect().head.getAs[Point]("geom")
    // geomesa_jts_func(sedona_func(geom))
    var row = firstPointDf.select(expr("st_envelope(sedona_ST_Buffer(geom, 1.0)) AS geom")).collect().head
    val envelope = firstPoint.buffer(1).getEnvelope
    row.getAs[Geometry]("geom") mustEqual envelope
    // sedona_func(geomesa_jts_func(geom))
    row = firstPointDf.select(expr("sedona_ST_Centroid(st_castToGeometry(geom)) AS geom")).collect().head
    row.getAs[Geometry]("geom") mustEqual firstPoint
    // use literal DataFrame functions. Be cautious that we should use st_equals rather than ===, since geometry
    // objects serialized by Apache Sedona might be different than GeoMesa Spark JTS.
    val resDf = firstPointDf.filter(st_equals(
      expr("sedona_ST_Envelope(sedona_ST_Buffer(geom, 1.0))"), geomLit(envelope)))
    resDf.count() mustEqual 1L
  }

  "fold constant expressions" >> {
    val df = spark.sql("SELECT sedona_ST_Intersects(st_point(10, 20), sedona_ST_PolygonFromEnvelope(-30.0, 30.0, -10.0, 50.0))")
    df.queryExecution.optimizedPlan must beAnInstanceOf[Project]
    val project = df.queryExecution.optimizedPlan.asInstanceOf[Project]
    project.projectList.head must beAnInstanceOf[Alias]
    project.projectList.head.asInstanceOf[Alias].children.head must beAnInstanceOf[Literal]
    project.projectList.head.asInstanceOf[Alias].children.head.asInstanceOf[Literal].value mustEqual false
  }

  "fold constant expressions to geometry literal" >> {
    val df = spark.sql("SELECT sedona_ST_Boundary(st_translate(sedona_ST_PolygonFromEnvelope(0.0, 0.0, 1.0, 1.0), 1.0, 1.0))")
    df.queryExecution.optimizedPlan must beAnInstanceOf[Project]
    val project = df.queryExecution.optimizedPlan.asInstanceOf[Project]
    project.projectList.head must beAnInstanceOf[Alias]
    project.projectList.head.asInstanceOf[Alias].children.head must beAnInstanceOf[GeometryLiteral]
    val geomLiteral = project.projectList.head.asInstanceOf[Alias].children.head.asInstanceOf[GeometryLiteral]
    geomLiteral.geom mustEqual WKTUtils.read("LINESTRING (1 1, 1 2, 2 2, 2 1, 1 1)")
  }

  "push down sedona predicates" >> {
    val gtFilter = ECQL.toFilter("bbox(geom, -30, 30, -10, 50)")
    val expectedCount = ds.getFeatureSource("points").getFeatures(gtFilter).size()

    val sql =
      """SELECT name, geom FROM points WHERE
        | sedona_ST_Intersects(geom, sedona_ST_PolygonFromEnvelope(-30.0, 30.0, -10.0, 50.0))
        |""".stripMargin
    val df = spark.sql(sql)
    df.queryExecution.optimizedPlan.children must haveLength(1)
    df.queryExecution.optimizedPlan.children.head must beAnInstanceOf[LogicalRelation]
    df.count() mustEqual expectedCount

    // Predicate can also be a mix of GeoMesa Spark JTS UDF with Apache Sedona Catalyst expressions
    val sql2 =
      """SELECT name, geom FROM points WHERE
        | sedona_ST_Intersects(geom, st_makeBox2d(st_point(-30, 30), st_point(-10, 50)))
        |""".stripMargin
    val df2 = spark.sql(sql2)
    df2.queryExecution.optimizedPlan.children must haveLength(1)
    df2.queryExecution.optimizedPlan.children.head must beAnInstanceOf[LogicalRelation]
    df2.count() mustEqual expectedCount
  }

  "spatial join" >> {
    val sql =
      """SELECT points.name as name1, polys.name as name2 FROM points
        | JOIN polys ON sedona_ST_Intersects(polys.geom, points.geom)
        |""".stripMargin
    val sedonaJoinDf = spark.sql(sql)
    sedonaJoinDf.queryExecution.executedPlan.toString().contains("RangeJoin") must beTrue
    val sedonaJoinResultCount = sedonaJoinDf.count()

    val broadcastJoinDf = pointsDf.join(broadcast(polysDf),
      st_intersects(polysDf.col("geom"), pointsDf.col("geom")))
    val broadcastJoinResultCount = broadcastJoinDf.count()
    sedonaJoinResultCount mustEqual broadcastJoinResultCount
  }

  "override JTS functions when prefix is empty" >> {
    val spark2 = spark.newSession()
    spark2.conf.set("spark.geomesa.sedona.udf.prefix", "")
    SQLTypes.init(spark2.sqlContext)
    val row1 = spark.sql("SELECT ST_PointFromText('POINT(-74.0060 40.7128)') AS geom").collect().head
    val row2 = spark2.sql("SELECT ST_PointFromText('-74.0060,40.7128', ',') AS geom").collect().head
    val point1 = row1.getAs[Point]("geom")
    val point2 = row2.getAs[Point]("geom")
    point1 mustEqual point2
  }
}
