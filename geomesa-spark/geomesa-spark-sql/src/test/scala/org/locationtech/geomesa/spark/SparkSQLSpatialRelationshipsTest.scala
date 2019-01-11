/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}
import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Point
import org.apache.spark.sql.{DataFrame, SQLContext, SQLTypes, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLSpatialRelationshipsTest extends Specification with LazyLogging {

  sequential

  "SQL spatial relationships" should {

    val geomFactory = JTSFactoryFinder.getGeometryFactory

    // we turn off the geo-index on the CQEngine DataStore because
    // BucketIndex doesn't do polygon <-> polygon comparisons properly;
    // acceptable performance-wise because the test data set is small
    val dsParams: JMap[String, String] = Map(
      "cqengine" -> "true",
      "geotools" -> "true",
      "useGeoIndex" -> "false")
    var ds: DataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var dfPoints: DataFrame = null
    var dfLines: DataFrame = null
    var dfBoxes: DataFrame = null

    val pointRef = "POINT(0 0)"
    val boxRef   = "POLYGON((0  0,  0 10, 10 10, 10  0,  0  0))"
    val lineRef  = "LINESTRING(0 10, 0 -10)"

    val points = Map(
      "int"    -> "POINT(5 5)",
      "edge"   -> "POINT(0 5)",
      "corner" -> "POINT(0 0)",
      "ext"    -> "POINT(-5 0)")

    val lines = Map(
      "touches" -> "LINESTRING(0 0, 1 0)",
      "crosses" -> "LINESTRING(-1 0, 1 0)",
      "disjoint" -> "LINESTRING(1 0, 2 0)")

    val boxes = Map(
      "int"     -> "POLYGON(( 1  1,  1  2,  2  2,  2  1,  1  1))",
      "intEdge" -> "POLYGON(( 0  1,  0  2,  1  2,  1  1,  0  1))",
      "overlap" -> "POLYGON((-1  1, -1  2,  1  2,  1  1, -1  1))",
      "extEdge" -> "POLYGON((-1  1, -1  2,  0  2,  0  1, -1  1))",
      "ext"     -> "POLYGON((-2  1, -2  2, -1  2, -1  1, -2  1))",
      "corner"  -> "POLYGON((-1 -1, -1  0,  0  0,  0 -1, -1 -1))")

    // before
    step {
      ds = DataStoreFinder.getDataStore(dsParams)
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)

      SparkSQLTestUtils.ingestPoints(ds, "points", points)
      dfPoints = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "points")
        .load()
      logger.info(dfPoints.schema.treeString)
      dfPoints.createOrReplaceTempView("points")

      SparkSQLTestUtils.ingestGeometries(ds, "lines", lines)
      dfLines = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "lines")
        .load()
      logger.info(dfLines.schema.treeString)
      dfLines.createOrReplaceTempView("lines")

      SparkSQLTestUtils.ingestGeometries(ds, "boxes", boxes)
      dfBoxes = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "boxes")
        .load()
      logger.info(dfBoxes.schema.treeString)
      dfBoxes.createOrReplaceTempView("boxes")
    }

    // DE-9IM comparisons
    def testData(sql: String, expectedNames: Seq[String]) = {
      val r = sc.sql(sql)
      val d = r.collect()
      val column = d.map(row => row.getAs[String]("name")).toSeq
      column must containTheSameElementsAs(expectedNames)
    }

    def testDirect(f: String, name: String, g1: String, g2: String, expected: Boolean) = {
     val sql = s"select $f(st_geomFromWKT('$g1'), st_geomFromWKT('$g2'))"
     val r = sc.sql(sql).collect()
     r.head.getBoolean(0) mustEqual expected
    }

    "st_contains" >> {
      testData(
        s"select * from points where st_contains(st_geomFromWKT('$boxRef'), geom)",
        Seq("int")
      )
      testData(
        s"select * from boxes where st_contains(st_geomFromWKT('$boxRef'), geom)",
        Seq("int", "intEdge")
      )

      testDirect("st_contains", "pt1", boxRef, points("int"),    true)
      testDirect("st_contains", "pt2", boxRef, points("edge"),   false)
      testDirect("st_contains", "pt3", boxRef, points("corner"), false)
      testDirect("st_contains", "pt4", boxRef, points("ext"),    false)

      testDirect("st_contains", "poly1", boxRef, boxes("int"),     true)
      testDirect("st_contains", "poly2", boxRef, boxes("intEdge"), true)
      testDirect("st_contains", "poly3", boxRef, boxes("overlap"), false)
      testDirect("st_contains", "poly4", boxRef, boxes("extEdge"), false)
      testDirect("st_contains", "poly5", boxRef, boxes("ext"),     false)
      testDirect("st_contains", "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_contains(null, null)").collect.head(0) must beNull
    }

    "st_covers" >> {
      // planner optimizer rules should prevent this clause from being
      // pushed down to the GeoTools store ("covers" is not a CQL op);
      // should be evaluated at the Spark level instead
      testData(
        s"select * from points where st_covers(st_geomFromWKT('$boxRef'), geom)",
        Seq("int", "edge", "corner")
      )
      testData(
        s"select * from boxes where st_covers(st_geomFromWKT('$boxRef'), geom)",
        Seq("int", "intEdge")
      )

      testDirect("st_covers", "pt1", boxRef, points("int"),    true)
      testDirect("st_covers", "pt2", boxRef, points("edge"),   true)
      testDirect("st_covers", "pt3", boxRef, points("corner"), true)
      testDirect("st_covers", "pt4", boxRef, points("ext"),    false)

      testDirect("st_covers", "poly1", boxRef, boxes("int"),     true)
      testDirect("st_covers", "poly2", boxRef, boxes("intEdge"), true)
      testDirect("st_covers", "poly3", boxRef, boxes("overlap"), false)
      testDirect("st_covers", "poly4", boxRef, boxes("extEdge"), false)
      testDirect("st_covers", "poly5", boxRef, boxes("ext"),     false)
      testDirect("st_covers", "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_covers(null, null)").collect.head(0) must beNull
    }

    "st_crosses" >> {
      testData(
        s"select * from lines where st_crosses(st_geomFromWKT('$lineRef'), geom)",
        Seq("crosses")
      )
      testDirect("st_crosses", "touches",  lineRef, lines("touches"),  false)
      testDirect("st_crosses", "crosses",  lineRef, lines("crosses"),  true)
      testDirect("st_crosses", "disjoint", lineRef, lines("disjoint"), false)

      sc.sql("select st_crosses(null, null)").collect.head(0) must beNull
    }

    "st_disjoint" >> {
      testData(
        s"select * from points where st_disjoint(st_geomFromWKT('$boxRef'), geom)",
        Seq("ext")
      )
      testData(
        s"select * from boxes where st_disjoint(st_geomFromWKT('$boxRef'), geom)",
        Seq("ext")
      )

      testDirect("st_disjoint", "pt1", boxRef, points("int"),    false)
      testDirect("st_disjoint", "pt2", boxRef, points("edge"),   false)
      testDirect("st_disjoint", "pt3", boxRef, points("corner"), false)
      testDirect("st_disjoint", "pt4", boxRef, points("ext"),    true)

      testDirect("st_disjoint", "poly1", boxRef, boxes("int"),     false)
      testDirect("st_disjoint", "poly2", boxRef, boxes("intEdge"), false)
      testDirect("st_disjoint", "poly3", boxRef, boxes("overlap"), false)
      testDirect("st_disjoint", "poly4", boxRef, boxes("extEdge"), false)
      testDirect("st_disjoint", "poly5", boxRef, boxes("ext"),     true)
      testDirect("st_disjoint", "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_disjoint(null, null)").collect.head(0) must beNull
    }

    "st_equals" >> {
      testData(
        s"select * from points where st_equals(st_geomFromWKT('POINT(0 0)'), geom)",
        Seq("corner")
      )
      testDirect("st_equals", "pt1", "POINT(0 0)", points("corner"), true)
      testDirect("st_equals", "pt2", "POINT(0 0)", points("edge"), false)

      testData(
        s"select * from lines where st_equals(st_geomFromWKT('${lines("crosses")}'), geom)",
        Seq("crosses")
      )
      testDirect("st_equals", "line", "LINESTRING(0 0, 1 1)", "LINESTRING(1 1, 0 0)", true)

      testData(
        s"select * from boxes where st_equals(st_geomFromWKT('${boxes("int")}'), geom)",
        Seq("int")
      )
      testDirect("st_equals", "polygon", boxRef, "POLYGON((10 0, 10 10, 0 10, 0 0, 10 0))", true)

      sc.sql("select st_equals(null, null)").collect.head(0) must beNull
    }

    "st_intersects" >> {
      testData(
        s"select * from points where st_intersects(st_geomFromWKT('$boxRef'), geom)",
        Seq("int", "edge", "corner")
      )
      testData(
        s"select * from boxes where st_intersects(st_geomFromWKT('$boxRef'), geom)",
        Seq("int", "intEdge", "overlap", "extEdge", "corner")
      )

      testDirect("st_intersects", "pt1", boxRef, points("int"),    true)
      testDirect("st_intersects", "pt2", boxRef, points("edge"),   true)
      testDirect("st_intersects", "pt3", boxRef, points("corner"), true)
      testDirect("st_intersects", "pt4", boxRef, points("ext"),    false)

      testDirect("st_intersects", "poly1", boxRef, boxes("int"),     true)
      testDirect("st_intersects", "poly2", boxRef, boxes("intEdge"), true)
      testDirect("st_intersects", "poly3", boxRef, boxes("overlap"), true)
      testDirect("st_intersects", "poly4", boxRef, boxes("extEdge"), true)
      testDirect("st_intersects", "poly5", boxRef, boxes("ext"),     false)
      testDirect("st_intersects", "poly6", boxRef, boxes("corner"),  true)

      sc.sql("select st_intersects(null, null)").collect.head(0) must beNull
    }

    "st_overlaps" >> {
      testData(
        s"select * from points where st_overlaps(st_geomFromWKT('$boxRef'), geom)",
        Seq()
      )
      testData(
        s"select * from boxes where st_overlaps(st_geomFromWKT('$boxRef'), geom)",
        Seq("overlap")
      )
      testDirect("st_overlaps", "pt1", boxRef, points("int"),    false)
      testDirect("st_overlaps", "pt2", boxRef, points("edge"),   false)
      testDirect("st_overlaps", "pt3", boxRef, points("corner"), false)
      testDirect("st_overlaps", "pt4", boxRef, points("ext"),    false)

      testDirect("st_overlaps", "poly1", boxRef, boxes("int"),     false)
      testDirect("st_overlaps", "poly2", boxRef, boxes("intEdge"), false)
      testDirect("st_overlaps", "poly3", boxRef, boxes("overlap"), true)
      testDirect("st_overlaps", "poly4", boxRef, boxes("extEdge"), false)
      testDirect("st_overlaps", "poly5", boxRef, boxes("ext"),     false)
      testDirect("st_overlaps", "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_overlaps(null, null)").collect.head(0) must beNull
    }

    "st_touches" >> {
      testData(
        s"select * from points where st_touches(st_geomFromWKT('$boxRef'), geom)",
        Seq("edge", "corner")
      )
      testData(
        s"select * from boxes where st_touches(st_geomFromWKT('$boxRef'), geom)",
        Seq("extEdge", "corner")
      )
      testDirect("st_touches", "pt1", boxRef, points("int"),    false)
      testDirect("st_touches", "pt2", boxRef, points("edge"),   true)
      testDirect("st_touches", "pt3", boxRef, points("corner"), true)
      testDirect("st_touches", "pt4", boxRef, points("ext"),    false)

      testDirect("st_touches", "poly1", boxRef, boxes("int"),     false)
      testDirect("st_touches", "poly2", boxRef, boxes("intEdge"), false)
      testDirect("st_touches", "poly3", boxRef, boxes("overlap"), false)
      testDirect("st_touches", "poly4", boxRef, boxes("extEdge"), true)
      testDirect("st_touches", "poly5", boxRef, boxes("ext"),     false)
      testDirect("st_touches", "poly6", boxRef, boxes("corner"),  true)

      sc.sql("select st_touches(null, null)").collect.head(0) must beNull
    }

    "st_within" >> {
      // reversed expressions because st_contains(g1, g2) == st_within(g2, g1)
      testData(
        s"select * from points where st_within(geom, st_geomFromWKT('$boxRef'))",
        Seq("int")
      )
      testData(
        s"select * from boxes where st_within(geom, st_geomFromWKT('$boxRef'))",
        Seq("int", "intEdge")
      )
      testDirect("st_within", "pt1", points("int"),    boxRef, true)
      testDirect("st_within", "pt2", points("edge"),   boxRef, false)
      testDirect("st_within", "pt3", points("corner"), boxRef, false)
      testDirect("st_within", "pt4", points("ext"),    boxRef, false)

      testDirect("st_within", "poly1", boxes("int"),     boxRef, true)
      testDirect("st_within", "poly2", boxes("intEdge"), boxRef, true)
      testDirect("st_within", "poly3", boxes("overlap"), boxRef, false)
      testDirect("st_within", "poly4", boxes("extEdge"), boxRef, false)
      testDirect("st_within", "poly5", boxes("ext"),     boxRef, false)
      testDirect("st_within", "poly6", boxes("corner"),  boxRef, false)

      sc.sql("select st_within(null, null)").collect.head(0) must beNull
    }

    "st_relate" >> {
      val l1 = "st_geomFromWKT('LINESTRING(1 2, 3 4)')"
      val l2 = "st_geomFromWKT('LINESTRING(5 6, 7 8)')"

      val r1 = sc.sql(s"select st_relate($l1, $l2)").collect()
      r1.head.getAs[String](0) mustEqual "FF1FF0102"

      val r2 = sc.sql(s"select st_relateBool($l1, $l2, 'FF*FF****')").collect()
      r2.head.getAs[Boolean](0) mustEqual true

      sc.sql("select st_relate(null, null)").collect.head(0) must beNull
      sc.sql("select st_relateBool(null, null, null)").collect.head(0) must beNull
    }

    // other relationship functions
    "st_area" >> {
      /* units of deg^2, which may not be that useful to anyone */
      val box1 = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"
      val box2 = "POLYGON((0 50, 0 60, 10 60, 10 50, 0 50))"

      val r1 = sc.sql(s"select st_area(st_geomFromWKT('$box1'))").collect()
      r1.head.getAs[Double](0) mustEqual 100.0

      val r2 = sc.sql(s"select st_area(st_geomFromWKT('$box2'))").collect()
      r2.head.getAs[Double](0) mustEqual 100.0

      val r3 = sc.sql(s"select * from boxes where st_intersects(st_geomFromWKT('$box1'), geom) and st_area(geom) > 1")
      r3.collect
        .map(row => row.getAs[String]("name"))
          .toSeq must containTheSameElementsAs(Seq("overlap"))

      val r4 = sc.sql(s"select * from boxes where st_area(geom) > 1 and st_intersects(st_geomFromWKT('$box1'), geom)")
      r4.collect
        .map(row => row.getAs[String]("name"))
          .toSeq must containTheSameElementsAs(Seq("overlap"))

      sc.sql("select st_area(null)").collect.head(0) must beNull
    }

    "st_centroid" >> {
      val r = sc.sql(s"select st_centroid(st_geomFromWKT('$boxRef'))")
      val d = r.collect()
      d.head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 5)").asInstanceOf[Point]

      sc.sql("select st_centroid(null)").collect.head(0) must beNull
    }

    "st_closestpoint" >> {
      val box1 = "st_geomFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')"
      val pt1  = "st_geomFromWKT('POINT(15 5)')"
      val r = sc.sql(s"select st_closestpoint($box1, $pt1)").collect()
      r.head.getAs[Point](0) mustEqual WKTUtils.read("POINT(10 5)")

      sc.sql("select st_closestpoint(null, null)").collect.head(0) must beNull
    }

    "st_distance" >> {
      val pt1 = "st_geomFromWKT('POINT(0 0)')"
      val pt2 = "st_geomFromWKT('POINT(10 0)')"

      val r1 = sc.sql(s"select st_distance($pt1, $pt2)").collect()
      r1.head.getAs[Double](0) mustEqual 10.0

      val r2 = sc.sql(s"select st_distanceSpheroid($pt1, $pt2)").collect()
      r2.head.getAs[Double](0) must beCloseTo(1113194.0, 1.0)

      sc.sql("select st_distance(null, null)").collect.head(0) must beNull
      sc.sql("select st_distanceSpheroid(null, null)").collect.head(0) must beNull
    }

    "st_length" >> {
      // length
      val r1 = sc.sql(
        s"select st_length(st_geomFromWKT('LINESTRING(0 0, 10 0)'))"
      ).collect()
      r1.head.getAs[Double](0) mustEqual 10.0

      // perimeter
      val r2 = sc.sql(
        s"select st_length(st_geomFromWKT('$boxRef'))"
      ).collect()
      r2.head.getAs[Double](0) mustEqual 40.0

      sc.sql("select st_length(null)").collect.head(0) must beNull
    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
