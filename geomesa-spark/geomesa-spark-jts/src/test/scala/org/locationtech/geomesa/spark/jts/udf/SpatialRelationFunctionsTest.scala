/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.{lang => jl}

import org.locationtech.jts.geom.Point
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, TypedColumn, _}
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.{SQLFunctionHelper, WKTUtils}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialRelationFunctionsTest extends Specification with TestEnvironment {
  type DFRelation = (Column, Column) => TypedColumn[Any, jl.Boolean]
  sequential

  "SQL spatial relationships" should {

    var dfPoints: DataFrame = null
    var dfLines: DataFrame = null
    var dfBoxes: DataFrame = null

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
      import spark.implicits._

      dfPoints = points.mapValues(WKTUtils.read).toSeq.toDF("name", "geom")
      dfPoints.createOrReplaceTempView("points")

      dfLines = lines.mapValues(WKTUtils.read).toSeq.toDF("name", "geom")
      dfLines.createOrReplaceTempView("lines")

      dfBoxes = boxes.mapValues(WKTUtils.read).toSeq.toDF("name", "geom")
      dfBoxes.createOrReplaceTempView("boxes")
    }

    // DE-9IM comparisons
    def testData(r: DataFrame, expectedNames: Seq[String]) = {
      val d = r.collect()
      val column = d.map(row => row.getAs[String]("name")).toSeq
      column must containTheSameElementsAs(expectedNames)
    }

    def testDirect(relation: DFRelation, name: String, g1: String, g2: String, expected: Boolean) = {
      import spark.implicits._
      dfBlank.select(relation(st_geomFromWKT(g1), st_geomFromWKT(g2)).as[Boolean]).first mustEqual expected
      // NB: Hack to pull SQL-land name from columnar function expression.
      val relationName = SQLFunctionHelper.columnName(relation(lit(null), lit(null))).split('(').head
      val sql = s"select $relationName(st_geomFromWKT('$g1'), st_geomFromWKT('$g2'))"
      sc.sql(sql).as[Boolean].first mustEqual expected
    }

    "st_contains" >> {
      testData(
        sc.sql(s"select * from points where st_contains(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int")
      )
      testData(
        dfPoints.where(st_contains(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int")
      )
      testData(
        sc.sql(s"select * from boxes where st_contains(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int", "intEdge")
      )
      testData(
        dfBoxes.where(st_contains(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int", "intEdge")
      )

      testDirect(st_contains, "pt1", boxRef, points("int"),    true)
      testDirect(st_contains, "pt2", boxRef, points("edge"),   false)
      testDirect(st_contains, "pt3", boxRef, points("corner"), false)
      testDirect(st_contains, "pt4", boxRef, points("ext"),    false)
      testDirect(st_contains, "poly1", boxRef, boxes("int"),     true)
      testDirect(st_contains, "poly2", boxRef, boxes("intEdge"), true)
      testDirect(st_contains, "poly3", boxRef, boxes("overlap"), false)
      testDirect(st_contains, "poly4", boxRef, boxes("extEdge"), false)
      testDirect(st_contains, "poly5", boxRef, boxes("ext"),     false)
      testDirect(st_contains, "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_contains(null, null)").collect.head(0) must beNull
      dfBlank.select(st_contains(lit(null), lit(null))).first must beNull
    }

    "st_covers" >> {
      testData(
        sc.sql(s"select * from points where st_covers(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int", "edge", "corner")
      )
      testData(
        dfPoints.where(st_covers(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int", "edge", "corner")
      )
      testData(
        sc.sql(s"select * from boxes where st_covers(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int", "intEdge")
      )
      testData(
        dfBoxes.where(st_covers(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int", "intEdge")
      )

      testDirect(st_covers, "pt1", boxRef, points("int"),    true)
      testDirect(st_covers, "pt2", boxRef, points("edge"),   true)
      testDirect(st_covers, "pt3", boxRef, points("corner"), true)
      testDirect(st_covers, "pt4", boxRef, points("ext"),    false)

      testDirect(st_covers, "poly1", boxRef, boxes("int"),     true)
      testDirect(st_covers, "poly2", boxRef, boxes("intEdge"), true)
      testDirect(st_covers, "poly3", boxRef, boxes("overlap"), false)
      testDirect(st_covers, "poly4", boxRef, boxes("extEdge"), false)
      testDirect(st_covers, "poly5", boxRef, boxes("ext"),     false)
      testDirect(st_covers, "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_covers(null, null)").collect.head(0) must beNull
      dfBlank.select(st_covers(lit(null), lit(null))).first must beNull
    }

    "st_crosses" >> {
      testData(
        sc.sql(s"select * from lines where st_crosses(st_geomFromWKT('$lineRef'), geom)"),
        Seq("crosses")
      )
      testData(
        dfLines.where(st_crosses(st_geomFromWKT(lineRef), col("geom"))),
        Seq("crosses")
      )
      testDirect(st_crosses, "touches",  lineRef, lines("touches"),  false)
      testDirect(st_crosses, "crosses",  lineRef, lines("crosses"),  true)
      testDirect(st_crosses, "disjoint", lineRef, lines("disjoint"), false)

      sc.sql("select st_crosses(null, null)").collect.head(0) must beNull
      dfBlank.select(st_crosses(lit(null), lit(null))).first must beNull
    }

    "st_disjoint" >> {
      testData(
        sc.sql(s"select * from points where st_disjoint(st_geomFromWKT('$boxRef'), geom)"),
        Seq("ext")
      )
      testData(
        dfPoints.where(st_disjoint(st_geomFromWKT(boxRef), col("geom"))),
        Seq("ext")
      )
      testData(
        sc.sql(s"select * from boxes where st_disjoint(st_geomFromWKT('$boxRef'), geom)"),
        Seq("ext")
      )
      testData(
        dfBoxes.where(st_disjoint(st_geomFromWKT(boxRef), col("geom"))),
        Seq("ext")
      )

      testDirect(st_disjoint, "pt1", boxRef, points("int"),    false)
      testDirect(st_disjoint, "pt2", boxRef, points("edge"),   false)
      testDirect(st_disjoint, "pt3", boxRef, points("corner"), false)
      testDirect(st_disjoint, "pt4", boxRef, points("ext"),    true)

      testDirect(st_disjoint, "poly1", boxRef, boxes("int"),     false)
      testDirect(st_disjoint, "poly2", boxRef, boxes("intEdge"), false)
      testDirect(st_disjoint, "poly3", boxRef, boxes("overlap"), false)
      testDirect(st_disjoint, "poly4", boxRef, boxes("extEdge"), false)
      testDirect(st_disjoint, "poly5", boxRef, boxes("ext"),     true)
      testDirect(st_disjoint, "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_disjoint(null, null)").collect.head(0) must beNull
      dfBlank.select(st_disjoint(lit(null), lit(null))).first must beNull
    }

    "st_equals" >> {
      testData(
        sc.sql(s"select * from points where st_equals(st_geomFromWKT('POINT(0 0)'), geom)"),
        Seq("corner")
      )
      testData(
        dfPoints.where(st_equals(st_geomFromWKT("POINT(0 0)"), col("geom"))),
        Seq("corner")
      )
      testDirect(st_equals, "pt1", "POINT(0 0)", points("corner"), true)
      testDirect(st_equals, "pt2", "POINT(0 0)", points("edge"), false)

      testData(
        sc.sql(s"select * from lines where st_equals(st_geomFromWKT('${lines("crosses")}'), geom)"),
        Seq("crosses")
      )
      testData(
        dfLines.where(st_equals(st_geomFromWKT(lines("crosses")), col("geom"))),
        Seq("crosses")
      )
      testDirect(st_equals, "line", "LINESTRING(0 0, 1 1)", "LINESTRING(1 1, 0 0)", true)

      testData(
        sc.sql(s"select * from boxes where st_equals(st_geomFromWKT('${boxes("int")}'), geom)"),
        Seq("int")
      )
      testData(
        dfBoxes.where(st_equals(st_geomFromWKT(boxes("int")), col("geom"))),
        Seq("int")
      )
      testDirect(st_equals, "polygon", boxRef, "POLYGON((10 0, 10 10, 0 10, 0 0, 10 0))", true)

      sc.sql("select st_equals(null, null)").collect.head(0) must beNull
      dfBlank.select(st_equals(lit(null), lit(null))).first must beNull
    }

    "st_intersects" >> {
      testData(
        sc.sql(s"select * from points where st_intersects(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int", "edge", "corner")
      )
      testData(
        dfPoints.where(st_intersects(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int", "edge", "corner")
      )
      testData(
        sc.sql(s"select * from boxes where st_intersects(st_geomFromWKT('$boxRef'), geom)"),
        Seq("int", "intEdge", "overlap", "extEdge", "corner")
      )
      testData(
        dfBoxes.where(st_intersects(st_geomFromWKT(boxRef), col("geom"))),
        Seq("int", "intEdge", "overlap", "extEdge", "corner")
      )

      testDirect(st_intersects, "pt1", boxRef, points("int"),    true)
      testDirect(st_intersects, "pt2", boxRef, points("edge"),   true)
      testDirect(st_intersects, "pt3", boxRef, points("corner"), true)
      testDirect(st_intersects, "pt4", boxRef, points("ext"),    false)

      testDirect(st_intersects, "poly1", boxRef, boxes("int"),     true)
      testDirect(st_intersects, "poly2", boxRef, boxes("intEdge"), true)
      testDirect(st_intersects, "poly3", boxRef, boxes("overlap"), true)
      testDirect(st_intersects, "poly4", boxRef, boxes("extEdge"), true)
      testDirect(st_intersects, "poly5", boxRef, boxes("ext"),     false)
      testDirect(st_intersects, "poly6", boxRef, boxes("corner"),  true)

      sc.sql("select st_intersects(null, null)").collect.head(0) must beNull
      dfBlank.select(st_intersects(lit(null), lit(null))).first must beNull
    }

    "st_overlaps" >> {
      testData(
        sc.sql(s"select * from points where st_overlaps(st_geomFromWKT('$boxRef'), geom)"),
        Seq()
      )
      testData(
        dfPoints.where(st_overlaps(st_geomFromWKT(boxRef), col("geom"))),
        Seq()
      )
      testData(
        sc.sql(s"select * from boxes where st_overlaps(st_geomFromWKT('$boxRef'), geom)"),
        Seq("overlap")
      )
      testData(
        dfBoxes.where(st_overlaps(st_geomFromWKT(boxRef), col("geom"))),
        Seq("overlap")
      )
      testDirect(st_overlaps, "pt1", boxRef, points("int"),    false)
      testDirect(st_overlaps, "pt2", boxRef, points("edge"),   false)
      testDirect(st_overlaps, "pt3", boxRef, points("corner"), false)
      testDirect(st_overlaps, "pt4", boxRef, points("ext"),    false)

      testDirect(st_overlaps, "poly1", boxRef, boxes("int"),     false)
      testDirect(st_overlaps, "poly2", boxRef, boxes("intEdge"), false)
      testDirect(st_overlaps, "poly3", boxRef, boxes("overlap"), true)
      testDirect(st_overlaps, "poly4", boxRef, boxes("extEdge"), false)
      testDirect(st_overlaps, "poly5", boxRef, boxes("ext"),     false)
      testDirect(st_overlaps, "poly6", boxRef, boxes("corner"),  false)

      sc.sql("select st_overlaps(null, null)").collect.head(0) must beNull
      dfBlank.select(st_overlaps(lit(null), lit(null))).first must beNull
    }

    "st_touches" >> {
      testData(
        sc.sql(s"select * from points where st_touches(st_geomFromWKT('$boxRef'), geom)"),
        Seq("edge", "corner")
      )
      testData(
        dfPoints.where(st_touches(st_geomFromWKT(boxRef), col("geom"))),
        Seq("edge", "corner")
      )
      testData(
        sc.sql(s"select * from boxes where st_touches(st_geomFromWKT('$boxRef'), geom)"),
        Seq("extEdge", "corner")
      )
      testData(
        dfBoxes.where(st_touches(st_geomFromWKT(boxRef), col("geom"))),
        Seq("extEdge", "corner")
      )

      testDirect(st_touches, "pt1", boxRef, points("int"),    false)
      testDirect(st_touches, "pt2", boxRef, points("edge"),   true)
      testDirect(st_touches, "pt3", boxRef, points("corner"), true)
      testDirect(st_touches, "pt4", boxRef, points("ext"),    false)

      testDirect(st_touches, "poly1", boxRef, boxes("int"),     false)
      testDirect(st_touches, "poly2", boxRef, boxes("intEdge"), false)
      testDirect(st_touches, "poly3", boxRef, boxes("overlap"), false)
      testDirect(st_touches, "poly4", boxRef, boxes("extEdge"), true)
      testDirect(st_touches, "poly5", boxRef, boxes("ext"),     false)
      testDirect(st_touches, "poly6", boxRef, boxes("corner"),  true)

      sc.sql("select st_touches(null, null)").collect.head(0) must beNull
      dfBlank.select(st_touches(lit(null), lit(null))).first must beNull
    }

    "st_within" >> {
      // reversed expressions because st_contains(g1, g2) == st_within(g2, g1)
      testData(
        sc.sql(s"select * from points where st_within(geom, st_geomFromWKT('$boxRef'))"),
        Seq("int")
      )
      testData(
        dfPoints.where(st_within(col("geom"), st_geomFromWKT(boxRef))),
        Seq("int")
      )
      testData(
        sc.sql(s"select * from boxes where st_within(geom, st_geomFromWKT('$boxRef'))"),
        Seq("int", "intEdge")
      )
      testData(
        dfBoxes.where(st_within(col("geom"), st_geomFromWKT(boxRef))),
        Seq("int", "intEdge")
      )

      testDirect(st_within, "pt1", points("int"),    boxRef, true)
      testDirect(st_within, "pt2", points("edge"),   boxRef, false)
      testDirect(st_within, "pt3", points("corner"), boxRef, false)
      testDirect(st_within, "pt4", points("ext"),    boxRef, false)

      testDirect(st_within, "poly1", boxes("int"),     boxRef, true)
      testDirect(st_within, "poly2", boxes("intEdge"), boxRef, true)
      testDirect(st_within, "poly3", boxes("overlap"), boxRef, false)
      testDirect(st_within, "poly4", boxes("extEdge"), boxRef, false)
      testDirect(st_within, "poly5", boxes("ext"),     boxRef, false)
      testDirect(st_within, "poly6", boxes("corner"),  boxRef, false)

      sc.sql("select st_within(null, null)").collect.head(0) must beNull
      dfBlank.select(st_within(lit(null), lit(null))).first must beNull
    }

    "st_relate" >> {
      import spark.implicits._
      val ls1 = "LINESTRING(1 2, 3 4)"
      val ls2 = "LINESTRING(5 6, 7 8)"
      val l1 = s"st_geomFromWKT('$ls1')"
      val l2 = s"st_geomFromWKT('$ls2')"

      val expected = "FF1FF0102"
      sc.sql(s"select st_relate($l1, $l2)").as[String].first mustEqual expected
      dfBlank.select(st_relate(st_geomFromWKT(ls1), st_geomFromWKT(ls2))).first mustEqual expected

      sc.sql(s"select st_relateBool($l1, $l2, 'FF*FF****')").as[Boolean].first mustEqual true
      dfBlank.select(st_relateBool(st_geomFromWKT(ls1), st_geomFromWKT(ls2), lit("FF*FF****"))).first mustEqual true

      sc.sql("select st_relate(null, null)").collect.head(0) must beNull
      dfBlank.select(st_relate(lit(null), lit(null))).first must beNull
      sc.sql("select st_relateBool(null, null, null)").collect.head(0) must beNull
      dfBlank.select(st_relateBool(lit(null), lit(null), lit(null))).first must beNull
    }

    // other relationship functions
    "st_area" >> {
      import spark.implicits._
      /* units of deg^2, which may not be that useful to anyone */
      val box1 = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"
      val box2 = "POLYGON((0 50, 0 60, 10 60, 10 50, 0 50))"

      sc.sql(s"select st_area(st_geomFromWKT('$box1'))").as[Double].first mustEqual 100.0
      dfBlank.select(st_area(st_geomFromWKT(box1))).first mustEqual 100.0

      sc.sql(s"select st_area(st_geomFromWKT('$box2'))").as[Double].first mustEqual 100.0
      dfBlank.select(st_area(st_geomFromWKT(box2))).first mustEqual 100.0

      val r3 = sc.sql(s"select * from boxes where st_intersects(st_geomFromWKT('$box1'), geom) and st_area(geom) > 1")
      r3.collect
        .map(row => row.getAs[String]("name"))
          .toSeq must containTheSameElementsAs(Seq("overlap"))

      dfBoxes.select("name").as[String]
        .where(st_intersects(st_geomFromWKT(box1), col("geom")) && st_area(col("geom")) > 1)
        .collect.toSeq must containTheSameElementsAs(Seq("overlap"))

      val r4 = sc.sql(s"select * from boxes where st_area(geom) > 1 and st_intersects(st_geomFromWKT('$box1'), geom)")
      r4.collect
        .map(row => row.getAs[String]("name"))
          .toSeq must containTheSameElementsAs(Seq("overlap"))

      dfBoxes.select("name").as[String]
        .where(st_area(col("geom")) > 1 && st_intersects(st_geomFromWKT(box1), col("geom")))
        .collect.toSeq must containTheSameElementsAs(Seq("overlap"))

      sc.sql("select st_area(null)").collect.head(0) must beNull
      dfBlank.select(st_area(lit(null))).first must beNull
    }

    "st_centroid" >> {
      val expected = WKTUtils.read("POINT(5 5)").asInstanceOf[Point]
      sc.sql(s"select st_centroid(st_geomFromWKT('$boxRef'))").as[Point].first mustEqual expected

      dfBlank.select(st_centroid(st_geomFromWKT(boxRef))).first mustEqual expected

      sc.sql("select st_centroid(null)").collect.head(0) must beNull
      dfBlank.select(st_centroid(lit(null))).first must beNull
    }

    "st_closestpoint" >> {
      val box1 = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"
      val pt1  = "POINT(15 5)"

      val expected = WKTUtils.read("POINT(10 5)")

      sc.sql(s"select st_closestPoint(st_geomFromWKT('$box1'), st_geomFromWKT('$pt1'))").as[Point].first mustEqual expected
      dfBlank.select(st_closestPoint(st_geomFromWKT(box1), st_geomFromWKT(pt1))).first mustEqual expected

      sc.sql("select st_closestPoint(null, null)").collect.head(0) must beNull
      dfBlank.select(st_closestPoint(lit(null), lit(null))).first must beNull
    }

    "st_distance" >> {
      import spark.implicits._
      val pt1 = "POINT(0 0)"
      val pt2 = "POINT(10 0)"

      sc.sql(s"select st_distance(st_geomFromWKT('$pt1'), st_geomFromWKT('$pt2'))")
        .as[Double].first mustEqual 10.0
      dfBlank.select(st_distance(st_geomFromWKT(pt1), st_geomFromWKT(pt2))).first mustEqual 10.0

      val expected = beCloseTo(1111950.0, 1.0)
      sc.sql(s"select st_distanceSphere(st_geomFromWKT('$pt1'), st_geomFromWKT('$pt2'))")
        .as[Double].first must expected
      dfBlank.select(st_distanceSphere(st_geomFromWKT(pt1), st_geomFromWKT(pt2))).first.doubleValue() must expected

      sc.sql("select st_distance(null, null)").collect.head(0) must beNull
      dfBlank.select(st_distance(lit(null), lit(null))).first must beNull

      sc.sql("select st_distanceSphere(null, null)").collect.head(0) must beNull
      dfBlank.select(st_distanceSphere(lit(null), lit(null))).first must beNull
    }

    "st_length" >> {
      import spark.implicits._
      // length
      sc.sql(s"select st_length(st_geomFromWKT('LINESTRING(0 0, 10 0)'))").as[Double].first mustEqual 10.0
      dfBlank.select(st_length(st_geomFromWKT("LINESTRING(0 0, 10 0)"))).first mustEqual 10.0

      // perimeter
      sc.sql(s"select st_length(st_geomFromWKT('$boxRef'))").as[Double].first mustEqual 40.0
      dfBlank.select(st_length(st_geomFromWKT(boxRef))).first mustEqual 40.0

      sc.sql("select st_length(null)").collect.head(0) must beNull
      dfBlank.select(st_length(lit(null))).first must beNull
    }

    "st_translate" >> {
      val expected = WKTUtils.read("LINESTRING(1 2, 11 2)")
      val trans = dfBlank.select(st_translate(st_geomFromWKT("LINESTRING(0 0, 10 0)"), 1, 2)).first
      trans mustEqual expected
    }

    "st_aggregateDistanceSphere" >> {
      val p1 = points("int")
      val p2 = points("edge")
      dfBlank.select(st_aggregateDistanceSphere(array(st_geomFromWKT(p1), st_geomFromWKT(p2)))).first must not(throwAn[Exception])
    }

    "st_lengthSphere" >> {
      val line = "LINESTRING(1 2, 11 2)"
      dfBlank.select(st_lengthSphere(st_castToLineString(st_geomFromWKT(line)))).first must not(throwAn[Exception])
    }

    // after
    step {
      spark.stop()
    }
  }
}
