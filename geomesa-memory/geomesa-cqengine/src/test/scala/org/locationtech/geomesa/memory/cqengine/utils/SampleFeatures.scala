/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.locationtech.jts.geom.Point
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.language._
import scala.util.Random

object SampleFeatures {
  val spec = List(
    "Who:String:index=full",
    "What:Integer",
    "WhatLong:Long",
    "WhatFloat:Float",
    "WhatDouble:Double",
    "WhatBool:Boolean",
    "When:Date",
    "*Where:Point:srid=4326",
    "Why:String"
  ).mkString(",")
  val sft = SimpleFeatureTypes.createType("test", spec)

  val specIndexes = List(
    "Who:String:cq-index=default",
    "What:Integer:cq-index=navigable",
    "WhatLong:Long:cq-index=navigable",
    "WhatFloat:Float:cq-index=navigable",
    "WhatDouble:Double:cq-index=navigable",
    "WhatBool:Boolean",
    "When:Date:cq-index=navigable",
    "*Where:Point:srid=4326:cq-index=geometry",
    "Why:String" // Why can have nulls
  ).mkString(",")
  val sftWithIndexes = SimpleFeatureTypes.createType("test2", specIndexes)

  val cq = SFTAttributes(sft)
  val ff = CommonFactoryFinder.getFilterFactory2

  val MIN_DATE = ZonedDateTime.of(2014, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val seconds_per_year = 365L * 24L * 60L * 60L
  val string = "foo"

  def randDate = Date.from(MIN_DATE.plusSeconds(scala.math.round(scala.util.Random.nextFloat * seconds_per_year)).toInstant)

  val builder = new SimpleFeatureBuilder(sft)

  val names = Array("Addams", "Bierce", "Clemens", "Damon", "Evan", "Fred", "Goliath", "Harry")

  def getName: String = names(Random.nextInt(names.length))

  /* get non-uniformly distributed booleans */
  def randomBoolean(fracTrue: Double): java.lang.Boolean =
    Random.nextDouble() < fracTrue

  def getPoint: Point = {
    val minx = -180
    val miny = -90
    val dx = 360
    val dy = 180

    val x = minx + Random.nextDouble * dx
    val y = miny + Random.nextDouble * dy
    WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  }

  def buildFeature(i: Int): SimpleFeature = {
    builder.set("Who", getName)
    builder.set("What", Random.nextInt(10))
    builder.set("WhatLong", Random.nextInt(10).toLong)
    builder.set("WhatFloat", Random.nextFloat * 10.0F)
    builder.set("WhatDouble", Random.nextDouble() * 10.0)
    builder.set("WhatBool", randomBoolean(0.4))
    builder.set("When", randDate)
    builder.set("Where", getPoint)
    if (randomBoolean(0.4)) {
      // make sure n_{null} != n_{not null}
      builder.set("Why", string)
    }
    builder.buildFeature(i.toString)
  }
}

/**
  * List of sample filters to test sample features.
  * Each is designed so they are likely to return non-zero results over a reasonably
  * large (>= 1000) set of random features generated with SampleFeatures.buildFeature()
  */
object SampleFilters {
  implicit def stringToFilter(s: String): Filter = ECQL.toFilter(s)
  val ff = CommonFactoryFinder.getFilterFactory2

  // big enough so there are likely to be points in them
  val bbox1 = "POLYGON((-89 89, -1 89, -1 -89, -89 -89, -89 89))"
  val bbox2 = "POLYGON((-180 90, 0 90, 0 0, -180 0, -180 90))"

  val equalityFilters: Seq[Filter] = Seq(
    "Who = 'Addams'",
    "Who IN ('Addams', 'Bierce')",

    "What = 5",
    "What IN (1, 2, 3, 4)",
    "What <> 5",

    "WhatLong = 5",
    "WhatLong <> 5",
    "WhatLong IN (1, 2, 3, 4)",

    "When = '2014-07-01T00:00:00.000Z'",
    "When <> '2014-07-01T00:00:00.000Z'",

    "WhatBool = true",
    "WhatBool = false",

    ff.notEqual(ff.property("What"), ff.literal(5))  // CQL parser doesn't like "What != 5"
  )

  val specialFilters: Seq[Filter] = Seq(
    Filter.INCLUDE,
    ff.and("What = 5", Filter.INCLUDE),
    Filter.EXCLUDE,
    "IN (1)",
    "IN (1, 2, 3)"
  )

  val nullFilters: Seq[Filter] = Seq(
    "Why IS NULL",
    "Why IS NOT NULL",
    ff.isNil(ff.property("Why"), null)  // Not sure how to put in (E)CQL
  )

  val comparableFilters = Seq[Filter](
    "What > 5",
    "WhatLong > 5",
    "WhatFloat > 5.0",
    "WhatDouble > 5.0",
    "When > '2014-07-01T00:00:00Z'",

    "What >= 5",
    "WhatLong >= 5",
    "WhatFloat >= 5.0",
    "WhatDouble >= 5.0",
    "When >= '2014-07-01T00:00:00Z'",

    "What < 5",
    "WhatLong < 5",
    "WhatFloat < 5.0",
    "WhatDouble < 5.0",
    "When < '2014-07-01T00:00:00Z'",

    "What <= 5",
    "WhatLong <= 5",
    "WhatFloat <= 5.0",
    "WhatDouble <= 5.0",
    "When <= '2014-07-01T00:00:00Z'",

    "What BETWEEN 1 AND 5",
    "WhatLong BETWEEN 1 AND 5",
    "When BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'",
    "When BETWEEN '2014-01-01T00:00:00.000Z' AND '2014-06-30T00:00:00.000Z'"
  )

  val temporalFilters: Seq[Filter] = Seq(
    "When DURING 2014-01-01T00:00:00.000Z/2014-07-01T00:00:00.000Z",
    "When BEFORE 2014-09-01T00:00:00.000Z",
    "When AFTER  2014-03-01T00:00:00.000Z"
  )

  val oneLevelAndFilters: Seq[Filter] = Seq(
    s"(INTERSECTS(Where, $bbox1) AND INTERSECTS(Where, $bbox2))",
    s"(INTERSECTS(Where, $bbox1) AND Who = 'Addams')",
    s"(Who = 'Addams' AND INTERSECTS(Where, $bbox1))",
    s"INTERSECTS(Where, $bbox1) AND When DURING 2014-03-01T00:00:00.000Z/2014-09-30T23:59:59.000Z"
  )

  val oneLevelMultipleAndsFilters: Seq[Filter] = Seq(
    s"((INTERSECTS(Where, $bbox1) AND INTERSECTS(Where, $bbox2)) AND Who = 'Addams')",
    s"(INTERSECTS(Where, $bbox1) AND INTERSECTS(Where, $bbox2) AND Who = 'Addams')",
    s"(Who = 'Addams' AND ((INTERSECTS(Where, $bbox1) AND What <= 9) AND WhatFloat > 1.0))"
  )

  val oneLevelOrFilters: Seq[Filter] = Seq(
    s"(INTERSECTS(Where, $bbox1) OR INTERSECTS(Where, $bbox2))",
    s"(INTERSECTS(Where, $bbox1) OR Who = 'Addams')",
    s"(Who = 'Addams' OR INTERSECTS(Where, $bbox1))",
    s"(Who = 'Addams' OR Who = 'Bierce')",
    s"(Who = 'Addams' OR What = 1)"
  )

  val oneLevelMultipleOrsFilters: Seq[Filter] = Seq(
    s"(INTERSECTS(Where, $bbox1) OR INTERSECTS(Where, $bbox2) OR Who = 'Addams')",
    s"(Who = 'Addams' OR INTERSECTS(Where, $bbox1) OR What = 1)",
    s"(Who = 'Addams' OR Who = 'Bierce' or What = 1)",
    s"(Who = 'Addams' OR INTERSECTS(Where, $bbox1) OR Who = 'Bierce')"
  )

  val simpleNotFilters: Seq[Filter] = Seq(
    s"NOT (INTERSECTS(Where, $bbox1))",
    s"NOT (INTERSECTS(Where, $bbox2))",
    s"NOT (Who = 'Addams')",
    s"NOT (What = 1)",
    s"NOT (When BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
    s"NOT (When BETWEEN '2014-01-01T00:00:00.000Z' AND '2014-04-30T23:59:59.000Z')"
  )

  val spatialPredicates: Seq[Filter] = Seq(
    s"INTERSECTS(Where, $bbox2)",
    s"OVERLAPS(Where, $bbox2)",
    s"WITHIN(Where, $bbox2)",
    s"CONTAINS(Where, $bbox2)",
    s"CROSSES(Where, $bbox2)",
    s"BBOX(Where, -180, 0, 0, 90)"
  )

  val andedSpatialPredicates: Seq[Filter] = Seq(
    s"INTERSECTS(Where, $bbox1) AND OVERLAPS(Where, $bbox2)",
    s"INTERSECTS(Where, $bbox1) AND WITHIN(Where, $bbox2)",
    s"INTERSECTS(Where, $bbox1) AND DISJOINT(Where, $bbox2)",
    s"INTERSECTS(Where, $bbox1) AND CROSSES(Where, $bbox2)",
    s"OVERLAPS(Where, $bbox1) AND INTERSECTS(Where, $bbox2)",
    s"OVERLAPS(Where, $bbox1) AND WITHIN(Where, $bbox2)",
    s"OVERLAPS(Where, $bbox1) AND DISJOINT(Where, $bbox2)",
    s"OVERLAPS(Where, $bbox1) AND CROSSES(Where, $bbox2)",
    s"WITHIN(Where, $bbox1) AND INTERSECTS(Where, $bbox2)",
    s"WITHIN(Where, $bbox1) AND OVERLAPS(Where, $bbox2)",
    s"WITHIN(Where, $bbox1) AND DISJOINT(Where, $bbox2)",
    s"WITHIN(Where, $bbox1) AND CROSSES(Where, $bbox2)",
    s"DISJOINT(Where, $bbox1) AND INTERSECTS(Where, $bbox2)",
    s"DISJOINT(Where, $bbox1) AND OVERLAPS(Where, $bbox2)",
    s"DISJOINT(Where, $bbox1) AND WITHIN(Where, $bbox2)",
    s"DISJOINT(Where, $bbox1) AND CROSSES(Where, $bbox2)",
    s"CROSSES(Where, $bbox1) AND INTERSECTS(Where, $bbox2)",
    s"CROSSES(Where, $bbox1) AND OVERLAPS(Where, $bbox2)",
    s"CROSSES(Where, $bbox1) AND WITHIN(Where, $bbox2)",
    s"CROSSES(Where, $bbox1) AND DISJOINT(Where, $bbox2)")

  val attributePredicates: Seq[Filter] = Seq(
    "Who LIKE '%ams'",
    "Who LIKE 'Add%'",
    "Who LIKE '%da%'",
    "Who LIKE 'Ad_dams'",
    "Who ILIKE '%AMS'",
    "Who ILIKE 'ADD%'",
    "Who ILIKE '%DA%'"
  )

  val functionPredicates: Seq[Filter] = Seq(
    "strConcat(Who, What) = 'Addams1'",
    "WhatLong * WhatDouble > 1.0",
    "WhatLong * WhatDouble < 1.0",
    "WhatLong * WhatDouble >= 1.0",
    "WhatLong * WhatDouble <= 1.0",
    "WhatLong * WhatDouble <> 1.0",
    "strConcat(Who, What) = 'Addams1' AND WhatLong * WhatDouble < 1.0",
    "WhatLong * WhatDouble > 1.0 AND WhatLong * WhatDouble < 2.0",
    "WhatLong * WhatDouble >= 1.0 AND WhatLong * WhatDouble <= 2.0",
    "strConcat(Who, What) = 'Addams1' OR WhatLong * WhatDouble < 1.0",
    "WhatLong * WhatDouble > 1.0 OR WhatLong * WhatDouble < 2.0",
    "WhatLong * WhatDouble >= 1.0 OR WhatLong * WhatDouble <= 2.0"
  )
}
