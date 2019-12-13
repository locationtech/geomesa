/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.data

import com.google.common.collect.{ImmutableMap, ImmutableSetMultimap}
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloRasterQueryPlannerTest extends Specification with LazyLogging {

  sequential

  val availableResolutions = List[Double](45.0/256.0, 45.0/1024.0)

  val dataMap: ImmutableSetMultimap[Double, Int] = ImmutableSetMultimap.of(45.0/256.0, 1, 45.0/1024.0, 1)

  val boundsMap: ImmutableMap[Double, BoundingBox] = ImmutableMap.of(45.0/256.0, wholeworld, 45.0/1024.0, wholeworld)

  val arqp = AccumuloRasterQueryPlanner

  val testCases = List(
    (128, 45.0/256.0),
    (156, 45.0/256.0),
    (201, 45.0/256.0),
    (256, 45.0/256.0),
    (257, 45.0/1024.0),
    (432, 45.0/1024.0),
    (512, 45.0/1024.0),
    (1000, 45.0/1024.0),
    (1024, 45.0/1024.0),
    (1025, 45.0/1024.0),
    (2000, 45.0/1024.0)
  )

  def runTest(size: Int, expectedResolution: Double): MatchResult[Double] = {
    val q1 = generateQuery(0, 45, 0, 45, 45.0/size)
    val qp = arqp.getQueryPlan(q1, dataMap, boundsMap).get

    val rangeString = qp.ranges.head.getStartKey.getRow.toString
    val encodedDouble = rangeString.split("~")(0)

    val queryResolution = lexiDecodeStringToDouble(encodedDouble)

    logger.debug(s"Query pixel size: $size. Expected query resolution: $expectedResolution.  " +
      s"Returned query resolution $queryResolution.")

    val roundedResolution = BigDecimal(expectedResolution).round(mc).toDouble

    queryResolution should be equalTo roundedResolution
  }

  "RasterQueryPlanner" should {
    "return a valid resolution by rounding down" in {
      testCases.map {
        case (size, expected) =>
          runTest(size, expected)
      }
    }
  }

}
