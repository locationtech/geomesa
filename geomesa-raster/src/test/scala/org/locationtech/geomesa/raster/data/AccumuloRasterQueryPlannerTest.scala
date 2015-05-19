/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.data

import com.google.common.collect.ImmutableSetMultimap
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloRasterQueryPlannerTest extends Specification {

  sequential

  val schema = RasterIndexSchema()
  val availableResolutions = List[Double](45.0/256.0, 45.0/1024.0)

  val dataMap: ImmutableSetMultimap[Double, Int] = ImmutableSetMultimap.of(45.0/256.0, 1, 45.0/1024.0, 1)

  val arqp = AccumuloRasterQueryPlanner(schema)

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
    val qp = arqp.getQueryPlan(q1, dataMap).get

    val rangeString = qp.ranges.head.getStartKey.getRow.toString
    val encodedDouble = rangeString.split("~")(0)

    val queryResolution = lexiDecodeStringToDouble(encodedDouble)

    println(s"Query pixel size: $size. Expected query resolution: $expectedResolution.  " +
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
