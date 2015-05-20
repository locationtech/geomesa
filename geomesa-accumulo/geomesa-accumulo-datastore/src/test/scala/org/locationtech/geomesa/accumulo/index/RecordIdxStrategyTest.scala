/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.index

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter._
import org.opengis.filter.Id
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._


case class IntersectionResult(idSeq: Option[Set[String]], correctIdSeq: Set[String] ) {
  // checks to see if idSeq and correctIdSeq are equivalent
  def okay: Boolean = (idSeq, correctIdSeq) match {
    case (None, s) if s.forall(_.isEmpty) => true
    case (Some(a), b) => (a diff b).isEmpty
    case _ => false
  }
}


@RunWith(classOf[JUnitRunner])
class RecordIdxStrategyTest extends Specification {

  def intersectionTestHelper(stringSeqToTest: Seq[String],
                             correctIntersection: String): IntersectionResult = {
    // extract the IDs from the a priori correct answer
    val trueIntersectionIds = ECQL.toFilter(correctIntersection)
      .asInstanceOf[Id].getIDs.asScala.toSet.map { a:AnyRef =>a.toString }
    // process the string sequences for the test
    val filterSeq=stringSeqToTest.map ( ECQL.toFilter )
    val combinedIDFilter = RecordIdxStrategy.intersectIDFilters(filterSeq)
    val computedIntersectionIds = combinedIDFilter.map {_.getIDs.asScala.toSet.map { a:AnyRef =>a.toString } }

    IntersectionResult(computedIntersectionIds, trueIntersectionIds)
  }

  "partitionID" should {
    "correctly extract multiple IDs" in {
      val idFilter1 = ECQL.toFilter("IN ('val56','val55')")
      val idFilter2 = ECQL.toFilter("IN('val59','val54')")
      val oFilter = ECQL.toFilter("attr2 = val56 AND attr2 = val60")
      val theFilter = ff.and (List(oFilter, idFilter1, idFilter2).asJava)
      val (idFilters, _) = partitionID(theFilter)
      val areSame = (idFilters.toSet diff Set(idFilter1, idFilter2)).isEmpty

      areSame must beTrue
    }

    "return an empty set when no ID filter is present" in {
      val oFilter = ECQL.toFilter("attr2 = val56 AND attr2 = val60")
      val (idFilters, _) = partitionID(oFilter)

      idFilters.isEmpty must beTrue
    }
  }

  "intersectIDFilters" should {
    "correctly handle the ANDs of a series of ID Filters" in {
      val stringSeq = Seq(
        "IN ('val54','val55','val56')",
        "IN ('val55','val56')",
        "IN ('val56','val55')",
        "IN ('val57','val55')")
      val intersectionString = "IN('val55')"
      val result = intersectionTestHelper(stringSeq, intersectionString)

      result.okay must beTrue
    }

    "correctly handle a single ID Filter" in {
      val singleIDFilter = "IN ('val55','val56')"
      val stringSeq = Seq(singleIDFilter)
      val intersectionString = singleIDFilter
      val result = intersectionTestHelper(stringSeq, intersectionString)

      result.okay must beTrue
    }

    "correctly handle the ANDs of a series of ID filters which have no intersection" in {
      val stringSeq = Seq(
        "IN ('val54','val55','val56')",
        "IN ('val55','val56')",
        "IN ('val56','val57')",
        "IN ('val57','val58')")
      val intersectionString = "IN('')"
      val result = intersectionTestHelper(stringSeq, intersectionString)

      result.okay must beTrue
    }

    "Correctly handle empty filter sequences" in {
      val stringSeq = Seq()
      val intersectionString = "IN('')"
      val result = intersectionTestHelper(stringSeq, intersectionString)

      result.okay must beTrue
    }
  }
}
