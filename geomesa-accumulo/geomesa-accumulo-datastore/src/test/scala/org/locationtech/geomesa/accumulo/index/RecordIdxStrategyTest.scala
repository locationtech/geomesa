/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
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
class RecordIdxStrategyTest extends Specification with TestWithDataStore {

  def intersectionTestHelper(stringSeqToTest: Seq[String],
                             correctIntersection: String): IntersectionResult = {
    // extract the IDs from the a priori correct answer
    val trueIntersectionIds = ECQL.toFilter(correctIntersection)
      .asInstanceOf[Id].getIDs.asScala.toSet.map { a:AnyRef =>a.toString }
    // process the string sequences for the test
    val filterSeq=stringSeqToTest.map ( ECQL.toFilter )
    val combinedIDFilter = RecordIdxStrategy.intersectIdFilters(filterSeq)
    val computedIntersectionIds = combinedIDFilter.map {_.getIDs.asScala.toSet.map { a:AnyRef =>a.toString } }

    IntersectionResult(computedIntersectionIds, trueIntersectionIds)
  }

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  val features = (0 until 5).map { i =>
    val name = s"name$i"
    val dtg = s"2010-05-07T0$i:00:00.000Z"
    val geom = s"POINT(40 6$i)"
    val sf = new ScalaSimpleFeature(s"$i", sft)
    sf.setAttributes(Array[AnyRef](name, dtg, geom))
    sf
  }

  addFeatures(features)

  "RecordIdxStrategy" should {
    "support bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("IN ('2', '3')"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      val queryPlanner = new QueryPlanner(sft, ds.getFeatureEncoding(sft),
        ds.getIndexSchemaFmt(sftName), ds, ds.strategyHints(sft))
      val results = queryPlanner.runQuery(query, Some(StrategyType.RECORD)).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toSeq
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(2)
      bins.map(_.trackId) must containAllOf(Seq("name2", "name3").map(_.hashCode.toString))
    }
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
