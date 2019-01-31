/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.{Filter, Id}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RecordIdxStrategyTest extends Specification with TestWithDataStore {

  case class IntersectionResult(idSeq: Option[Set[String]], correctIdSeq: Set[String] ) {
    // checks to see if idSeq and correctIdSeq are equivalent
    def okay: Boolean = (idSeq, correctIdSeq) match {
      case (None, s) if s.forall(_.isEmpty) => true
      case (Some(a), b) => (a diff b).isEmpty
      case _ => false
    }
  }

  def intersectionTestHelper(stringSeqToTest: Seq[String],
                             correctIntersection: String): IntersectionResult = {
    // extract the IDs from the a priori correct answer
    val trueIntersectionIds = ECQL.toFilter(correctIntersection)
      .asInstanceOf[Id].getIDs.asScala.toSet[AnyRef].map(_.toString)
    // process the string sequences for the test
    val filterSeq = stringSeqToTest.map(ECQL.toFilter)
    val combinedIDFilter = IdFilterStrategy.intersectIdFilters(ff.and(filterSeq.asJava))
    val computedIntersectionIds = if (combinedIDFilter.isEmpty) None else Some(combinedIDFilter)

    IntersectionResult(computedIntersectionIds, trueIntersectionIds)
  }

  override val spec = "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indexes.enabled='id'"

  val features = (0 until 20).map { i =>
    val name = s"name$i"
    val track = if (i < 10) "track1" else "track2"
    val dtg = f"2010-05-07T$i%02d:00:00.000Z"
    val geom = s"POINT(${i * 2} $i)"
    val sf = new ScalaSimpleFeature(sft, s"$i")
    sf.setAttributes(Array[AnyRef](name, track, dtg, geom))
    sf
  }

  addFeatures(features)

  val planner = ds.queryPlanner

  def runQuery(query: Query): CloseableIterator[SimpleFeature] = {
    query.getHints.put(QUERY_INDEX, IdIndex.name)
    planner.runQuery(sft, query, ExplainNull)
  }

  "RecordIdxStrategy" should {
    "support NOT queries" in {
      val query = new Query(sftName, ECQL.toFilter("NOT IN('2', '3')"))
      val results = runQuery(query).toList
      results must haveLength(18)
      results.map(_.getID) must containTheSameElementsAs(Seq("0", "1") ++ (4 until 20).map(_.toString))
    }

    "support bin queries" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("IN ('2', '3')"))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(2)
      bins.map(_.trackId) must containTheSameElementsAs(Seq("name2", "name3").map(_.hashCode))
    }

    "support sampling" in {
      val query = new Query(sftName, Filter.INCLUDE)
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(10)
    }

    "support sampling with cql" in {
      val query = new Query(sftName, ECQL.toFilter("track = 'track1'"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(5)
      forall(results)(_.getAttribute("track") mustEqual "track1")
    }

    "support sampling with transformations" in {
      val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(10)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      val query = new Query(sftName, ECQL.toFilter("track = 'track2'"), Array("name", "geom"))
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      val results = runQuery(query).toList
      results must haveLength(2)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling by thread" in {
      val query = new Query(sftName, Filter.INCLUDE)
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      query.getHints.put(SAMPLE_BY, "track")
      val results = runQuery(query).toList
      results.length must beLessThan(12)
      results.count(_.getAttribute("track") == "track1") must beLessThan(6)
      results.count(_.getAttribute("track") == "track2") must beLessThan(6)
    }

    "support sampling with bin queries" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      // important - id filters will create multiple ranges and cause multiple iterators to be created
      val query = new Query(sftName, ECQL.toFilter("dtg AFTER 2010-05-07T07:30:00.000Z"))
      query.getHints.put(BIN_TRACK, "track")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      query.getHints.put(SAMPLE_BY, "track")
      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins.length must beLessThan(5)
      bins.map(_.trackId) must containAllOf(Seq("track1", "track2").map(_.hashCode))
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
