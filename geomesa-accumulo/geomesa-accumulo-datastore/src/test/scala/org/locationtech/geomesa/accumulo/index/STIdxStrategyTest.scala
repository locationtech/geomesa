/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
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
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class STIdxStrategyTest extends Specification with TestWithDataStore {

  override val spec = "name:String,track:String,dtg:Date,*geom:Point:srid=4326;table.indexes.enabled='st_idx'"

  val features = (0 until 20).map { i =>
    val name = s"name$i"
    val track = if (i < 10) "track1" else "track2"
    val dtg = f"2010-05-07T$i%02d:00:00.000Z"
    val geom = s"POINT(${i * 2} $i)"
    val sf = new ScalaSimpleFeature(s"$i", sft)
    sf.setAttributes(Array[AnyRef](name, track, dtg, geom))
    sf
  }

  addFeatures(features)

  val queryPlanner = QueryPlanner(sft, ds)
  def runQuery(query: Query) = queryPlanner.runQuery(query, Some(StrategyType.ST))

  "STIdxStrategy" should {
    "support sampling" in {
      val query = new Query(sftName, Filter.INCLUDE)
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(10)
    }

    "support sampling with cql" in {
      val query = new Query(sftName, ECQL.toFilter("track = 'track1'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(5)
      forall(results)(_.getAttribute("track") mustEqual "track1")
    }

    "support sampling with transformations" in {
      val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(10)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      val query = new Query(sftName, ECQL.toFilter("track = 'track2'"), Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.2f))
      val results = runQuery(query).toList
      results must haveLength(2)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling by thread" in {
      val query = new Query(sftName, Filter.INCLUDE)
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      query.getHints.put(SAMPLE_BY_KEY, "track")
      val results = runQuery(query).toList
      results must haveLength(10)
      results.count(_.getAttribute("track") == "track1") mustEqual 5
      results.count(_.getAttribute("track") == "track2") mustEqual 5
    }

    "support sampling with bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, Filter.INCLUDE)
      query.getHints.put(BIN_TRACK_KEY, "track")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.2f))
      query.getHints.put(SAMPLE_BY_KEY, "track")

      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveLength(4)
      bins.map(_.trackId) must containTheSameElementsAs(Seq("track1", "track1", "track2", "track2").map(_.hashCode.toString))
    }
  }
}
