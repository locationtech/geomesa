/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class XZ2IdxStrategyTest extends Specification with TestWithDataStore {

  val spec = "name:String,track:String,dtg:Date,*geom:Geometry:srid=4326;" +
      "geomesa.mixed.geometries='true',geomesa.indexes.enabled='xz2'"

  val features =
    (0 until 10).map { i =>
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttributes(Array[AnyRef](s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(40 6$i)"))
      sf
    } ++ (10 until 20).map { i =>
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttributes(Array[AnyRef](s"name$i", "track2", s"2010-05-07T$i:00:00.000Z",
        s"POLYGON((40 3${i - 10}, 42 3${i - 10}, 42 2${i - 10}, 40 2${i - 10}, 40 3${i - 10}))"))
      sf
    }
  addFeatures(features)

  val binHints = Map(BIN_TRACK -> "name", BIN_BATCH_SIZE -> 100)
  val sampleHalfHints = Map(SAMPLING -> new java.lang.Float(.5f))
  val sample20Hints = Map(SAMPLING -> new java.lang.Float(.2f))

  "XZ2IdxStrategy" should {
    "return all features for Filter.INCLUDE" >> {
      val filter = "INCLUDE"
      val features = execute(filter).toList
      features must haveSize(20)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 19)
    }

    "work with whole world filter" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)"
      val features = execute(filter).toList
      features must haveSize(20)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 19)
    }

    "return all for inclusive filter - points" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)"
      val features = execute(filter).toList
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
    }

    "return all for inclusive filter - polygons" >> {
      val filter = "bbox(geom, 35, 29, 45, 31)"
      val features = execute(filter).toList
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 19)
    }

    "return some for exclusive filter - points" >> {
      val filter = "bbox(geom, 35, 55, 45, 65)"
      val features = execute(filter).toList
      features must haveSize(6)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
    }

    "return some for exclusive filter - polygons" >> {
      val filter = "bbox(geom, 35, 38, 45, 40)"
      val features = execute(filter).toList
      features must haveSize(2)
      features.map(_.getID.toInt) must containTheSameElementsAs(18 to 19)
    }

    "work with small bboxes - points" >> {
      val filter = "bbox(geom, 39.999, 60.999, 40.001, 61.001)"
      val features = execute(filter).toList
      features must haveSize(1)
      features.head.getID.toInt mustEqual 1
    }

    "work with small bboxes - polygons" >> {
      val filter = "bbox(geom, 39.999, 21.999, 40.001, 22.001)"
      val features = execute(filter).toList
      features must haveSize(3)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 12)
    }

    "apply secondary filters - points" >> {
      val filter = "bbox(geom, 35, 55, 45, 75) AND name = 'name8'"
      val features = execute(filter).toList
      features must haveSize(1)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
    }

    "apply secondary filters - polygons" >> {
      val filter = "bbox(geom,  35, 22, 45, 24) AND name = 'name11'"
      val features = execute(filter).toList
      features must haveSize(1)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(11))
    }

    "apply transforms - points" >> {
      val filter = "bbox(geom, 35, 66, 45, 75)"
      val features = execute(filter, Some(Array("name"))).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 1)
      forall(features)((f: SimpleFeature) => f.getAttribute("name") must not(beNull))
    }

    "apply transforms - polygons" >> {
      val filter = "bbox(geom, 35, 22, 45, 24)"
      val features = execute(filter, Some(Array("name"))).toList
      features must haveSize(5)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 14)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 1)
      forall(features)((f: SimpleFeature) => f.getAttribute("name") must not(beNull))
    }

    "apply functional transforms" >> {
      val filter = "bbox(geom, 35, 66, 45, 75)"
      val features = execute(filter, Some(Array("derived=strConcat('my', name)"))).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 1)
      forall(features)((f: SimpleFeature) => f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d"))
    }

    "optimize for bin format" >> {
      val filter = "bbox(geom,  35, 55, 45, 75)"
      val qps = plan(filter, hints = binHints)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // the same simple feature gets reused - so make sure you access in serial order
      val aggregates = execute(filter, hints = binHints).map(f =>
        f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      bin.map(_.lat) must containAllOf((0 until 10).map(_ + 60.0f))
      forall(bin.map(_.lon))(_ mustEqual 40.0)
    }

    "optimize for bin format with label" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)"
      val hints = binHints.updated(BIN_LABEL, "name")

      val qps = plan(filter, hints = hints)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // the same simple feature gets reused - so make sure you access in serial order
      val aggregates = execute(filter, hints = hints).map(f =>
        f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      val bin = aggregates.flatMap(a => a.grouped(24).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      bin.map(_.lat) must containAllOf((0 until 10).map(_ + 60.0f))
      forall(bin.map(_.lon))(_ mustEqual 40.0)
      bin.map(_.label) must containAllOf((0 until 10).map(i => BinaryOutputEncoder.convertToLabel(s"name$i")))
    }

    "support sampling" in {
      val results = execute("INCLUDE", hints = sampleHalfHints).toList
      results must haveLength(10)
    }

    "support sampling with cql" in {
      // run a full table scan to ensure we only get 1 iterator instantiated
      val results = execute("track = 'track1'", hints = sampleHalfHints).toList
      results must haveLength(5)
      forall(results)(_.getAttribute("track") mustEqual "track1")
    }

    "support sampling with transformations" in {
      val results = execute("INCLUDE", Some(Array("name", "geom")), sampleHalfHints).toList
      results must haveLength(10)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      // run a full table scan to ensure we only get 1 iterator instantiated
      val results = execute("track = 'track1'", Some(Array("name", "geom")), sampleHalfHints).toList
      results must haveLength(5)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling by threading key" in {
      val results = execute("INCLUDE", hints = sampleHalfHints.updated(SAMPLE_BY, "track")).toList
      results.length must beLessThan(12)
      results.count(_.getAttribute("track") == "track1") must beLessThan(6)
      results.count(_.getAttribute("track") == "track2") must beLessThan(6)
    }

    "support sampling with bin queries" in {
      val hints = binHints.updated(BIN_TRACK, "track") ++ sample20Hints.updated(SAMPLE_BY, "track")
      val resultFeatures = execute("INCLUDE", hints = hints)
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX

      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = resultFeatures.map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveLength(4)
      bins.map(_.trackId) must containTheSameElementsAs {
        Seq("track1", "track1", "track2", "track2").map(_.hashCode)
      }
    }
  }

  def execute(ecql: String,
              transforms: Option[Array[String]] = None,
              hints: Map[_, _] = Map.empty,
              explain: Explainer = ExplainNull): Iterator[SimpleFeature] = {
    if (explain != ExplainNull) {
      ds.getQueryPlan(getQuery(ecql, transforms, hints), explainer = explain)
    }
    SelfClosingIterator(ds.getFeatureReader(getQuery(ecql, transforms, hints), Transaction.AUTO_COMMIT))
  }

  def plan(ecql: String,
           transforms: Option[Array[String]] = None,
           hints: Map[_, _] = Map.empty,
           explain: Explainer = ExplainNull): Seq[AccumuloQueryPlan] =
    ds.getQueryPlan(getQuery(ecql, transforms, hints), explainer = explain)

  def getQuery(ecql: String, transforms: Option[Array[String]], hints: Map[_, _]): Query = {
    val query = transforms match {
      case None    => new Query(sftName, ECQL.toFilter(ecql))
      case Some(t) => new Query(sftName, ECQL.toFilter(ecql), t)
    }
    hints.foreach { case (k, v) => query.getHints.put(k, v) }
    query
  }
}
