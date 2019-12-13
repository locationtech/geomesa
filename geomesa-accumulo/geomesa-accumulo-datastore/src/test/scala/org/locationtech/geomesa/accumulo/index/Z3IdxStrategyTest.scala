/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.iterators.{BinAggregatingIterator, Z3Iterator}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.sfcurve.zorder.Z3
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Z3IdxStrategyTest extends Specification with TestWithDataStore {

  sequential // note: test doesn't need to be sequential but it actually runs faster this way

  val spec = "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indexes.enabled='z3'"

  val features =
    (0 until 10).map { i =>
      val name = s"name$i"
      val track = "track1"
      val dtg = s"2010-05-07T0$i:00:00.000Z"
      val geom = s"POINT(4$i 60)"
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttributes(Array[AnyRef](name, track, dtg, geom))
      sf
    } ++ (10 until 20).map { i =>
      val name = s"name$i"
      val track = "track2"
      val dtg = s"2010-05-${i}T$i:00:00.000Z"
      val geom = s"POINT(4${i - 10} 60)"
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttributes(Array[AnyRef](name, track, dtg, geom))
      sf
    } ++ (20 until 30).map { i =>
      val name = s"name$i"
      val track = "track3"
      val dtg = s"2010-05-${i}T${i-10}:00:00.000Z"
      val geom = s"POINT(6${i - 20} 60)"
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttributes(Array[AnyRef](name, track, dtg, geom))
      sf
    }
  addFeatures(features)

  def runQuery(filter: String, transforms: Array[String] = null): Iterator[SimpleFeature] =
    runQuery(new Query(sftName, ECQL.toFilter(filter), transforms))

  def runQuery(query: Query): Iterator[SimpleFeature] =
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))

  "Z3IdxStrategy" should {
    "print values" >> {
      skipped("used for debugging")
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      ds.manager.indices(sft).filter(_.name == Z3Index.name).flatMap(_.getTableNames()).foreach { table =>
        println(table)
        ds.connector.createScanner(table, new Authorizations()).foreach { r =>
          val prefix = 2 // table sharing + split
          val bytes = r.getKey.getRow.getBytes
          val keyZ = Longs.fromByteArray(bytes.drop(prefix))
          val (x, y, t) = Z3SFC(sft.getZ3Interval).invert(Z3(keyZ))
          val weeks = Shorts.fromBytes(bytes(prefix), bytes(prefix + 1))
          println(s"row: $weeks $x $y $t")
        }
      }
      println()
      success
    }

    "return all features for inclusive filter" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
    }

    "return some features for exclusive geom filter" >> {
      val filter = "bbox(geom, 38, 59, 45, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(6)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
    }

    "return some features for exclusive date filter" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
    }

    "work with whole world filter" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T08:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(5 to 8)
    }

    "work across week bounds" >> {
      val filter = "bbox(geom, 45, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(9)
      features.map(_.getID.toInt) must containTheSameElementsAs((6 to 9) ++ (15 to 19))
    }

    "work across 2 weeks" >> {
      val filter = "bbox(geom, 44.5, 59, 50, 61)" +
          " AND dtg between '2010-05-10T00:00:00.000Z' and '2010-05-17T23:59:59.999Z'"
      val features = runQuery(filter).toList
      features must haveSize(3)
      features.map(_.getID.toInt) must containTheSameElementsAs(15 to 17)
    }

    "work with whole world filter across week bounds" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(15)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 20)
    }

    "work with whole world filter across 3 week periods" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
        " AND dtg between '2010-05-08T06:00:00.000Z' and '2010-05-30T00:00:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(20)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 29)
    }

    "work with small bboxes and date ranges" >> {
      val filter = "bbox(geom, 40.999, 59.999, 41.001, 60.001)" +
        " AND dtg between '2010-05-07T00:59:00.000Z' and '2010-05-07T01:01:00.000Z'"
      val features = runQuery(filter).toList
      features must haveSize(1)
      features.head.getID.toInt mustEqual 1
    }

    "support AND'ed GT/LT for dates" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg >= '2010-05-07T06:00:00.000Z' AND dtg <= '2010-05-08T00:00:00.000Z'"
      // validate that date was used for range processing and not applied as a post-scan filter
      val qps = ds.getQueryPlan(new Query(sftName, ECQL.toFilter(filter)))
      forall(qps) { qp =>
        qp.iterators must haveLength(1)
        qp.iterators.head.getIteratorClass mustEqual classOf[Z3Iterator].getName
      }

      // validate correct results
      val features = runQuery(filter).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
    }

    "apply secondary filters" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T10:00:00.000Z'" +
          " AND name = 'name8'"
      val features = runQuery(filter).toList
      features must haveSize(1)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
    }

    "apply transforms" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = runQuery(filter, Array("name")).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features) { f =>
        f.getAttributeCount mustEqual 1
        f.getAttribute("name") must not(beNull)
      }
    }

    "apply functional transforms" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
      val features = runQuery(filter, Array("derived=strConcat('my', name)")).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features) { f =>
        f.getAttributeCount mustEqual 1
        f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d")
      }
    }

    "optimize for bin format" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 100)

      val qps = ds.getQueryPlan(query)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // reduce our scan ranges so that we get fewer iterator instances and some aggregation
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val aggregates = try {
        // the same simple feature gets reused - so make sure you access in serial order
        runQuery(query).map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      forall(bin.map(_.lat))(_ mustEqual 60.0)
      bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
    }

    "optimize for bin format with sorting" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 100)
      query.getHints.put(BIN_SORT, true)

      val qps = ds.getQueryPlan(query)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // reduce our scan ranges so that we get fewer iterator instances and some aggregation
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val aggregates = try {
        // the same simple feature gets reused - so make sure you access in serial order
        runQuery(query).map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      forall(aggregates) { a =>
        val window = a.grouped(16).map(BinaryOutputEncoder.decode(_).dtg).sliding(2).filter(_.length > 1)
        forall(window.toSeq)(w => w.head must beLessThanOrEqualTo(w(1)))
      }
      val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      forall(bin.map(_.lat))(_ mustEqual 60.0)
      bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
    }

    "optimize for bin format with label" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_LABEL, "name")
      query.getHints.put(BIN_BATCH_SIZE, 100)

      val qps = ds.getQueryPlan(query)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // reduce our scan ranges so that we get fewer iterator instances and some aggregation
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val aggregates = try {
        // the same simple feature gets reused - so make sure you access in serial order
        runQuery(query).map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      val bin = aggregates.flatMap(a => a.grouped(24).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      forall(bin.map(_.lat))(_ mustEqual 60.0)
      bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
      bin.map(_.label) must containAllOf((0 until 10).map(i => BinaryOutputEncoder.convertToLabel(s"name$i")))
    }

    "optimize for bin format with transforms" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter), Array("name", "geom"))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 100)

      val qps = ds.getQueryPlan(query)
      forall(qps)(_.iterators.map(_.getIteratorClass) must contain(classOf[BinAggregatingIterator].getCanonicalName))

      // reduce our scan ranges so that we get fewer iterator instances and some aggregation
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val aggregates = try {
        // the same simple feature gets reused - so make sure you access in serial order
        runQuery(query).map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
      } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      aggregates.size must beLessThan(10) // ensure some aggregation was done
      val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
      bin must haveSize(10)
      bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
      bin.map(_.dtg) must
          containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
      forall(bin.map(_.lat))(_ mustEqual 60.0)
      bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
    }

    // note: b/c sampling is per iterator, we don't get much reduction with the
    // small queries and large number of ranges in this test

    "support sampling" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      // reduce our scan ranges so that we get fewer iterator instances and some sampling
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val results = try { runQuery(query).toList } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      results.length must beLessThan(10)
    }

    "support sampling with transformations" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter), Array("name", "geom"))
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      // reduce our scan ranges so that we get fewer iterator instances and some sampling
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val results = try { runQuery(query).toList } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      results.length must beLessThan(10)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling by thread" >> {
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      query.getHints.put(SAMPLE_BY, "track")
      // reduce our scan ranges so that we get fewer iterator instances and some sampling
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val results = try { runQuery(query).toList } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      results.length must beLessThan(10)
    }

    "support sampling with bin queries" >> {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val filter = "bbox(geom, 38, 59, 51, 61)" +
          " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(BIN_TRACK, "track")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      query.getHints.put(SAMPLE_BY, "track")

      // reduce our scan ranges so that we get fewer iterator instances and some sampling
      QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
      val results = try {
        // have to evaluate attributes before pulling into collection, as the same sf is reused
        runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      } finally {
        QueryProperties.ScanRangesTarget.threadLocalValue.remove()
      }
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins.length must beLessThan(10)
    }
  }
}
