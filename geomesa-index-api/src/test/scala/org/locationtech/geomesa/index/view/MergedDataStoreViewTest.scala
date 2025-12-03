/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

  import org.locationtech.geomesa.filter.{andFilters, decomposeAnd}

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createImmutableType("test",
    "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg=dtg")

  def emptyReader(): SimpleFeatureReader = new SimpleFeatureReader() {
    override def getFeatureType: SimpleFeatureType = sft
    override def next(): SimpleFeature = Iterator.empty.next
    override def hasNext: Boolean = false
    override def close(): Unit = {}
  }

  def stores(): Seq[(DataStore, Option[Filter])] = Seq.tabulate(3) { i =>
    val store = mock[DataStore]
    val filter = i match {
      case 0 => ECQL.toFilter("dtg < '2022-02-02T00:00:00.000Z'")
      case 1 => ECQL.toFilter("dtg >= '2022-02-02T00:00:00.000Z' AND dtg < '2022-02-03T00:00:00.000Z'")
      case 2 => ECQL.toFilter("dtg >= '2022-02-03T00:00:00.000Z'")
    }
    store.getSchema(sft.getTypeName) returns sft
    store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns emptyReader()
    store -> Some(filter)
  }

  // standardizes filters so that comparisons work as expected for non-equals but equivalent values
  def compareFilters(actual: Filter, expected: Filter): MatchResult[_] = {
    decomposeAnd(FastFilterFactory.optimize(sft, actual)) must
      containTheSameElementsAs(decomposeAnd(FastFilterFactory.optimize(sft, expected)) )
  }

  "MergedDataStoreView" should {
    "pass through INCLUDE filters" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        captor.getValue.getFilter mustEqual filter
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val noDates = Seq("IN ('1', '2')", "name = 'bar'", "age = 21", "bbox(geom,120,45,130,55)").map(ECQL.toFilter)
      noDates.foreach { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
      }
      foreach(stores) { case (store, Some(filter)) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was noDates.size.times(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        foreach(captor.getAllValues.asScala.map(_.getFilter).zip(noDates)) { case (actual, expected) =>
          compareFilters(actual, andFilters(Seq(filter, expected)))
        }
      }
    }

    "filter out queries from stores that aren't applicable - before" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val before = ECQL.toFilter("dtg during 2022-02-01T00:00:00.000Z/2022-02-01T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, before), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores.take(1)) { case (store, Some(filter)) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        decomposeAnd(FastFilterFactory.optimize(sft, captor.getValue.getFilter)) must
          containTheSameElementsAs(decomposeAnd(FastFilterFactory.optimize(sft, andFilters(Seq(before, filter)))))
      }
      foreach(stores.drop(1)) { case (store, _) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        captor.getValue.getFilter mustEqual Filter.EXCLUDE
      }
    }

    "filter out queries from stores that aren't applicable - after" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val after = ECQL.toFilter("dtg during 2022-02-04T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, after), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores.take(2)) { case (store, _) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        captor.getValue.getFilter mustEqual Filter.EXCLUDE
      }
      foreach(stores.drop(2)) { case (store, Some(filter)) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        compareFilters(captor.getValue.getFilter, andFilters(Seq(after, filter)))
      }
    }

    "filter out queries from stores that aren't applicable - overlapping" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val after = ECQL.toFilter("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, after), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        compareFilters(captor.getValue.getFilter, andFilters(Seq(after, filter)))
      }
    }

    "close iterators with parallel scans" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)

      val readers = ArrayBuffer.empty[CloseableFeatureReader]
      stores.foreach { case (store, _) =>
        store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
          val reader = new CloseableFeatureReader()
          readers += reader
          reader
        }
      }

      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      readers must haveLength(stores.length)
      foreach(readers)(_.closed must beTrue)
    }

    "close iterators with parallel push-down scans" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)

      val readers = ArrayBuffer.empty[CloseableFeatureReader]
      stores.foreach { case (store, _) =>
        store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
          val reader = new CloseableFeatureReader(BinaryOutputEncoder.BinEncodedSft)
          readers += reader
          reader
        }
      }

      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.BIN_GEOM, "geom")
      query.getHints.put(QueryHints.BIN_DTG, "dtg")
      query.getHints.put(QueryHints.BIN_TRACK, "name")
      WithClose(view.getFeatureReader(query, Transaction.AUTO_COMMIT))(_.hasNext)
      readers must haveLength(stores.length)
      foreach(readers)(_.closed must beTrue)
    }
  }

  class CloseableFeatureReader(val getFeatureType: SimpleFeatureType = sft)
      extends FeatureReader[SimpleFeatureType, SimpleFeature] {
    var closed: Boolean = false
    override def next(): SimpleFeature = null
    override def hasNext: Boolean = false
    override def close(): Unit = closed = true
  }
}
