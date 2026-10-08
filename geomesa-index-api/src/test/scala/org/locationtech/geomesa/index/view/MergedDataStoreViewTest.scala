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
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.stats.impl.{CountStat, MinMax}
import org.locationtech.geomesa.security.{FilteringAuthorizationsProvider, ThreadLocalAuthorizationsProvider}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
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
    "propagate request authorizations to parallel counts and bounds" in {
      val provider = new FilteringAuthorizationsProvider(
        new ThreadLocalAuthorizationsProvider, java.util.Arrays.asList[String]("A", "B"))
      val callers = new java.util.concurrent.CopyOnWriteArrayList[Long]()
      val sources = Seq.fill(2) {
        val source = mock[SimpleFeatureSource]
        respond(source.getCount(ArgumentMatchers.any[Query]())) {
          callers.add(Thread.currentThread().getId)
          provider.getAuthorizations.size()
        }
        def bounds(): ReferencedEnvelope = {
          callers.add(Thread.currentThread().getId)
          val n = provider.getAuthorizations.size().toDouble
          new ReferencedEnvelope(0, n, 0, n, org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
        }
        respond(source.getBounds)(bounds())
        respond(source.getBounds(ArgumentMatchers.any[Query]()))(bounds())
        source -> None
      }
      val view = new MergedFeatureSourceView(null, sources, parallel = true, sft)
      val query = new Query(sft.getTypeName)
      val users = Seq(Seq("A", "C"), Seq("A", "B"), Seq.empty[String]).map(_.asJava)
      foreach(users) {
        auths => ThreadLocalAuthorizationsProvider.withAuthorizations(auths) {
          val expected = provider.getAuthorizations.size()
          view.getCount(query) mustEqual 2 * expected
          view.getBounds.getMaxX mustEqual expected.toDouble
          view.getBounds(query).getMaxX mustEqual expected.toDouble
        }
      }
      callers.asScala must not contain Thread.currentThread().getId
      provider.getAuthorizations.isEmpty must beTrue
    }

    "propagate request authorizations to parallel statistics" in {
      val provider = new ThreadLocalAuthorizationsProvider
      val stores = Seq.fill(2) {
        val store = mock[StatsStore]
        val stats = mock[GeoMesaStats]
        store.stats returns stats
        respond(stats.getCount(sft, Filter.INCLUDE, true, new Hints())) {
          Some(provider.getAuthorizations.size().toLong)
        }
        respond(stats.getMinMax[Integer](sft, "age", Filter.INCLUDE, true)) {
          val stat = new MinMax[Integer](sft, "age")
          val feature = mock[SimpleFeature]
          feature.getAttribute(sft.indexOf("age")) returns Integer.valueOf(provider.getAuthorizations.size())
          stat.observe(feature)
          Some(stat)
        }
        respond(stats.getStat[CountStat](sft, "Count()", Filter.INCLUDE, true)) {
          val stat = new CountStat(sft)
          (0 until provider.getAuthorizations.size()).foreach(_ => stat.observe(null))
          Some(stat)
        }
        store -> None
      }
      val stats = new MergedDataStoreView.MergedStats(stores, parallel = true)
      val users = Seq(Seq("A"), Seq("A", "B"), Seq.empty[String]).map(_.asJava)
      foreach(users) {
        auths => ThreadLocalAuthorizationsProvider.withAuthorizations(auths) {
          stats.getCount(sft, Filter.INCLUDE, true, new Hints()) must beSome(2L * auths.size())
          stats.getMinMax[Integer](sft, "age", Filter.INCLUDE, true).map(_.min) must
            beSome(Integer.valueOf(auths.size()))
          stats.getStat[CountStat](sft, "Count()", Filter.INCLUDE, true).map(_.count) must
            beSome(2L * auths.size())
        }
      }
      provider.getAuthorizations.isEmpty must beTrue
    }

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

  // Evaluate on the calling executor thread, rather than while setting up the mock.
  private def respond[T](call: T)(value: => T): Unit = {
    org.mockito.Mockito.when(call).thenAnswer(new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = value
    })
  }

  trait StatsStore extends DataStore with HasGeoMesaStats

  class CloseableFeatureReader(val getFeatureType: SimpleFeatureType = sft)
      extends FeatureReader[SimpleFeatureType, SimpleFeature] {
    var closed: Boolean = false
    override def next(): SimpleFeature = null
    override def hasNext: Boolean = false
    override def close(): Unit = closed = true
  }
}
