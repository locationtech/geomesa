/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.Feature
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.feature.visitor.CalcResult
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.index.stats.impl.CountStat
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedDataStoreViewTest.CountingTaskDecorator
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.UnaryOperator
import scala.collection.mutable.ArrayBuffer

class MergedDataStoreViewTest extends SpecificationWithJUnit with Mockito with MergedDataStoreViewTestSupport {

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

  def featureSource(i: Int): SimpleFeatureSource = {
    val source = mock[SimpleFeatureSource]
    source.getCount(ArgumentMatchers.any()) returns i
    source.getBounds() returns new ReferencedEnvelope(0, i, 0, i, CRS_EPSG_4326)
    source.getBounds(ArgumentMatchers.any()) returns new ReferencedEnvelope(0, i, 0, i, CRS_EPSG_4326)
    source.getSchema returns sft
    source
  }

  def stores(): Seq[MergedSource[DataStore]] = Seq.tabulate(3) { i =>
    val store = mock[DataStore]
    val provider = i match {
      case 0 => MergedFilterProvider.to("2022-02-02T00:00:00.000Z")
      case 1 => MergedFilterProvider.and(Seq(
        MergedFilterProvider.from("2022-02-02T00:00:00.000Z"),
        MergedFilterProvider.to("2022-02-03T00:00:00.000Z")))
      case 2 => MergedFilterProvider.from("2022-02-03T00:00:00.000Z")
    }
    store.getSchema(sft.getTypeName) returns sft
    store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns emptyReader()
    val fs = featureSource(i)
    store.getFeatureSource(ArgumentMatchers.anyString()) returns fs
    MergedSource(store, provider, new TaskDecorator(None))
  }

  // standardizes filters so that comparisons work as expected for non-equals but equivalent values
  def compareFilters(actual: Filter, expected: Filter): MatchResult[_] = {
    decomposeAnd(FastFilterFactory.optimize(sft, actual)) must
      containTheSameElementsAs(decomposeAnd(FastFilterFactory.optimize(sft, expected)) )
  }

  private def staticView(
      source: Store,
      destination: Store,
      parallel: Boolean = false,
      taskDecorator: Option[UnaryOperator[Runnable]] = None): MergedDataStoreView = {
    val sources = Seq(
      MergedSource[DataStore](source, MergedFilterProvider.to(boundary.toString)),
      MergedSource[DataStore](destination, MergedFilterProvider.from(boundary.toString)))
      .map(_.copy(decorator = new TaskDecorator(taskDecorator)))
    new MergedDataStoreView(sources, deduplicate = false, parallel = parallel)
  }

  "MergedDataStoreView" should {
    "propagate an explicit task decorator to parallel readers, counts, bounds and stats" in {
      import scala.collection.JavaConverters._
      val source = new ExactStore; val destination = new ExactStore
      populate(source, "source"); populate(destination, "destination")
      val store = staticView(source, destination,
        parallel = true, taskDecorator = Some(decorator))
      try {
        val featureSource = store.getFeatureSource(typeName)
        val operations: Seq[() => Any] = Seq(
          () => readParallel(store) mustEqual 3,
          () => featureSource.getCount(new Query(typeName)) mustEqual 3,
          () => featureSource.getBounds().getMaxX mustEqual 2d,
          () => store.stats.getCount(store.getSchema(typeName), exact = true) must beSome(3L))
        Seq("first-user", "second-user").foreach { user =>
          context.set(user)
          operations.foreach { operation =>
            source.identities.clear(); destination.identities.clear()
            operation()
            source.identities.asScala.toSet mustEqual Set(user)
            destination.identities.asScala.toSet mustEqual Set(user)
            context.get() mustEqual user
          }
        }
      } finally { context.remove(); store.dispose() }
      success
    }

    "advertise only types shared by every delegate" in {
      val source = new Store; val destination = new Store
      populate(source, "source"); populate(destination, "destination")
      populate(source, "source", "source-only")
      populate(destination, "destination", "destination-only")
      val store = staticView(source, destination)
      try {
        store.getTypeNames.toSet mustEqual Set(typeName)
        store.getSchema("source-only") must throwA[java.io.IOException]
        store.getSchema("destination-only") must throwA[java.io.IOException]
        populate(destination, "destination", "source-only")
        store.getTypeNames.toSet mustEqual Set(typeName, "source-only")
      } finally { store.dispose() }
    }

    "retain merged ownership for feature sources and process visitors" in {
      val source = new Store; val destination = new Store
      populate(source, "source"); populate(destination, "destination")
      val store = new MergedDataStoreView(Seq(MergedSource[DataStore](source), MergedSource[DataStore](destination)),
        deduplicate = false, parallel = false, namespace = Some("example"))
      try {
        val fs = store.getFeatureSource(typeName)
        fs.getDataStore must beTheSameAs(store)
        fs.getDataStore.getNames.get(0).getNamespaceURI mustEqual "example"
        var visitorSource: SimpleFeatureSource = null
        val visitor = new GeoMesaProcessVisitor {
          override def execute(s: SimpleFeatureSource, q: Query): Unit = { visitorSource = s }
          override def visit(feature: Feature): Unit = {}
          override def getResult: CalcResult = null
        }
        GeoMesaFeatureCollection.visit(fs.getFeatures(), visitor)
        visitorSource.getDataStore must beTheSameAs(store)
      } finally { store.dispose() }
    }

    "pass through INCLUDE filters" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        captor.getValue.getFilter mustEqual store.filter(sft.getTypeName, QueryContext(Filter.INCLUDE))
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val noDates = Seq("IN ('1', '2')", "name = 'bar'", "age = 21", "bbox(geom,120,45,130,55)").map(ECQL.toFilter)
      noDates.foreach { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
      }
      foreach(stores) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was noDates.size.times(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        foreach(captor.getAllValues.asScala.map(_.getFilter).zip(noDates)) { case (actual, expected) =>
          compareFilters(actual, andFilters(Seq(store.filter(sft.getTypeName, QueryContext(Filter.INCLUDE)), expected)))
        }
      }
    }

    "filter out queries from stores that aren't applicable - before" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val before = ECQL.toFilter("dtg during 2022-02-01T00:00:00.000Z/2022-02-01T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, before), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores.take(1)) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        decomposeAnd(FastFilterFactory.optimize(sft, captor.getValue.getFilter)) must
          containTheSameElementsAs(decomposeAnd(FastFilterFactory.optimize(sft,
            andFilters(Seq(before, store.filter(sft.getTypeName, QueryContext(Filter.INCLUDE)))))))
      }
      foreach(stores.drop(1)) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        decomposeAnd(captor.getValue.getFilter) must contain(Filter.EXCLUDE)
      }
    }

    "filter out queries from stores that aren't applicable - after" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val after = ECQL.toFilter("dtg during 2022-02-04T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, after), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores.take(2)) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        decomposeAnd(captor.getValue.getFilter) must contain(Filter.EXCLUDE)
      }
      foreach(stores.drop(2)) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        compareFilters(captor.getValue.getFilter, andFilters(Seq(after, store.filter(sft.getTypeName, QueryContext(Filter.INCLUDE)))))
      }
    }

    "filter out queries from stores that aren't applicable - overlapping" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)

      val after = ECQL.toFilter("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, after), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { store =>
        val captor = ArgumentCaptor.forClass(classOf[Query])
        there was one(store.source).getFeatureReader(captor.capture(), ArgumentMatchers.eq(Transaction.AUTO_COMMIT))
        compareFilters(captor.getValue.getFilter, andFilters(Seq(after, store.filter(sft.getTypeName, QueryContext(Filter.INCLUDE)))))
      }
    }

    "close iterators with parallel scans" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)

      val readers = ArrayBuffer.empty[CloseableFeatureReader]
      stores.foreach { store =>
        store.source.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
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
      stores.foreach { store =>
        store.source.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
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

    "support decorators through data store params" in {
      val params = java.util.Map.of("geomesa.merged.task.decorator", classOf[CountingTaskDecorator].getName)
      val decorator = MergedDataStoreViewFactory.TaskDecoratorParam.lookup(params)
      decorator must beAnInstanceOf[CountingTaskDecorator]
    }

    "decorate parallel queries" in {
      val decorator = new CountingTaskDecorator()
      val stores = this.stores().map(_.copy(decorator = new TaskDecorator(Some(decorator))))
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)
      CloseableIterator(view.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList must beEmpty
      decorator.count mustEqual stores.length
    }

    "decorate parallel counts and bounds calls" in {
      val decorator = new CountingTaskDecorator()
      val stores = this.stores().map(_.copy(decorator = new TaskDecorator(Some(decorator))))
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)
      val fs = view.getFeatureSource(sft.getTypeName)
      val query = new Query(sft.getTypeName)
      fs.getCount(query) mustEqual 3
      decorator.count mustEqual 3
      fs.getBounds(query) mustEqual new ReferencedEnvelope(0, 2, 0, 2, CRS_EPSG_4326)
      decorator.count mustEqual 6
      fs.getBounds() mustEqual new ReferencedEnvelope(0, 2, 0, 2, CRS_EPSG_4326)
      decorator.count mustEqual 9
    }

    "decorate parallel statistics" in {
      val decorator = new CountingTaskDecorator()
      val stores = this.stores().map(_.copy(decorator = new TaskDecorator(Some(decorator))))
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)
      view.stats.getCount(sft) must beNone
      decorator.count mustEqual 3
      view.stats.getMinMax(sft, "age") must beNone
      decorator.count mustEqual 6
      view.stats.getStat[CountStat](sft, "Count()") must beNone
      decorator.count mustEqual 9
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

object MergedDataStoreViewTest {

  class CountingTaskDecorator extends UnaryOperator[Runnable] {

    private val counter = new AtomicInteger(0)

    def count: Int = counter.get

    override def apply(t: Runnable): Runnable = () => {
      counter.incrementAndGet()
      t.run()
    }
  }
}
