/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi, DataStoreFinder, Query, SimpleFeatureReader, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data.memory.MemoryDataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.{AuthorizationsProvider, HasAuthorizationsProvider}
import org.locationtech.geomesa.index.view.MergedDataStoreViewFactory._
import org.locationtech.geomesa.index.view.MergedDataStoreViewFactoryTest._
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.awt.RenderingHints
import java.time.Instant
import java.util.Date
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.function.UnaryOperator
import scala.collection.mutable.ArrayBuffer

class MergedDataStoreViewFactoryTest extends SpecificationWithJUnit {

  sequential

  import scala.collection.JavaConverters._

  private def fixture(run: DelegateFactory => Any): Unit = {
    val factory = new DelegateFactory
    TestFilter.instances = Vector.empty
    TestFilter.current = "name <> 'b'"
    DataStoreFinder.registerFactory(factory)
    try { run(factory) } finally {
      DataStoreFinder.deregisterFactory(factory)
      factory.opened.filter(_.disposals == 0).foreach(_.dispose())
    }
  }

  private def storeParams(id: String): Map[String, String] = Map(
    StoreKey -> id,
    StoreFilterParam.key -> "name <> 'c'",
    DateOffsetEndParam.key -> "2026-01-02T00:00:00Z")

  private def create(stores: Map[String, String]*): MergedDataStoreView = createWithParams(Map.empty, stores: _*)

  private def createWithParams(params: Map[String, AnyRef], stores: Map[String, String]*): MergedDataStoreView = {
    val config = ConfigFactory.empty().withValue("stores",
      ConfigValueFactory.fromIterable(stores.map(p => ConfigValueFactory.fromMap(p.asJava)).asJava))
    val merged = (params + (ConfigParam.key -> config.root().render(ConfigRenderOptions.concise()))).asJava
    DataStoreFinder.getDataStore(merged).asInstanceOf[MergedDataStoreView]
  }

  private def read(features: SimpleFeatureCollection): List[String] = {
    val reader = CloseableIterator(features.features())
    try { reader.map(_.getAttribute("name").toString).toList.sorted } finally { reader.close() }
  }

  "MergedDataStoreViewFactory" should {
    "load dynamic filters and combine them with ECQL, per-layer, date and query filters" in {
      fixture { factory =>
        val view = create(
          storeParams("first") ++ Map(StoreFilterProviderParam.key -> classOf[TestFilter].getName,
            s"${StoreFilterParam.key}.other" -> "name <> 'a'"),
          storeParams("second") + (StoreFilterParam.key -> "EXCLUDE"))
        val provider = TestFilter.instances.head
        try {
          val source = view.getFeatureSource("example")
          val captured = source.getFeatures()
          provider.lookups mustEqual 0
          read(captured) mustEqual List("a")
          read(view.getFeatureSource("other").getFeatures()) mustEqual List("c")
          provider.lookups mustEqual 2
          TestFilter.current = "name <> 'a'"
          read(captured) mustEqual List("b")
          provider.lookups mustEqual 3
          read(source.getFeatures()) mustEqual List("b")
          provider.lookups mustEqual 4
          read(source.getFeatures(new Query("example", ECQL.toFilter("name <> 'b'")))) must beEmpty
          view.stats.getCount(view.getSchema("example"), exact = true) must beSome(1L)
        } finally { view.dispose() }
        provider.closes mustEqual 1
        factory.opened.map(_.disposals).toList mustEqual List(1, 1)
      }
      success
    }

    "retain existing static filters when no provider is configured" in {
      fixture { _ =>
        val view = create(storeParams("first"))
        try {
          read(view.getFeatureSource("example").getFeatures()) mustEqual List("a", "b")
          TestFilter.instances must beEmpty
        } finally { view.dispose() }
      }
      success
    }

    "accept unquoted provider keys in delegate configurations" in {
      fixture { _ =>
        val config = s"""stores = [{
          $StoreKey = first
          ${StoreFilterProviderParam.key} = "${classOf[TestFilter].getName}"
          geomesa.merged.store.date.end = "2026-01-02T00:00:00Z"
        }]"""
        val view = new MergedDataStoreViewFactory().createDataStore(Map(ConfigParam.key -> config).asJava)
        try { read(view.getFeatureSource("example").getFeatures()) mustEqual List("a", "c") }
        finally { view.dispose() }
      }
      success
    }

    "forward only provider configuration and close initialized providers if a subsequent initialization fails" in {
      fixture { factory =>
        create(
          storeParams("first") ++ Map(StoreFilterProviderParam.key -> classOf[TestFilter].getName,
            s"${StoreFilterProviderParam.key}.option" -> "first-value"),
          storeParams("second") ++ Map(StoreFilterProviderParam.key -> classOf[TestFilter].getName,
            s"${StoreFilterProviderParam.key}.fail" -> "true")) must
            throwA[IllegalArgumentException]("provider initialization failed")
        TestFilter.instances.map(_.config) mustEqual Vector(Map("option" -> "first-value"), Map("fail" -> "true"))
        TestFilter.instances.map(_.closes) mustEqual Vector(1, 1)
        factory.opened.map(_.disposals).toList mustEqual List(1, 1)
      }
      success
    }

    "use delegate authorization decorators unless an explicit decorator is configured" in {
      foreach(Seq(false, true)) { explicit =>
        fixture { factory =>
          val decorator = new UnaryOperator[Runnable] {
            override def apply(task: Runnable): Runnable = {
              val captured = s"explicit:${context.get()}"
              () => withContext(captured)(task.run())
            }
          }
          val params: Map[String, AnyRef] = Map(ParallelScanParam.key -> "true") ++
            (if (explicit) { Map(TaskDecoratorParam.key -> decorator) } else { Map.empty[String, AnyRef] })
          val view = createWithParams(params, storeParams("first"), storeParams("second"))
          try {
            foreach(Seq("alice", "bob")) { user =>
              factory.opened.foreach(_.identities.clear())
              withContext(user) {
                read(view.getFeatureSource("example").getFeatures()) mustEqual List("a", "a", "b", "b")
                context.get() mustEqual user
              }
              foreach(factory.opened) { store =>
                store.identities.asScala.toSet mustEqual Set(s"${if (explicit) { "explicit" } else { store.id }}:$user")
              }
            }
          } finally { view.dispose() }
        }
        success
      }
    }

    "reject invalid provider classes and clean up every opened resource" in {
      Seq("missing.FilterClass", classOf[String].getName, classOf[NoDefaultConstructor].getName).foreach { invalid =>
        fixture { factory =>
          create(storeParams("first") + (StoreFilterProviderParam.key -> classOf[TestFilter].getName),
            storeParams("second") + (StoreFilterProviderParam.key -> invalid)) must
            throwA[Exception]
          factory.opened.map(_.disposals).toList mustEqual List(1, 1)
          TestFilter.instances.map(_.closes) mustEqual Vector(1)
        }
      }
      success
    }

    "close providers and delegates if loading a subsequent store fails" in {
      fixture { factory =>
        create(storeParams("first") + (StoreFilterProviderParam.key -> classOf[TestFilter].getName),
          storeParams("missing")) must throwA[IllegalArgumentException]
        factory.opened.map(_.disposals).toList mustEqual List(1)
        TestFilter.instances.map(_.closes) mustEqual Vector(1)
      }
      success
    }
  }
}

object MergedDataStoreViewFactoryTest {
  val StoreKey = "merged-filter-test-store"

  object TestFilter {
    var instances = Vector.empty[TestFilter]
    var current = "name <> 'b'"
  }

  class TestFilter extends MergedFilterProvider {
    var lookups = 0
    var closes = 0
    TestFilter.instances :+= this

    var config = Map.empty[String, String]
    override def init(config: Map[String, String]): Unit = {
      this.config = config
      if (config.get("fail").contains("true")) { throw new IllegalArgumentException("provider initialization failed") }
    }
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
      lookups += 1
      Some(ECQL.toFilter(TestFilter.current))
    }
    override def close(): Unit = { closes += 1 }
  }

  class NoDefaultConstructor(unused: String) extends MergedFilterProvider {
    override def init(config: Map[String, String]): Unit = {}
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = None
    override def close(): Unit = {}
  }

  val context = new ThreadLocal[String]()

  def withContext[T](value: String)(f: => T): T = {
    val previous = context.get()
    context.set(value)
    try { f } finally {
      if (previous == null) { context.remove() } else { context.set(previous) }
    }
  }

  class Store(val id: String) extends MemoryDataStore with HasAuthorizationsProvider {
    val identities = new ConcurrentLinkedQueue[String]()
    override val authProvider: AuthorizationsProvider = new AuthorizationsProvider {
      override def getAuthorizations: java.util.List[String] = java.util.Collections.emptyList[String]()
      override def configure(params: java.util.Map[String, _]): Unit = {}
      override def apply(task: Runnable): Runnable = {
        val captured = s"$id:${context.get()}"
        () => withContext(captured)(task.run())
      }
    }
    override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
      val reader = super.getFeatureReader(query, transaction)
      new SimpleFeatureReader {
        override def getFeatureType = reader.getFeatureType
        override def hasNext: Boolean = {
          identities.add(Option(context.get()).getOrElse("none"))
          reader.hasNext
        }
        override def next() = reader.next()
        override def close(): Unit = reader.close()
      }
    }
    var disposals = 0
    override def dispose(): Unit = { disposals += 1; super.dispose() }
  }

  class DelegateFactory extends DataStoreFactorySpi {
    val opened = ArrayBuffer.empty[Store]
    override def canProcess(params: java.util.Map[String, _]): Boolean = params.containsKey(StoreKey)
    override def createDataStore(params: java.util.Map[String, _]): DataStore = {
      if (params.get(StoreKey) == "missing") { return null }
      val store = new Store(params.get(StoreKey).toString)
      opened += store
      Seq("example", "other").foreach { typeName =>
        val schema = SimpleFeatureTypes.createType(typeName, "name:String,dtg:Date,*geom:Point:srid=4326")
        store.createSchema(schema)
        Seq("a", "b", "c", "d").foreach { name =>
          val date = if (name == "d") { "2026-01-03T00:00:00Z" } else { "2026-01-01T00:00:00Z" }
          store.addFeature(ScalaSimpleFeature.create(schema, name, name, Date.from(Instant.parse(date)), "POINT (0 0)"))
        }
      }
      store
    }
    override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)
    override def getDisplayName: String = "Merged filter test store"
    override def getDescription: String = getDisplayName
    override def getParametersInfo: Array[Param] = Array(new Param(StoreKey, classOf[String], "Test store", true))
    override def isAvailable: Boolean = true
    override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
  }
}
