/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.{DataStore, Query, SimpleFeatureReader, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.sort.SortOrder
import org.geotools.data.memory.MemoryDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit

import java.io.IOException
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.UnaryOperator

class MergedQueryRunnerTest extends SpecificationWithJUnit with Mockito {

  private val sft = SimpleFeatureTypes.createType("query-runner", "name:String,dtg:Date,*geom:Point:srid=4326")

  private def query(sorted: Boolean): Query = {
    val q = new Query(sft.getTypeName)
    if (sorted) { q.setSortBy(org.locationtech.geomesa.filter.ff.sort("name", SortOrder.ASCENDING)) }
    q
  }

  private def mockStore(): DataStore = {
    val store = mock[DataStore]
    store.getSchema(sft.getTypeName) returns sft
    store
  }

  class TrackingReader extends SimpleFeatureReader {
    @volatile var closed = false
    override def getFeatureType = sft
    override def hasNext: Boolean = false
    override def next(): SimpleFeature = Iterator.empty.next()
    override def close(): Unit = { closed = true }
  }

  "MergedQueryRunner" should {
    "deduplicate overlapping ids before limiting sequential, parallel and sorted results" in {
      foreach(Seq(false, true)) { parallel =>
        foreach(Seq(false, true)) { sorted =>
          foreach(Seq(false, true)) { deduplicate =>
            foreach(Seq(false, true)) { limited =>
              val stores = Seq(Seq("a", "b", "c"), Seq("a", "b", "d")).map { ids =>
                val store = new MemoryDataStore
                store.createSchema(sft)
                ids.foreach(id => store.addFeature(ScalaSimpleFeature.create(sft, id, id,
                  "2026-01-01T00:00:00Z", "POINT (0 0)")))
                MergedSource[DataStore](store)
              }
              val view = new MergedDataStoreView(stores, deduplicate, parallel)
              try {
                val q = query(sorted)
                if (limited) { q.setMaxFeatures(3) }
                val ids = WithClose(view.getFeatureReader(q, Transaction.AUTO_COMMIT)) { reader =>
                  CloseableIterator(reader).map(_.getID).toList
                }
                val expectedSize = if (limited) { 3 } else if (deduplicate) { 4 } else { 6 }
                ids must haveLength(expectedSize)
                if (deduplicate) { ids.distinct must haveLength(expectedSize) }
                if (sorted) {
                  val all = if (deduplicate) { List("a", "b", "c", "d") } else { List("a", "a", "b", "b", "c", "d") }
                  ids mustEqual (if (limited) { all.take(3) } else { all })
                } else if (!limited) {
                  ids.sorted mustEqual (if (deduplicate) { List("a", "b", "c", "d") } else { List("a", "a", "b", "b", "c", "d") })
                }
                success
              } finally { view.dispose() }
            }
          }
        }
      }
    }

    "close already opened readers if a subsequent delegate fails to open" in {
      foreach(Seq((false, false), (false, true), (true, false))) { case (parallel, sorted) =>
        val first = mockStore(); val second = mockStore()
        val reader = new TrackingReader
        first.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns reader
        second.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) throws new IOException("open failed")
        val view = new MergedDataStoreView(Seq(MergedSource(first), MergedSource(second)), false, parallel)
        try {
          WithClose(view.getFeatureReader(query(sorted), Transaction.AUTO_COMMIT)) { r =>
            while (r.hasNext) { r.next() }
          } must
            throwA[IOException]("open failed")
          reader.closed must beTrue
        } finally { view.dispose() }
      }
    }

    "propagate iteration errors and close readers" in {
      foreach(Seq((false, false), (false, true), (true, false))) { case (parallel, sorted) =>
        val first = mockStore(); val second = mockStore()
        val reader = new TrackingReader {
          override def hasNext: Boolean = throw new IOException("iteration failed")
        }
        val other = new TrackingReader
        first.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns reader
        second.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns other
        val view = new MergedDataStoreView(Seq(MergedSource(first), MergedSource(second)), false, parallel)
        try {
          WithClose(view.getFeatureReader(query(sorted), Transaction.AUTO_COMMIT)) { r =>
            while (r.hasNext) { r.next() }
          } must throwA[IOException]("iteration failed")
          reader.closed must beTrue
          if (parallel || sorted) { other.closed must beTrue }
          success
        } finally { view.dispose() }
      }
    }

    "cancel active parallel producers and close their readers on early closure" in {
      val started = new CountDownLatch(2)
      val stopped = new CountDownLatch(2)
      val decorator = new TaskDecorator(Some(new UnaryOperator[Runnable] {
        override def apply(task: Runnable): Runnable = () => {
          started.countDown()
          try { task.run() } finally { stopped.countDown() }
        }
      }))
      val readers = Seq.fill(2)(new TrackingReader {
        override def hasNext: Boolean = true
        override def next(): SimpleFeature =
          ScalaSimpleFeature.create(sft, "id", "name", "2026-01-01T00:00:00Z", "POINT (0 0)")
      })
      val stores = readers.map { reader =>
        val store = mockStore()
        store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns reader
        MergedSource(store, decorator = decorator)
      }
      val view = new MergedDataStoreView(stores, false, true)
      try {
        WithClose(view.getFeatureReader(query(sorted = false), Transaction.AUTO_COMMIT)) { reader =>
          reader.hasNext must beTrue
          started.await(5, TimeUnit.SECONDS) must beTrue
          reader.next()
        }
        stopped.await(5, TimeUnit.SECONDS) must beTrue
        foreach(readers)(_.closed must beTrue)
      } finally { view.dispose() }
    }
  }
}
