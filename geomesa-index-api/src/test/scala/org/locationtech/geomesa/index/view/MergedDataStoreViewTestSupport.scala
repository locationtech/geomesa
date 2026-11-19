/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.{Query, SimpleFeatureReader, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.data.memory.{MemoryDataStore, MemoryFeatureStore}
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{HasGeoMesaStats, NoopStatWriter, Stat}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Geometry

import java.time.Instant
import java.util.Date
import java.util.concurrent.ConcurrentLinkedQueue

trait MergedDataStoreViewTestSupport {

  protected val context = new ThreadLocal[String]()
  protected val decorator = new java.util.function.UnaryOperator[Runnable] {
    override def apply(task: Runnable): Runnable = {
      val captured = context.get()
      new Runnable {
        override def run(): Unit = {
          val previous = context.get()
          context.set(captured)
          try { task.run() } finally {
            if (previous == null) { context.remove() } else { context.set(previous) }
          }
        }
      }
    }
  }

  protected val boundary = Instant.parse("2026-01-02T00:00:00Z")
  protected val typeName = "track-sf-example"

  class Store extends MemoryDataStore {
    var closed = false
    var disposals = 0
    val identities = new ConcurrentLinkedQueue[String]()
    protected def recordIdentity(): Unit = identities.add(Option(context.get()).getOrElse("none"))
    override def dispose(): Unit = { closed = true; disposals += 1; super.dispose() }
    override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
      val reader = super.getFeatureReader(query, transaction)
      new SimpleFeatureReader {
        override def getFeatureType = reader.getFeatureType
        override def hasNext: Boolean = {
          recordIdentity()
          reader.hasNext
        }
        override def next() = reader.next()
        override def close(): Unit = reader.close()
      }
    }
  }

  class ExactStore extends Store with HasGeoMesaStats {
    var statsCloses = 0
    var statsWrites = 0
    override val stats = new UnoptimizedRunnableStats(this) {
      override val writer: GeoMesaStatWriter = new GeoMesaStatWriter {
        override def analyze(sft: SimpleFeatureType): Seq[Stat] = { statsWrites += 1; Seq.empty }
        override def updater(sft: SimpleFeatureType) = NoopStatWriter.updater(sft)
        override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = { statsWrites += 1 }
        override def clear(sft: SimpleFeatureType): Unit = { statsWrites += 1 }
      }
      override def close(): Unit = { statsCloses += 1 }
    }
    override protected def createFeatureSource(entry: ContentEntry, query: Query): ContentFeatureSource =
      new MemoryFeatureStore(entry, query) {
        override protected def getCountInternal(query: Query): Int = {
          recordIdentity()
          val reader = CloseableIterator(getReader(query))
          try { reader.length } finally { reader.close() }
        }
        override protected def getBoundsInternal(query: Query): ReferencedEnvelope = {
          recordIdentity()
          val bounds = new ReferencedEnvelope(getSchema.getCoordinateReferenceSystem)
          val reader = CloseableIterator(getReader(query))
          try { reader.foreach(f => bounds.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)) }
          finally { reader.close() }
          bounds
        }
      }
  }

  def populate(store: Store, origin: String, name: String = typeName): Unit = {
    val schema = SimpleFeatureTypes.createType(name, "origin:String,dtg:Date,*geom:Point:srid=4326")
    store.createSchema(schema)
    Seq(-1L, 0L, 1L).zipWithIndex.foreach { case (seconds, i) =>
      store.addFeature(ScalaSimpleFeature.create(schema, i.toString, origin,
        Date.from(boundary.plusSeconds(seconds)), s"POINT ($i $i)"))
    }
  }

  def read(store: MergedDataStoreView, name: String = typeName): Seq[(String, Long)] = {
    val query = new Query(name, Filter.INCLUDE)
    query.setSortBy(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.ASCENDING))
    val reader = CloseableIterator(store.getFeatureReader(query, Transaction.AUTO_COMMIT))
    try { reader.map(f => f.getAttribute("origin").toString -> f.getAttribute("dtg").asInstanceOf[Date].getTime).toList }
    finally { reader.close() }
  }

  def readParallel(store: MergedDataStoreView): Int = {
    // Sorted merging reads on the calling thread; an unsorted query exercises parallel delegate scans.
    val reader = CloseableIterator(store.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT))
    try { reader.length } finally { reader.close() }
  }

}
