/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.locationtech.geomesa.index.view.RolloverDataStoreView.Delegate
import org.geotools.api.data.{Query, SimpleFeatureReader, SimpleFeatureSource, SimpleFeatureStore, Transaction}
import org.geotools.api.feature.Feature
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.memory.MemoryFeatureStore
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.feature.visitor.{CalcResult, CountVisitor}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.process.{GeoMesaProcessVisitor, MinMaxVisitor}
import org.locationtech.geomesa.index.stats.{HasGeoMesaStats, NoopStatWriter, Stat}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.index.stats.impl.CountStat
import org.locationtech.jts.geom.Geometry
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.data.memory.MemoryDataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.time.Instant
import java.util.Date
import java.util.concurrent.ConcurrentLinkedQueue

class RolloverDataStoreViewTest extends SpecificationWithJUnit {
  sequential

  private val context = new ThreadLocal[String]()
  private val decorator = new java.util.function.UnaryOperator[Runnable] {
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

  private val boundary = Instant.parse("2026-01-02T00:00:00Z")
  private val typeName = "track-sf-example"

  class Coverage extends RolloverCoverage {
    var start = boundary.minusSeconds(3600)
    var published = Map(typeName -> boundary)
    var reads = 0
    var closed = false
    var closes = 0
    var failed = false
    override def coverage(name: String): Option[CoverageInterval] = {
      reads += 1
      if (failed) { throw new java.sql.SQLException("coverage boundary unavailable") }
      published.get(name).map(end => CoverageInterval(start, end))
    }
    override def close(): Unit = { closed = true; closes += 1 }
  }

  class Store extends MemoryDataStore {
    var closed = false
    val identities = new ConcurrentLinkedQueue[String]()
    override def dispose(): Unit = { closed = true; super.dispose() }
    override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
      val reader = super.getFeatureReader(query, transaction)
      new SimpleFeatureReader {
        override def getFeatureType = reader.getFeatureType
        override def hasNext: Boolean = {
          val identity = Option(context.get()).getOrElse("none")
          identities.add(identity)
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
          val reader = CloseableIterator(getReader(query))
          try { reader.length } finally { reader.close() }
        }
        override protected def getBoundsInternal(query: Query): ReferencedEnvelope = {
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

  def fixture(f: (RolloverDataStoreView, Store, Store, Coverage) => Any): Unit = {
    val source = new Store; val destination = new Store; val coverage = new Coverage
    populate(source, "source"); populate(destination, "destination")
    val store = new RolloverDataStoreView(
      Seq(Delegate(source)), Delegate(destination), coverage, parallel = true,
      taskDecorator = Some(decorator), namespace = Some("rollover"))
    try { f(store, source, destination, coverage) } finally { store.dispose(); source.dispose(); destination.dispose() }
  }

  def read(store: RolloverDataStoreView, name: String = typeName): Seq[(String, Long)] = {
    val query = new Query(name, Filter.INCLUDE)
    query.setSortBy(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.ASCENDING))
    val reader = CloseableIterator(store.getFeatureReader(query, Transaction.AUTO_COMMIT))
    try { reader.map(f => f.getAttribute("origin").toString -> f.getAttribute("dtg").asInstanceOf[Date].getTime).toList }
    finally { reader.close() }
  }

  "Rollover view" should {
    "split overlapping sources without a gap or duplicate at the coverage boundary" in {
      fixture { (store, _, _, coverage) =>
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
        coverage.reads mustEqual 1
        val query = new Query(typeName, ECQL.toFilter("dtg >= '2026-01-02T00:00:00Z'"))
        query.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)
        val features = CloseableIterator(store.getFeatureSource(typeName).getFeatures(query).features())
        try { features.size mustEqual 2 } finally { features.close() }
      }
      success
    }

    "keep all source rows until coverage is certified and refresh subsequent queries" in {
      fixture { (store, _, _, coverage) =>
        coverage.published = Map.empty
        read(store).map(_._1) mustEqual Seq("source", "source", "source")
        coverage.published = Map(typeName -> boundary)
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        read(store).map(_._1) mustEqual Seq("destination", "destination", "source")
      }
      success
    }

    "discover new types, include source-only types and use independent type coverage" in {
      fixture { (store, source, destination, coverage) =>
        val other = "summary-sf-other"
        populate(source, "recent-summary", other)
        store.getTypeNames.toSet mustEqual Set(typeName, other)
        read(store, other).map(_._1) mustEqual Seq.fill(3)("recent-summary")
        populate(destination, "old-summary", other)
        coverage.published += other -> boundary.plusSeconds(1)
        read(store, other).map(_._1) mustEqual Seq("old-summary", "old-summary", "recent-summary")
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
      }
      success
    }

    "fail instead of hiding destination data when the coverage boundary cannot be read" in {
      fixture { (store, _, _, coverage) =>
        coverage.failed = true
        read(store) must throwA[java.sql.SQLException]
      }
      success
    }

    "propagate injected context to both readers and leave physical pools owned by their original stores" in {
      import scala.collection.JavaConverters._
      fixture { (store, source, destination, coverage) =>
        context.set("example-context")
        try { read(store).size mustEqual 3 } finally { context.remove() }
        source.identities.asScala.toSet mustEqual Set("example-context")
        destination.identities.asScala.toSet mustEqual Set("example-context")
        store.dispose()
        store.dispose()
        coverage.closed must beTrue
        coverage.closes mustEqual 1
        source.closed must beFalse
        destination.closed must beFalse
      }
      success
    }

    "refresh cached sources while freezing each returned collection" in {
      fixture { (store, _, _, coverage) =>
        coverage.start = boundary
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        val source = store.getFeatureSource(typeName)
        val collection = source.getFeatures()
        read(store).map(_._1) mustEqual Seq("source", "destination", "source")
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        val features = CloseableIterator(collection.features())
        try { features.map(_.getAttribute("origin").toString).toList.sorted mustEqual Seq("destination", "source", "source") }
        finally { features.close() }
        read(store).map(_._1) mustEqual Seq("source", "destination", "destination")
        val refreshed = CloseableIterator(source.getFeatures().features())
        try { refreshed.map(_.getAttribute("origin").toString).toList.sorted mustEqual Seq("destination", "destination", "source") }
        finally { refreshed.close() }
      }
      success
    }

    "require coverage for destination-only types and apply both interval bounds" in {
      fixture { (store, _, destination, coverage) =>
        val other = "destination-only"
        populate(destination, "destination", other)
        read(store, other) must throwA[IllegalArgumentException]
        coverage.start = boundary
        coverage.published += other -> boundary.plusSeconds(1)
        read(store, other).size mustEqual 1
      }
      success
    }

    "reject an ambiguous source placement instead of merging unrelated copies" in {
      val first = new Store; val second = new Store; val destination = new Store
      populate(first, "one"); populate(second, "two"); populate(destination, "destination")
      val coverage = new Coverage
      val store = new RolloverDataStoreView(
        Seq(Delegate(first), Delegate(second)), Delegate(destination), coverage)
      try { store.getSchema(typeName) must throwA[IllegalArgumentException] }
      finally { store.dispose(); first.dispose(); second.dispose(); destination.dispose() }
    }
    "apply delegate predicates to discovery, schema lookup and query routing" in {
      val source = new Store; val destination = new Store; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      populate(source, "hidden", "excluded")
      val store = new RolloverDataStoreView(Seq(Delegate(source, _ == typeName)),
        Delegate(destination, _ == typeName), coverage, namespace = Some("example"))
      try {
        store.getTypeNames.toSeq mustEqual Seq(typeName)
        store.getNames.get(0).getNamespaceURI mustEqual "example"
        store.getSchema("excluded") must beNull
        read(store, "excluded") must throwA[IllegalArgumentException]
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "reject invalid coverage intervals and aliased source/destination stores" in {
      CoverageInterval(boundary, boundary) must throwA[IllegalArgumentException]
      CoverageInterval(boundary, boundary.plusNanos(1)) must throwA[IllegalArgumentException]
      val source = new Store
      try {
        new RolloverDataStoreView(Seq(Delegate(source)), Delegate(source), new Coverage) must
          throwA[IllegalArgumentException]
      } finally { source.dispose() }
    }

    "keep ownership and type selection through feature sources and process visitors" in {
      val source = new Store; val destination = new Store; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      populate(source, "hidden", "hidden"); populate(destination, "hidden", "hidden")
      val store = new RolloverDataStoreView(Seq(Delegate(source, _ == typeName)),
        Delegate(destination, _ == typeName), coverage)
      try {
        val fs = store.getFeatureSource(typeName)
        fs.getDataStore must beTheSameAs(store)
        fs.getDataStore.getNames.size() mustEqual 1
        fs.getFeatures(new Query("hidden")) must throwA[IllegalArgumentException]
        fs.getCount(new Query("hidden")) must throwA[IllegalArgumentException]
        fs.getBounds(new Query("hidden")) must throwA[IllegalArgumentException]
        val input = new Query(typeName)
        val features = fs.getFeatures(input)
        input.setTypeName("hidden")
        input.setFilter(Filter.EXCLUDE)
        var visitorSource: SimpleFeatureSource = null
        val visitor = new GeoMesaProcessVisitor {
          override def execute(s: SimpleFeatureSource, q: Query): Unit = {
            visitorSource = s
            q.setTypeName("hidden") // mutation must not alter the collection's own query
          }
          override def visit(feature: Feature): Unit = {}
          override def getResult: CalcResult = null
        }
        GeoMesaFeatureCollection.visit(features, visitor)
        visitorSource.getDataStore must beTheSameAs(store)
        visitorSource.getFeatures(new Query("hidden")) must throwA[IllegalArgumentException]
        val reader = features.features()
        try { CloseableIterator(reader).length mustEqual 3 } finally { reader.close() }
        visitorSource.getDataStore.dispose()
        source.closed must beFalse
        destination.closed must beFalse
        coverage.closes mustEqual 1
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "refresh a cached feature source after publication and source retention" in {
      fixture { (store, source, _, coverage) =>
        coverage.published = Map.empty
        val cached = store.getFeatureSource(typeName)
        val before = cached.getFeatures()
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        source.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
        val current = CloseableIterator(cached.getFeatures().features())
        try { current.length mustEqual 3 } finally { current.close() }
        val original = CloseableIterator(before.features())
        try { original.length mustEqual 0 } finally { original.close() }
      }
      success
    }

    "retain source records with null timestamps after publication" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      Seq(source -> "source", destination -> "destination").foreach { case (ds, origin) =>
        populate(ds, origin)
        ds.addFeature(ScalaSimpleFeature.create(ds.getSchema(typeName), "null-time", origin, null, "POINT (5 5)"))
      }
      val store = new RolloverDataStoreView(Seq(Delegate(source)), Delegate(destination), coverage)
      try {
        val cached = store.getFeatureSource(typeName)
        coverage.published = Map.empty
        cached.getCount(new Query(typeName)) mustEqual 4
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        val query = new Query(typeName, ECQL.toFilter("dtg IS NULL"))
        val rows = CloseableIterator(cached.getFeatures(query).features())
        try { rows.map(_.getAttribute("origin").toString).toList mustEqual List("source") }
        finally { rows.close() }
        cached.getCount(new Query(typeName)) mustEqual 4
        cached.getCount(query) mustEqual 1
        store.stats.getCount(store.getSchema(typeName), query.getFilter, exact = true) must beSome(1L)
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "resolve one plan for count, bounds, stats and collection visitors without owning delegate stats" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      source.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
      val store = new RolloverDataStoreView(Seq(Delegate(source)), Delegate(destination), coverage)
      try {
        val cached = store.getFeatureSource(typeName)
        val query = new Query(typeName)
        cached.getCount(query) mustEqual 1
        coverage.reads mustEqual 1
        cached.getBounds(query).getMaxX mustEqual 0d
        coverage.reads mustEqual 2
        val collection = cached.getFeatures(query)
        coverage.reads mustEqual 3
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        collection.size() mustEqual 1
        val countVisitor = new CountVisitor
        GeoMesaFeatureCollection.visit(collection, countVisitor)
        countVisitor.getCount mustEqual 1
        coverage.reads mustEqual 3
        cached.getCount(query) mustEqual 3
        cached.getBounds(query).getMaxX mustEqual 2d
        coverage.reads mustEqual 5
        store.stats.getCount(store.getSchema(typeName), exact = true) must beSome(3L)
        coverage.reads mustEqual 6
        store.stats.getStat[CountStat](store.getSchema(typeName), Stat.Count(), exact = true).map(_.count) must beSome(3L)
        coverage.reads mustEqual 7
        store.stats.writer.clear(store.getSchema(typeName))
        store.stats.close()
        source.statsWrites mustEqual 0
        destination.statsWrites mustEqual 0
        source.statsCloses mustEqual 0
        destination.statsCloses mustEqual 0
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "retain the operation interval through sorting and subcollections" in {
      fixture { (store, _, _, coverage) =>
        val collection = store.getFeatureSource(typeName).getFeatures()
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        val filtered = collection.subCollection(ECQL.toFilter("origin = 'source'"))
          .sort(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.DESCENDING))
        val reader = CloseableIterator(filtered.features())
        try { reader.map(_.getAttribute("dtg").asInstanceOf[Date].toInstant).toList mustEqual Seq(boundary.plusSeconds(1), boundary) }
        finally { reader.close() }
        coverage.reads mustEqual 1
      }
      success
    }

    "keep min/max process statistics on the collection's captured interval" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      source.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
      val store = new RolloverDataStoreView(Seq(Delegate(source)), Delegate(destination), coverage)
      try {
        val cached = store.getFeatureSource(typeName)
        val collection = cached.getFeatures()
        def minMax(features: org.geotools.data.simple.SimpleFeatureCollection): String = {
          val visitor = new MinMaxVisitor(features, "dtg", cached = false)
          GeoMesaFeatureCollection.visit(features, visitor)
          val result = visitor.getResult.results.features()
          try { result.next().getAttribute(0).toString } finally { result.close() }
        }
        val original = minMax(collection)
        coverage.reads mustEqual 1
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        minMax(collection) mustEqual original
        coverage.reads mustEqual 1
        minMax(cached.getFeatures()) must not(beEqualTo(original))
        coverage.reads mustEqual 2
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "preserve min/max visitor fallbacks for ordinary feature sources" in {
      Seq(new Store, new ExactStore).foreach { store =>
        try {
          populate(store, "source")
          val source = store.getFeatureSource(typeName)
          val features = source.getFeatures()
          val expected = Stat(source.getSchema, Stat.MinMax("dtg"))
          CloseableIterator(features.features()).foreach(expected.observe)
          val visitor = new MinMaxVisitor(features, "dtg", cached = false)
          visitor.execute(source, new Query(typeName))
          val result = visitor.getResult.results.features()
          try { result.next().getAttribute(0).toString mustEqual expected.toJson }
          finally { result.close() }
        } finally { store.dispose() }
      }
      success
    }

    "reject different default temporal attributes even when attribute definitions match" in {
      val source = new Store; val destination = new Store; val coverage = new Coverage
      Seq(source -> "eventTime", destination -> "arrivalTime").foreach { case (ds, dtg) =>
        val schema = SimpleFeatureTypes.createType(typeName, "eventTime:Date,arrivalTime:Date,*geom:Point:srid=4326")
        schema.getUserData.put("geomesa.index.dtg", dtg)
        ds.createSchema(schema)
      }
      val store = new RolloverDataStoreView(Seq(Delegate(source)), Delegate(destination), coverage)
      try {
        store.getSchema(typeName) must throwA[IllegalArgumentException](".*different temporal attributes.*")
        read(store) must throwA[IllegalArgumentException](".*different temporal attributes.*")
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "fail when published coverage has no advertised destination and propagate provider failures" in {
      val source = new Store; val destination = new Store; val coverage = new Coverage
      populate(source, "source")
      val store = new RolloverDataStoreView(Seq(Delegate(source)), Delegate(destination), coverage)
      try {
        read(store) must throwA[IllegalArgumentException](".*no available destination schema.*")
        coverage.reads mustEqual 1
        coverage.failed = true
        read(store) must throwA[java.sql.SQLException]
        coverage.failed = false
        coverage.published = Map.empty
        read(store).size mustEqual 3
      } finally { store.dispose(); source.dispose(); destination.dispose() }
    }

    "preserve the internal GeoMesa reader and encoded stats query paths" in {
      fixture { (store, _, _, coverage) =>
        val api: HasGeoMesaFeatureReader = store
        val schema = store.getSchema(typeName)
        val query = new Query(typeName)
        val reader = api.getFeatureReader(schema, Transaction.AUTO_COMMIT, query)
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        query.setTypeName("hidden")
        query.setFilter(Filter.EXCLUDE)
        val features = CloseableIterator(reader.reader())
        try { features.map(_.getAttribute("origin").toString).toList.sorted mustEqual Seq("destination", "source", "source") }
        finally { features.close() }
        coverage.reads mustEqual 1
        val countQuery = new Query(typeName)
        countQuery.getHints.put(QueryHints.STATS_STRING, Stat.Count())
        countQuery.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)
        val statsReader = store.getFeatureReader(countQuery, Transaction.AUTO_COMMIT)
        try {
          statsReader.hasNext must beTrue
          StatsScan.decodeStat(schema)(statsReader.next().getAttribute(0).toString).asInstanceOf[CountStat].count mustEqual 3L
          statsReader.hasNext must beFalse
        } finally { statsReader.close() }
        coverage.reads mustEqual 2
      }
      success
    }

  }
}
