/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.{DataStore, Query, SimpleFeatureStore, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.feature.visitor.CountVisitor
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.process.MinMaxVisitor
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.index.stats.impl.CountStat
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.index.view.filters.RolloverFilterProvider.{CoverageInterval, DestinationProvider, SourceProvider}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.specs2.mutable.SpecificationWithJUnit

import java.time.Instant
import java.util.Date
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class MergedDataStoreViewCoverageTest extends SpecificationWithJUnit with MergedDataStoreViewTestSupport {
  sequential

  // Exercise the real rollover filters with an in-memory lookup; JDBC context sharing is tested separately.
  class Coverage {
    var start = boundary.minusSeconds(3600)
    var published = Map(typeName -> boundary)
    val reads = new AtomicInteger()
    val closes = new AtomicInteger()
    val timestamps = new ConcurrentLinkedQueue[Instant]()
    val contexts = new ConcurrentLinkedQueue[QueryContext]()
    var failed = false

    private def coverage(name: String, context: QueryContext): Option[CoverageInterval] = {
      contexts.add(context)
      val asOf = context.now
      reads.incrementAndGet()
      timestamps.add(asOf)
      if (failed) { throw new java.sql.SQLException("coverage boundary unavailable") }
      published.get(name).map(end => CoverageInterval(name, start, end))
    }

    val source: SourceProvider = new SourceProvider {
      override protected def coverage(sft: SimpleFeatureType, context: QueryContext): Option[CoverageInterval] =
        Coverage.this.coverage(sft.getTypeName, context)
      override def close(): Unit = { closes.incrementAndGet() }
    }
    val destination: DestinationProvider = new DestinationProvider {
      override protected def coverage(sft: SimpleFeatureType, context: QueryContext): Option[CoverageInterval] =
        Coverage.this.coverage(sft.getTypeName, context)
      override def close(): Unit = { closes.incrementAndGet() }
    }
  }

  def coverageView(
      source: MergedSource[DataStore],
      destination: MergedSource[DataStore],
      coverage: Coverage,
      parallel: Boolean = false,
      taskDecorator: Option[java.util.function.UnaryOperator[Runnable]] = None,
      namespace: Option[String] = None): MergedDataStoreView = {
    val stores = Seq(source.copy(provider = coverage.source), destination.copy(provider = coverage.destination)).map { source =>
      taskDecorator.fold(source)(op => source.copy(decorator = new TaskDecorator(Some(op))))
    }
    new MergedDataStoreView(stores, deduplicate = false, parallel = parallel, namespace = namespace)
  }

  def fixture(f: (MergedDataStoreView, Store, Store, Coverage) => Any): Unit = {
    val source = new Store; val destination = new Store; val coverage = new Coverage
    populate(source, "source"); populate(destination, "destination")
    val store = coverageView(
      MergedSource(source), MergedSource(destination), coverage, parallel = true,
      taskDecorator = Some(decorator), namespace = Some("coverage"))
    try { f(store, source, destination, coverage) } finally { store.dispose() }
  }

  "Merged view with destination coverage" should {
    "share one context across delegates and create a fresh context for each operation" in {
      import scala.collection.JavaConverters._
      foreach(Seq(false, true)) { parallel =>
        val coverage = new Coverage
        val source = new ExactStore
        val destination = new ExactStore
        populate(source, "source")
        populate(destination, "destination")
        val view = coverageView(MergedSource(source), MergedSource(destination), coverage, parallel)
        try {
          val schema = view.getSchema(typeName)
          val filter = ECQL.toFilter("dtg >= '2026-01-02T00:00:00Z'")
          val query = new Query(typeName, filter)
          val featureSource = view.getFeatureSource(typeName)
          val operations = Seq[() => Unit](
            () => { val reader = CloseableIterator(view.getFeatureReader(query, Transaction.AUTO_COMMIT))
              try { reader.toList } finally { reader.close() } },
            () => { featureSource.getCount(query) },
            () => { featureSource.getBounds(query) },
            () => { view.stats.getCount(schema, filter, exact = true) },
            () => { view.stats.getMinMax[Date](schema, "dtg", filter, exact = true) },
            () => { view.stats.getStat[CountStat](schema, Stat.Count(), filter, exact = true) })
          var previous: QueryContext = null
          foreach(operations) { run =>
            coverage.contexts.clear()
            run()
            val contexts = coverage.contexts.asScala.toList
            contexts must haveLength(2)
            (contexts.head eq contexts.last) must beTrue
            (contexts.head eq previous) must beFalse
            FastFilterFactory.optimize(schema, contexts.head.base) mustEqual FastFilterFactory.optimize(schema, filter)
            contexts.head.state.isEmpty must beTrue
            contexts.head.state.put("previous-operation", true)
            previous = contexts.head
            success
          }
        } finally { view.dispose() }
      }
    }

    "reject invalid or submillisecond coverage bounds" in {
      CoverageInterval(typeName, boundary, boundary) must throwA[IllegalArgumentException]
      CoverageInterval(typeName, boundary.plusSeconds(1), boundary) must throwA[IllegalArgumentException]
      CoverageInterval(typeName, boundary.minusNanos(1), boundary) must throwA[IllegalArgumentException]
      CoverageInterval(typeName, boundary.minusSeconds(1), boundary.plusNanos(1)) must
        throwA[IllegalArgumentException]
      CoverageInterval(typeName, null, boundary) must throwA[IllegalArgumentException]
    }

    "split overlapping sources without a gap or duplicate at the coverage boundary" in {
      fixture { (store, _, _, coverage) =>
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
        coverage.reads.get() mustEqual 2
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
        coverage.reads.get() mustEqual 2
        coverage.published = Map(typeName -> boundary)
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        read(store).map(_._1) mustEqual Seq("destination", "destination", "source")
      }
      success
    }

    "discover types shared by both stores and use independent type coverage" in {
      fixture { (store, source, destination, coverage) =>
        val other = "summary-sf-other"
        populate(source, "recent-summary", other)
        store.getTypeNames.toSet mustEqual Set(typeName)
        populate(destination, "old-summary", other)
        store.getTypeNames.toSet mustEqual Set(typeName, other)
        read(store, other).map(_._1) mustEqual Seq.fill(3)("recent-summary")
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

    "pass the same query timestamp to both coverage providers" in {
      import scala.collection.JavaConverters._

      fixture { (store, _, _, coverage) =>
        read(store).map(_._1) mustEqual Seq("destination", "source", "source")
        coverage.reads.get() mustEqual 2
        val timestamps = coverage.timestamps.asScala.toList
        timestamps must haveLength(2)
        timestamps.distinct must haveLength(1)
      }
      success
    }

    "route timestamps before, within and after the covered interval in sequential and parallel scans" in {
      foreach(Seq(false, true)) { parallel =>
        val source = new Store; val destination = new Store; val coverage = new Coverage
        populate(source, "source"); populate(destination, "destination")
        coverage.start = boundary
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        val store = coverageView(MergedSource(source), MergedSource(destination), coverage, parallel)
        try {
          read(store).map(_._1) mustEqual Seq("source", "destination", "source")
          readParallel(store) mustEqual 3
        } finally { store.dispose() }
      }
    }

    "close both rollover providers and delegate stores" in {
      val source = new Store; val destination = new Store; val coverage = new Coverage
      val store = coverageView(MergedSource(source), MergedSource(destination), coverage)
      store.dispose()
      coverage.closes.get() mustEqual 2
      source.disposals mustEqual 1
      destination.disposals mustEqual 1
      source.closed must beTrue
      destination.closed must beTrue
    }

    "resolve coverage when a collection is read and refresh cached feature sources" in {
      fixture { (store, _, _, coverage) =>
        coverage.start = boundary
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        val source = store.getFeatureSource(typeName)
        val collection = source.getFeatures()
        coverage.reads.get() mustEqual 0
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        val features = CloseableIterator(collection.features())
        try { features.map(_.getAttribute("origin").toString).toList.sorted mustEqual Seq("destination", "destination", "source") }
        finally { features.close() }
        read(store).map(_._1) mustEqual Seq("source", "destination", "destination")
      }
      success
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
        try { original.length mustEqual 3 } finally { original.close() }
      }
      success
    }

    "retain source records with null timestamps after publication" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      Seq(source -> "source", destination -> "destination").foreach { case (ds, origin) =>
        populate(ds, origin)
        ds.addFeature(ScalaSimpleFeature.create(ds.getSchema(typeName), "null-time", origin, null, "POINT (5 5)"))
      }
      val store = coverageView(MergedSource(source), MergedSource(destination), coverage)
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
      } finally { store.dispose() }
    }

    "refresh coverage for count, bounds, stats and collection visitors and preserve merged stats ownership" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      source.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
      val store = coverageView(MergedSource(source), MergedSource(destination), coverage)
      try {
        val cached = store.getFeatureSource(typeName)
        val query = new Query(typeName)
        cached.getCount(query) mustEqual 1
        coverage.reads.get() mustEqual 2
        cached.getBounds(query).getMaxX mustEqual 0d
        coverage.reads.get() mustEqual 4
        val collection = cached.getFeatures(query)
        coverage.reads.get() mustEqual 4
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        collection.size() mustEqual 3
        val countVisitor = new CountVisitor
        GeoMesaFeatureCollection.visit(collection, countVisitor)
        countVisitor.getCount mustEqual 3
        coverage.reads.get() mustEqual 8
        cached.getCount(query) mustEqual 3
        cached.getBounds(query).getMaxX mustEqual 2d
        coverage.reads.get() mustEqual 12
        store.stats.getCount(store.getSchema(typeName), exact = true) must beSome(3L)
        coverage.reads.get() mustEqual 14
        store.stats.getStat[CountStat](store.getSchema(typeName), Stat.Count(), exact = true).map(_.count) must beSome(3L)
        coverage.reads.get() mustEqual 16
        store.stats.writer.clear(store.getSchema(typeName))
        store.stats.close()
        source.statsWrites mustEqual 1
        destination.statsWrites mustEqual 1
        source.statsCloses mustEqual 1
        destination.statsCloses mustEqual 1
      } finally { store.dispose() }
    }

    "resolve current coverage for sorted subcollections" in {
      fixture { (store, _, _, coverage) =>
        val collection = store.getFeatureSource(typeName).getFeatures()
        coverage.start = boundary
        coverage.published = Map(typeName -> boundary.plusSeconds(1))
        val filtered = collection.subCollection(ECQL.toFilter("origin = 'source'"))
          .sort(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.DESCENDING))
        val reader = CloseableIterator(filtered.features())
        try {
          reader.map(_.getAttribute("dtg").asInstanceOf[Date].toInstant).toList mustEqual
            Seq(boundary.plusSeconds(1), boundary.minusSeconds(1))
        } finally { reader.close() }
        coverage.reads.get() mustEqual 2
      }
      success
    }

    "refresh coverage for min/max process statistics" in {
      val source = new ExactStore; val destination = new ExactStore; val coverage = new Coverage
      populate(source, "source"); populate(destination, "destination")
      source.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
      val store = coverageView(MergedSource(source), MergedSource(destination), coverage)
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
        coverage.reads.get() mustEqual 2
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        minMax(collection) must not(beEqualTo(original))
        coverage.reads.get() mustEqual 4
        minMax(cached.getFeatures()) must not(beEqualTo(original))
        coverage.reads.get() mustEqual 6
      } finally { store.dispose() }
    }

    "preserve the internal GeoMesa reader and encoded stats query paths" in {
      fixture { (store, _, _, coverage) =>
        val api: HasGeoMesaFeatureReader = store
        val schema = store.getSchema(typeName)
        val query = new Query(typeName)
        val reader = api.getFeatureReader(schema, Transaction.AUTO_COMMIT, query)
        coverage.published = Map(typeName -> boundary.plusSeconds(2))
        val features = CloseableIterator(reader.reader())
        try { features.map(_.getAttribute("origin").toString).toList mustEqual Seq.fill(3)("destination") }
        finally { features.close() }
        coverage.reads.get() mustEqual 2
        val countQuery = new Query(typeName)
        countQuery.getHints.put(QueryHints.STATS_STRING, Stat.Count())
        countQuery.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)
        val statsReader = store.getFeatureReader(countQuery, Transaction.AUTO_COMMIT)
        try {
          statsReader.hasNext must beTrue
          StatsScan.decodeStat(schema)(statsReader.next().getAttribute(0).toString).asInstanceOf[CountStat].count mustEqual 3L
          statsReader.hasNext must beFalse
        } finally { statsReader.close() }
        coverage.reads.get() mustEqual 4
      }
      success
    }

  }
}
