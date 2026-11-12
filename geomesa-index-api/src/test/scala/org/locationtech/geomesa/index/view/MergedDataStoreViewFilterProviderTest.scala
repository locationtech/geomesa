/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.{DataStore, FeatureReader, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.memory.MemoryDataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.index.view.MergedDataStoreView.{DateFilter, FilterProvider, MergedSource}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.TypeOption
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.time.{Duration, Instant}
import java.util.Date

class MergedDataStoreViewFilterProviderTest extends SpecificationWithJUnit {

  class Store extends MemoryDataStore {
    var queries = Vector.empty[Query]
    override def getFeatureReader(query: Query, transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
      queries :+= new Query(query)
      super.getFeatureReader(query, transaction)
    }
  }

  "Merged filter providers" should {
    "combine provider, store and user filters for schemas without a date attribute" in {
      val schema = SimpleFeatureTypes.createType("example", "name:String,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      Seq("a", "b", "c").foreach { name =>
        store.addFeature(ScalaSimpleFeature.create(schema, name, name, "POINT (0 0)"))
      }
      var calls = 0
      var current = "name <> 'b'"
      val provider = new FilterProvider {
        override def filters(name: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] = {
          calls += 1
          Seq(ECQL.toFilter(current))
        }
      }
      val view = new MergedDataStoreView(
        Seq(MergedSource(store, sourceFilter = TypeOption(ECQL.toFilter("name <> 'c'")))),
        deduplicate = false, parallel = false, filterProvider = Some(provider))
      try {
        val source = view.getFeatureSource(schema.getTypeName)
        val collection = source.getFeatures(ECQL.toFilter("name <> 'z'"))
        calls mustEqual 1
        current = "name <> 'a'"
        val original = CloseableIterator(collection.features())
        try { original.map(_.getAttribute("name")).toList mustEqual List("a") } finally { original.close() }
        calls mustEqual 1
        val refreshed = CloseableIterator(source.getFeatures().features())
        try { refreshed.map(_.getAttribute("name")).toList mustEqual List("b") } finally { refreshed.close() }
        calls mustEqual 2
      } finally { view.dispose() }
    }

    "freeze configured relative dates using the provider's operation time" in {
      val schema = SimpleFeatureTypes.createType("example", "dtg:Date,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      var times = Vector.empty[Instant]
      val provider = new FilterProvider {
        override def filters(name: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] = {
          times :+= now
          Seq(Filter.INCLUDE)
        }
      }
      val view = new MergedDataStoreView(
        Seq(MergedSource(store, from = TypeOption[DateFilter](DateFilter.RelativeDate(Duration.ZERO)))),
        deduplicate = false, parallel = false, filterProvider = Some(provider))
      try {
        val source = view.getFeatureSource(schema.getTypeName)
        val collection = source.getFeatures()
        times.size mustEqual 1
        val expected = ff.greaterOrEqual(ff.property("dtg"), ff.literal(Date.from(times.head)))
        Seq(collection, collection.subCollection(Filter.INCLUDE)).foreach { features =>
          val reader = CloseableIterator(features.features())
          try { reader.length mustEqual 0 } finally { reader.close() }
        }
        store.queries.map(_.getFilter) mustEqual Vector(expected, expected)
        times.size mustEqual 1
        source.getFeatures()
        times.size mustEqual 2
      } finally { view.dispose() }
    }

    "retain captured delegate selection when type predicates change" in {
      val schema = SimpleFeatureTypes.createType("example", "name:String,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      store.addFeature(ScalaSimpleFeature.create(schema, "a", "a", "POINT (0 0)"))
      var accepted = true
      val provider = new FilterProvider {
        override def filters(name: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] =
          stores.map(_ => Filter.INCLUDE)
      }
      val view = new MergedDataStoreView(Seq(MergedSource(store, acceptsType = _ => accepted)),
        deduplicate = false, parallel = false, filterProvider = Some(provider))
      try {
        val source = view.getFeatureSource(schema.getTypeName)
        val collection = source.getFeatures()
        accepted = false
        val reader = CloseableIterator(collection.features())
        try { reader.map(_.getAttribute("name")).toList mustEqual List("a") } finally { reader.close() }
        view.getTypeNames must beEmpty
        source.getFeatures() must throwA[IllegalArgumentException]
      } finally { view.dispose() }
    }

    "validate provider output before opening delegate readers" in {
      val schema = SimpleFeatureTypes.createType("example", "name:String,*geom:Point:srid=4326")
      Seq(Seq.empty[Filter], Seq(null: Filter)).foreach { resolved =>
        val store = new Store
        store.createSchema(schema)
        val provider = new FilterProvider {
          override def filters(name: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] = resolved
        }
        val view = new MergedDataStoreView(Seq(MergedSource(store)), deduplicate = false,
          parallel = false, filterProvider = Some(provider))
        try {
          view.getFeatureReader(new Query(schema.getTypeName), Transaction.AUTO_COMMIT) must
            throwA[IllegalArgumentException]
          store.queries must beEmpty
        } finally { view.dispose() }
      }
      success
    }

    "enforce type predicates without a dynamic provider" in {
      val schema = SimpleFeatureTypes.createType("example", "name:String,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      val view = new MergedDataStoreView(Seq(MergedSource(store, acceptsType = _ => false)),
        deduplicate = false, parallel = false)
      try {
        view.getTypeNames must beEmpty
        view.getSchema(schema.getTypeName) must beNull
        view.getFeatureSource(schema.getTypeName) must throwA[IllegalArgumentException]
        view.getFeatureReader(schema, Transaction.AUTO_COMMIT, new Query(schema.getTypeName)) must
          throwA[IllegalArgumentException]
        store.queries must beEmpty
      } finally { view.dispose() }
    }

    "retain schema intersection when a provider does not override delegate selection" in {
      val first = new Store
      val second = new Store
      val common = SimpleFeatureTypes.createType("common", "name:String,*geom:Point:srid=4326")
      val other = SimpleFeatureTypes.createType("other", "name:String,*geom:Point:srid=4326")
      first.createSchema(common)
      first.createSchema(other)
      second.createSchema(common)
      var calls = 0
      val provider = new FilterProvider {
        override def filters(name: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] = {
          calls += 1
          stores.map(_ => Filter.INCLUDE)
        }
      }
      val view = new MergedDataStoreView(Seq(MergedSource(first), MergedSource(second)),
        deduplicate = false, parallel = false, filterProvider = Some(provider))
      try {
        view.getTypeNames.toSeq mustEqual Seq("common")
        view.getSchema("other") must beNull
        view.getSchema("common") must not(beNull)
        calls mustEqual 0
      } finally { view.dispose() }
    }
  }
}
