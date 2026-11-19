/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.{FeatureReader, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.memory.MemoryDataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.time.Instant
import java.util.Date

class MergedDataStoreViewDynamicFilterTest extends SpecificationWithJUnit {

  class Store extends MemoryDataStore {
    var queries = Vector.empty[Query]
    override def getFeatureReader(query: Query, transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
      queries :+= new Query(query)
      super.getFeatureReader(query, transaction)
    }
  }

  "Dynamic merged filters" should {
    "combine dynamic store filters with user filters for schemas without a date attribute" in {
      val schema = SimpleFeatureTypes.createType("example", "name:String,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      Seq("a", "b", "c", "d").foreach { name =>
        store.addFeature(ScalaSimpleFeature.create(schema, name, name, "POINT (0 0)"))
      }
      var calls = 0
      var current = "name <> 'b'"
      val filter = new MergedFilterProvider() {
        override def init(config: Map[String, String]): Unit = {}
        override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
          calls += 1
          Some(ff.and(ECQL.toFilter("name <> 'c'"), ECQL.toFilter(current)))
        }
        override def close(): Unit = {}
      }
      val view = new MergedDataStoreView(Seq(MergedSource(store, filter)), deduplicate = false, parallel = false)
      try {
        val source = view.getFeatureSource(schema.getTypeName)
        val collection = source.getFeatures(ECQL.toFilter("name <> 'd'"))
        calls mustEqual 0
        current = "name <> 'a'"
        val original = CloseableIterator(collection.features())
        try { original.map(_.getAttribute("name")).toList mustEqual List("b") } finally { original.close() }
        calls mustEqual 1
        val refreshed = CloseableIterator(source.getFeatures(ECQL.toFilter("name <> 'd'")).features())
        try { refreshed.map(_.getAttribute("name")).toList mustEqual List("b") } finally { refreshed.close() }
        calls mustEqual 2
      } finally {
        view.dispose()
      }
    }

    "calculate relative dates using each read operation timestamp" in {
      val schema = SimpleFeatureTypes.createType("example", "dtg:Date,*geom:Point:srid=4326")
      val store = new Store
      store.createSchema(schema)
      var times = Vector.empty[Instant]
      val filter = new MergedFilterProvider() {
        override def init(config: Map[String, String]): Unit = {}
        override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
          times :+= context.now
          None
        }
        override def close(): Unit = {}
      }
      val view = new MergedDataStoreView(
        Seq(MergedSource(store, MergedFilterProvider.and(Seq(filter, MergedFilterProvider.from("PT0H"))))),
        deduplicate = false, parallel = false)
      try {
        val source = view.getFeatureSource(schema.getTypeName)
        val collection = source.getFeatures()
        times must beEmpty
        Seq(collection, collection.subCollection(Filter.INCLUDE)).foreach { features =>
          val reader = CloseableIterator(features.features())
          try { reader.length mustEqual 0 } finally { reader.close() }
        }
        times.size mustEqual 2
        store.queries.map(_.getFilter) mustEqual times.map { time =>
          ff.greaterOrEqual(ff.property("dtg"), ff.literal(Date.from(time)))
        }
        source.getFeatures()
        times.size mustEqual 2
      } finally {
        view.dispose()
      }
    }
  }
}
