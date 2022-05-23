/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> df9a4d047d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7a7c301714 (GEOMESA-3202 Check for disjoint date queries in merged view store)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

<<<<<<< HEAD
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
=======
import org.geotools.data.simple.SimpleFeatureReader
<<<<<<< HEAD
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
>>>>>>> ed0b243ea9f (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.FilterHelper
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
<<<<<<< HEAD
=======
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.specs2.matcher.Matchers
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ed0b243ea9f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> d67c4751dd2 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 24d8c84c5aa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6a4ff24d14c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> aaac9bf1b27 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> d43590761fe (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

<<<<<<< HEAD
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
  import org.locationtech.geomesa.filter.andFilters

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

  "MergedDataStoreView" should {
    "pass through INCLUDE filters" in {
      val stores = this.stores()
<<<<<<< HEAD
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val query = new Query(sft.getTypeName, filter)
        there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
<<<<<<< HEAD
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)

      val noDates = Seq("IN ('1', '2')", "foo = 'bar'", "age = 21", "bbox(geom,120,45,130,55)")
      foreach(noDates.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(filter, f)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - before" in {
      val stores = this.stores()
<<<<<<< HEAD
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)

      val before = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-01T12:00:00.000Z and name = 'alice'")
      foreach(before.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores.take(1)) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
        foreach(stores.drop(1)) { case (store, _) =>
          val query = new Query(sft.getTypeName, Filter.EXCLUDE)
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - after" in {
      val stores = this.stores()
<<<<<<< HEAD
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)

      val after = Seq("dtg during 2022-02-04T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores.take(2)) { case (store, _) =>
          val query = new Query(sft.getTypeName, Filter.EXCLUDE)
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
        foreach(stores.drop(2)) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - overlapping" in {
      val stores = this.stores()
<<<<<<< HEAD
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)

      val after = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }
<<<<<<< HEAD

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
  }
}
