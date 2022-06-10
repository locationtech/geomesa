/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.matcher.Matchers
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

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
      val view = new MergedDataStoreView(stores, deduplicate = false)
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val query = new Query(sft.getTypeName, filter)
        there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false)

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
      val view = new MergedDataStoreView(stores, deduplicate = false)

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
      val view = new MergedDataStoreView(stores, deduplicate = false)

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
      val view = new MergedDataStoreView(stores, deduplicate = false)

      val after = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }
  }
}
