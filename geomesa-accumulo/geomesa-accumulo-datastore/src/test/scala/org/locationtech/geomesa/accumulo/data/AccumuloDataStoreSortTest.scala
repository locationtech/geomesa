/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter.sort.{SortBy, SortOrder}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreSortTest extends Specification with TestWithFeatureType {

  import org.locationtech.geomesa.filter.ff

  sequential

  override val spec = "name:String:index=join,age:Int:index=full,weight:Double,dtg:Date,*geom:Point:srid=4326"

  lazy val features = Seq.tabulate(5) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, i + 0.1, s"2018-01-02T00:0$i:01.000Z", "POINT (45 55)")
  }

  step {
    addFeatures(features)
  }

  val filters = Seq(
    "name IN ('name0', 'name1', 'name2', 'name3', 'name4')",
    "age >= 0 AND age < 5",
    "bbox(geom,40,50,50,60)",
    "bbox(geom,40,50,50,60) AND dtg during 2018-01-02T00:00:00.000Z/2018-01-02T00:06:00.000Z"
  )

  val transforms = Seq(
    null,
    Array.empty[String],
    Array("name", "age", "geom")
  )

  val sorts = Seq(
    Array(SortBy.NATURAL_ORDER),
    Array(ff.sort(null, SortOrder.ASCENDING)),
    Array(ff.sort("name", SortOrder.ASCENDING)),
    Array(ff.sort("age", SortOrder.ASCENDING)),
    Array(ff.sort("weight", SortOrder.ASCENDING)),
    Array(ff.sort("dtg", SortOrder.ASCENDING)),
    Array(ff.sort("name", SortOrder.ASCENDING), ff.sort("age", SortOrder.DESCENDING))
  )

  val reverses = Seq(
    Array(SortBy.REVERSE_ORDER),
    Array(ff.sort(null, SortOrder.DESCENDING)),
    Array(ff.sort("name", SortOrder.DESCENDING)),
    Array(ff.sort("age", SortOrder.DESCENDING)),
    Array(ff.sort("weight", SortOrder.DESCENDING)),
    Array(ff.sort("dtg", SortOrder.DESCENDING)),
    Array(ff.sort("name", SortOrder.DESCENDING), ff.sort("age", SortOrder.ASCENDING))
  )

  "AccumuloDataStore" should {
    "sort" in {
      foreach(filters) { ecql =>
        val filter = ECQL.toFilter(ecql)
        foreach(transforms) { transform =>
          foreach(sorts) { sort =>
            val query = new Query(sft.getTypeName, filter, transform)
            query.setSortBy(sort)
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            result.map(_.getID) mustEqual features.map(_.getID)
          }
          foreach(reverses) { sort =>
            val query = new Query(sft.getTypeName, filter, transform)
            query.setSortBy(sort)
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            result.map(_.getID) mustEqual features.map(_.getID).reverse
          }
        }
      }
    }
  }
}
