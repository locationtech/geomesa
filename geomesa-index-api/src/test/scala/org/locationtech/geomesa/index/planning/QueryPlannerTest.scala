/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.sort.{SortBy, SortOrder}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryPlannerTest extends Specification {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val sft = SimpleFeatureTypes.createType("query-planner", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  val ds = new TestGeoMesaDataStore(true)
  ds.createSchema(sft)

  val planner = ds.queryPlanner

  "QueryPlanner" should {
    "be a queryPlanner" in {
      planner.getClass mustEqual classOf[QueryPlanner[_]] // sanity check
    }

    "throw an exception for invalid requested index during explain" in {
      val query = new Query(sft.getTypeName)
      query.getHints.put(QueryHints.QUERY_INDEX, "foo")
      planner.planQuery(sft, query) must throwAn[IllegalArgumentException]
    }

    "throw an exception for invalid requested index during query" in {
      val query = new Query(sft.getTypeName)
      query.getHints.put(QueryHints.QUERY_INDEX, "foo")
      planner.runQuery(sft, query) must throwAn[IllegalArgumentException]
    }

    "return z3 index for spatio-temporal queries that are bounded by the epoch" in {
      val filter = ECQL.toFilter("BBOX(geom, -1, -1, 1, 1) and dtg > '1970-01-01' and dtg < '2018-01-01'")
      val query = new Query(sft.getTypeName, filter)
      foreach(ds.getQueryPlan(query)) { plan =>
        plan.filter.index.name mustEqual Z3Index.name
      }
    }

    "be able to sort by id asc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(SortBy.NATURAL_ORDER))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("", false)))
    }

    "be able to sort by id desc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(SortBy.REVERSE_ORDER))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("", true)))
    }

    "be able to sort by an attribute asc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(ff.sort("name", SortOrder.ASCENDING)))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", false)))
    }

    "be able to sort by an attribute desc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(ff.sort("name", SortOrder.DESCENDING)))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", true)))
    }

    "be able to sort by an attribute and id" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(ff.sort("name", SortOrder.ASCENDING), SortBy.NATURAL_ORDER))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", false), ("", false)))
    }

    "be able to sort by an multiple attributes" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(Array(ff.sort("age", SortOrder.DESCENDING), ff.sort("name", SortOrder.ASCENDING)))
      QueryPlanner.setQuerySort(sft, query)
      query.getHints.getSortFields must beSome(Seq(("age", true), ("name", false)))
    }
  }
}
