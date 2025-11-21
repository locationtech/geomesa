/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import org.geotools.api.data.Query
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.{SortBy, SortOrder}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryPlannerTest extends Specification {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val sft = SimpleFeatureTypes.createType("query-planner", "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326")

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

    "fail to return a query plan for a bad ilike filter" in {
      val filter = ECQL.toFilter("name ilike '%abc\\'")
      val query = new Query(sft.getTypeName, filter)

      ds.getQueryPlan(query) must throwA[IllegalArgumentException]
    }

    "plan 'attribute is null' filter" in {
      val filter = ECQL.toFilter("name IS NULL")
      val query = new Query(sft.getTypeName, filter)

      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      val plan = plans.head
      plan.filter.index.name must not(beEqualTo(AttributeIndex.name))
      plan.filter.primary must beNone
      plan.filter.secondary must beSome(filter)
    }

    "be able to sort by id asc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(SortBy.NATURAL_ORDER)
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("", false)))
    }

    "be able to sort by id desc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(SortBy.REVERSE_ORDER)
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("", true)))
    }

    "be able to sort by an attribute asc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(ff.sort("name", SortOrder.ASCENDING))
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", false)))
    }

    "be able to sort by an attribute desc" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(ff.sort("name", SortOrder.DESCENDING))
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", true)))
    }

    "be able to sort by an attribute and id" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(ff.sort("name", SortOrder.ASCENDING), SortBy.NATURAL_ORDER)
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("name", false), ("", false)))
    }

    "be able to sort by an multiple attributes" >> {
      val query = new Query(sft.getTypeName)
      query.setSortBy(ff.sort("age", SortOrder.DESCENDING), ff.sort("name", SortOrder.ASCENDING))
      QueryRunner.configureQuery(sft, query)
      query.getHints.getSortFields must beSome(Seq(("age", true), ("name", false)))
    }

    "account for requested index hints when generating plans" >> {
      val filter = ECQL.toFilter("name = 'bob' AND dtg DURING 2025-04-29T00:00:00.000Z/2025-04-30T00:00:00.000Z")
      val query = new Query(sft.getTypeName, filter)

      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head.filter.index.name mustEqual AttributeIndex.name
      plans.head.filter.primary must beSome

      query.getHints.put(QueryHints.QUERY_INDEX, Z3Index.name)
      val hintPlans = ds.getQueryPlan(query)
      hintPlans must haveLength(1)
      hintPlans.head.filter.index.name mustEqual Z3Index.name
      hintPlans.head.filter.primary must beSome
    }

    "compute target schemas from transformation expressions" in {
      val sftName = "targetSchemaTest"
      val defaultSchema = "name:String,geom:Point:srid=4326,dtg:Date"
      val origSFT = SimpleFeatureTypes.createType(sftName, defaultSchema)

      val query = new Query(sftName, Filter.INCLUDE, "name", "helloName=strConcat('hello', name)", "geom")
      QueryRunner.configureDefaultQuery(origSFT, query)

      val transform = query.getHints.getTransformSchema
      transform must beSome
      SimpleFeatureTypes.encodeType(transform.get) mustEqual "name:String,helloName:String,*geom:Point:srid=4326"
    }
  }
}
