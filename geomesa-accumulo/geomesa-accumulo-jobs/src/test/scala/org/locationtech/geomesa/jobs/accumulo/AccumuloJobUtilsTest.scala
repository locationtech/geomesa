/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.JoinPlan
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloJobUtilsTest extends Specification with TestWithDataStore {

  override val spec =
    "name:String:index=join:cardinality=high,age:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326"

  def getQuery(ecql: String, attributes: Array[String] = null): Query = {
    val q = new Query(sftName, ECQL.toFilter(ecql), attributes)
    // use heuristic cost evaluation to ensure consistent expectations
    q.getHints.put(QueryHints.COST_EVALUATION, CostEvaluation.Index)
    q
  }

  val queries = Seq(
    // check for non-join attributes queries
    ("name = 'foo' AND bbox(geom,0,0,10,10)", Array("geom"), JoinIndex),
    // check for join queries fall back to secondary option
    ("name = 'foo' AND bbox(geom,0,0,10,10)", null, Z2Index),
    // check for fall-back full table scan
    ("name = 'foo'", null, Z3Index),
    // check for full indices
    ("age = 20", null, AttributeIndex),
    // check for full indices in complex queries
    ("age = 20 and bbox(geom,0,0,10,10)", null, AttributeIndex),
    // check for other indices
    ("bbox(geom,0,0,10,10)", null, Z2Index)
  )

  "AccumuloJobUtils" should {
    "load list of jars from class resource" in {
      AccumuloJobUtils.defaultLibJars must not(beNull)
      AccumuloJobUtils.defaultLibJars must not(beEmpty)
      AccumuloJobUtils.defaultLibJars must contain("accumulo-core")
      AccumuloJobUtils.defaultLibJars must contain("libthrift")
    }
    "not return join plans for getSingleQueryPlan" in {
      foreach(queries) { case (ecql, attributes, index) =>
        // check that non-join attributes queries are supported
        val qp = AccumuloJobUtils.getSingleQueryPlan(ds, getQuery(ecql, attributes))
        qp must not(beAnInstanceOf[JoinPlan])
        qp.filter.index.name mustEqual index.name
      }
    }
    "not return join plans for getMultiQueryPlan" in {
      foreach(queries) { case (ecql, attributes, index) =>
        // check that non-join attributes queries are supported
        val qp = AccumuloJobUtils.getMultipleQueryPlan(ds, getQuery(ecql, attributes))
        foreach(qp)(_ must not(beAnInstanceOf[JoinPlan]))
        foreach(qp)(_ .filter.index.name mustEqual index.name)
      }
    }
  }
}
