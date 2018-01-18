/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryPlannerTest extends Specification {

  val typeName = "query-planner"
  val spec = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"

  val sft = SimpleFeatureTypes.createType(typeName, spec)

  val ds = new TestGeoMesaDataStore(true)
  ds.createSchema(sft)

  val planner = ds.queryPlanner

  "QueryPlanner" should {
    "be a queryPlanner" in {
      planner.getClass mustEqual classOf[QueryPlanner[_, _, _]] // sanity check
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
  }
}
