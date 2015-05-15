/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 */

package org.locationtech.geomesa.core.index

import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.TestWithDataStore
import org.locationtech.geomesa.core.data.INTERNAL_GEOMESA_VERSION
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Z3IdxStrategyTest extends Specification with TestWithDataStore {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  val features = (0 until 10).map { i =>
    val name = s"name$i"
    val dtg = s"2010-05-07T0$i:00:00.000Z"
    val geom = s"POINT(40 6$i)"
    val sf = new ScalaSimpleFeature(s"$i", sft)
    sf.setAttributes(Array[AnyRef](name, dtg, geom))
    sf
  }

  addFeatures(features)

  val queryPlanner = new QueryPlanner(sft, SerializationType.KRYO, "", ds, NoOpHints, INTERNAL_GEOMESA_VERSION)
  val strategy = new Z3IdxStrategy
  val output = ExplainNull

  "Z3IdxStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getZ3Table(sftName), new Authorizations())
      scanner.foreach(println)
      println()
      success
    }

    "return all features for inclusive filter" >> {
      val filter = ECQL.toFilter("bbox(geom, 35, 55, 45, 75) AND dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z")
      val query = new Query(sftName, filter)
      val qps = strategy.getQueryPlans(query, queryPlanner, output)
      val features = qps.flatMap(qp => strategy.execute(qp, ds, output).map(qp.kvsToFeatures.left.get)) // TODO more robust map
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    }

    "return some features for exclusive geom filter" >> {
      val filter = ECQL.toFilter("bbox(geom, 35, 55, 45, 65) AND dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z")
      val query = new Query(sftName, filter)
      val qps = strategy.getQueryPlans(query, queryPlanner, output)
      val features = qps.flatMap(qp => strategy.execute(qp, ds, output).map(qp.kvsToFeatures.left.get)) // TODO more robust map
      features must haveSize(6)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(0, 1, 2, 3, 4, 5))
    }

    "return some features for exclusive date filter" >> {
      val filter = ECQL.toFilter("bbox(geom, 35, 55, 45, 75) AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z")
      val query = new Query(sftName, filter)
      val qps = strategy.getQueryPlans(query, queryPlanner, output)
      val features = qps.flatMap(qp => strategy.execute(qp, ds, output).map(qp.kvsToFeatures.left.get)) // TODO more robust map
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(6, 7, 8, 9))
    }
  }
}
