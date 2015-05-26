/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 */

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.INTERNAL_GEOMESA_VERSION
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Z3IdxStrategyTest extends Specification with TestWithDataStore {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  val features =
    (0 until 10).map { i =>
      val name = s"name$i"
      val dtg = s"2010-05-07T0$i:00:00.000Z"
      val geom = s"POINT(40 6$i)"
      val sf = new ScalaSimpleFeature(s"$i", sft)
      sf.setAttributes(Array[AnyRef](name, dtg, geom))
      sf
    } ++ (10 until 20).map { i =>
      val name = s"name$i"
      val dtg = s"2010-05-${i}T$i:00:00.000Z"
      val geom = s"POINT(40 6${i - 10})"
      val sf = new ScalaSimpleFeature(s"$i", sft)
      sf.setAttributes(Array[AnyRef](name, dtg, geom))
      sf
    } ++ (20 until 30).map { i =>
      val name = s"name$i"
      val dtg = s"2010-05-${i}T${i-10}:00:00.000Z"
      val geom = s"POINT(40 8${i - 20})"
      val sf = new ScalaSimpleFeature(s"$i", sft)
      sf.setAttributes(Array[AnyRef](name, dtg, geom))
      sf
    }
  addFeatures(features)

  implicit val ff = CommonFactoryFinder.getFilterFactory2
  val queryPlanner = new QueryPlanner(sft, SerializationType.KRYO, null, ds, NoOpHints, INTERNAL_GEOMESA_VERSION)
  val strategy = new Z3IdxStrategy
  val output = ExplainNull

  "Z3IdxStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getZ3Table(sftName), new Authorizations())
      scanner.foreach(e => println(e.getKey.getRow().getBytes.toSeq))
      println()
      success
    }

    "return all features for inclusive filter" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(10)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
    }

    "return some features for exclusive geom filter" >> {
      val filter = "bbox(geom, 35, 55, 45, 65)" +
          " AND dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(6)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
    }

    "return some features for exclusive date filter" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
    }

    "work with whole world filter" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg during 2010-05-07T05:00:00.000Z/2010-05-07T08:00:00.000Z"
      val features = execute(filter)
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(5 to 8)
    }

    "work across week bounds" >> {
      val filter = "bbox(geom, 35, 65, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-21T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(9)
      features.map(_.getID.toInt) must containTheSameElementsAs((6 to 9) ++ (15 to 19))
    }

    "work with whole world filter across week bounds" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-21T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(15)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 20)
    }

    "work with whole world filter across 3 week periods" >> {
      val filter = "bbox(geom, -180, -90, 180, 90)" +
        " AND dtg during 2010-05-08T06:00:00.000Z/2010-05-30T00:00:00.000Z"
      val features = execute(filter)
      features must haveSize(20)
      features.map(_.getID.toInt) must containTheSameElementsAs(10 to 29)
    }

    "apply secondary filters" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z" +
          " AND name = 'name8'"
      val features = execute(filter)
      features must haveSize(1)
      features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
    }

    "apply transforms" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z"
      val features = execute(filter, Some(Array("name")))
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 2) // geom always gets added
      forall(features)((f: SimpleFeature) => f.getAttribute("geom") must not(beNull))
      forall(features)((f: SimpleFeature) => f.getAttribute("name") must not(beNull))
    }

    "apply functional transforms" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z"
      val features = execute(filter, Some(Array("derived=strConcat('my', name)")))
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 2) // geom always gets added
      forall(features)((f: SimpleFeature) => f.getAttribute("geom") must not(beNull))
      forall(features)((f: SimpleFeature) => f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d"))
    }

    "apply transforms using only the row key" >> {
      val filter = "bbox(geom, 35, 55, 45, 75)" +
          " AND dtg during 2010-05-07T06:00:00.000Z/2010-05-08T00:00:00.000Z"
      val (_, qps) = getQueryPlans(filter, Some(Array("geom", "dtg")))
      forall(qps)((s: StrategyPlan) => s.plan.columnFamilies must containTheSameElementsAs(Seq(Z3Table.BIN_CF)))

      val features = execute(filter, Some(Array("geom", "dtg")))
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
      forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 2)
      forall(features)((f: SimpleFeature) => f.getAttribute("geom") must not(beNull))
      forall(features)((f: SimpleFeature) => f.getAttribute("dtg") must not(beNull))
    }.pendingUntilFixed("not implemented")
  }

  def execute(ecql: String, transforms: Option[Array[String]] = None) = {
    val (query, qps) = getQueryPlans(ecql, transforms)
    queryPlanner.executePlans(query, qps, deduplicate = false).toSeq
  }

  def getQueryPlans(ecql: String, transforms: Option[Array[String]] = None): (Query, Seq[StrategyPlan]) = {
    val filter = org.locationtech.geomesa.accumulo.filter.rewriteFilterInDNF(ECQL.toFilter(ecql))
    val query = transforms match {
      case None    => new Query(sftName, filter)
      case Some(t) =>
        val q = new Query(sftName, filter, t)
        setQueryTransforms(q, sft)
        q
    }
    (query, strategy.getQueryPlans(query, queryPlanner, output).map(qp => StrategyPlan(strategy, qp)))
  }
}
