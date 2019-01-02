/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, JoinPlan}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BatchMultiScannerTest extends TestWithDataStore {

  override val spec = s"name:String:index=join,age:String:index=join,idStr:String:index=join,dtg:Date,*geom:Point:srid=4326"

  val features = for (
    name  <- List("a", "b", "c", "d");
    tuple <- List(1, 2, 3, 4).zip(List(45, 46, 47, 48))
  ) yield {
    tuple match { case (i, lat) =>
      ScalaSimpleFeature.create(sft, s"$name$i", name, null, s"$name$i", "2011-01-01T00:00:00Z", f"POINT($lat%d $lat%d)")
    }
  }

  step {
    addFeatures(features)
  }

  def attrIdxEqualQuery(attr: String, value: String, batchSize: Int): Int = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val qps = ds.getQueryPlan(new Query(sftName, ECQL.toFilter(s"$attr = '$value'")))
    qps must haveLength(1)
    val qp = qps.head
    qp must beAnInstanceOf[JoinPlan]
    qp.ranges must haveLength(sft.getAttributeShards)

    foreach(qp.tables)(table => connector.tableOperations.exists(table) must beTrue)
    val attrScanner = connector.createBatchScanner(qp.tables.head, new Authorizations(), 1)
    attrScanner.setRanges(qp.ranges)

    val jp = qp.join.get._2.asInstanceOf[BatchScanPlan]
    foreach(jp.tables)(table => connector.tableOperations.exists(table) must beTrue)

    val bms = new BatchMultiScanner(ds.connector, attrScanner, jp, qp.join.get._1, ds.auths, 5, batchSize)

    val retrieved = bms.iterator.map(jp.entriesToFeatures).toList
    forall(retrieved)(_.getAttribute(attr) mustEqual value)

    retrieved.size
  }

  "BatchMultiScanner" should {
    "handle corner cases for attr index queries" in {
      foreach(List(1, 2, 3, 4, 5, 6, 8, 15, 16, 17, 200)) { batchSize =>
        // test something that exists
        attrIdxEqualQuery("name", "b", batchSize) mustEqual 4

        // test something that doesn't exist!
        attrIdxEqualQuery("name", "no exista", batchSize) mustEqual 0

        // test size of 1
        attrIdxEqualQuery("idStr", "c1", batchSize) mustEqual 1

        // test something that was stored as a null
        attrIdxEqualQuery("age", "43", batchSize) mustEqual 0
      }
    }

    "should throw an exception on a bad batch size" in {
      attrIdxEqualQuery("age", "43", 0) must throwA[IllegalArgumentException]
      attrIdxEqualQuery("age", "43", -1) must throwA[IllegalArgumentException]
    }
  }

}
