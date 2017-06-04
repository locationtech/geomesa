/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{BatchScanPlan, JoinPlan}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BatchMultiScannerTest extends Specification {

  val sftName = "bmstest"
  val schema = SimpleFeatureTypes.createType(sftName,
    s"name:String:index=true,age:String:index=true,idStr:String:index=true,dtg:Date,*geom:Point:srid=4326")

  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
  val dateToIndex = sdf.parse("20140102")

  val catalogTable = "bmstestcatalog"
  val user = "myuser"
  val pass = "mypassword"
  val instanceName = "bmsTestInst"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(
      Map(
        "instanceId" -> instanceName,
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"       -> user,
        "password"   -> pass,
        "tableName"  -> catalogTable,
        "useMock"    -> "true")
    ).asInstanceOf[AccumuloDataStore]

  val ds = createStore
  ds.createSchema(schema)
  val sft = ds.getSchema(sftName)
  val fs = ds.getFeatureSource(sftName)

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  for (
    name  <- List("a", "b", "c", "d");
    tuple <- List(1, 2, 3, 4).zip(List(45, 46, 47, 48))
  ) tuple match { case (i, lat) =>
    val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
    sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
    sf.setAttribute("dtg", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
    sf.setAttribute("name", name)
    sf.setAttribute("idStr", sf.getID)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    featureCollection.add(sf)
  }

  fs.addFeatures(featureCollection)

  def attrIdxEqualQuery(attr: String, value: String, batchSize: Int): Int = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val qps = ds.getQueryPlan(new Query(sftName, ECQL.toFilter(s"$attr = '$value'")))
    qps must haveLength(1)
    val qp = qps.head
    qp must beAnInstanceOf[JoinPlan]
    qp.ranges must haveLength(sft.getAttributeShards)

    val instance = new MockInstance(instanceName)
    val conn = instance.getConnector(user, new PasswordToken(pass))

    conn.tableOperations.exists(qp.table) must beTrue
    val attrScanner = conn.createBatchScanner(qp.table, new Authorizations(), 1)
    attrScanner.setRanges(qp.ranges)

    val jp = qp.join.get._2.asInstanceOf[BatchScanPlan]
    conn.tableOperations().exists(jp.table) must beTrue

    val bms = new BatchMultiScanner(ds, attrScanner, jp, qp.join.get._1, 5, batchSize)

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
