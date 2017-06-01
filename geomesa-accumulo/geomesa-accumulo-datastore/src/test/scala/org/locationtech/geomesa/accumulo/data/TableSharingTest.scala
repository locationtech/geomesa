/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index.{RecordIndex, XZ2Index}
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.accumulo.iterators.TestData._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TableSharingTest extends Specification with LazyLogging {

  sequential

  val tableName = "sharingTest"

  val ds = {
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> tableName,
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  // Check existence of tables?
  val mockInstance = new MockInstance("mycloud")
  val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

  // Three datasets.  Each with a common field: attr2?
  val sft1 = TestData.getFeatureType("1", tableSharing = true)
  val sft2 = TestData.getFeatureType("2", tableSharing = true)
  val sft3 = TestData.getFeatureType("3", tableSharing = false)

  // Load up data
  val mediumData1 = mediumData.map(createSF(_, sft1))
  val mediumData2 = mediumData.map(createSF(_, sft2))
  val mediumData3 = mediumData.map(createSF(_, sft3))

  val fs1 = getFeatureStore(ds, sft1, mediumData1)
  val fs2 = getFeatureStore(ds, sft2, mediumData2)
  val fs3 = getFeatureStore(ds, sft3, mediumData3)

  // TODO: Add tests to check if the correct tables exist and if the metadata is all correct.

  // Check the sft's indexschema

  val retrievedSFT1 = ds.getSchema(sft1.getTypeName)

  val list2: util.SortedSet[String] = c.tableOperations().list

  // At least three queries: st, attr, id.
  def filterCount(f: Filter) = mediumData1.count(f.evaluate)
  // note: size returns an estimated amount, instead we need to actually count the features
  def queryCount(f: Filter, fs: SimpleFeatureSource) = SelfClosingIterator(fs.getFeatures(f)).length

  val id = "IN(100001, 100011)"
  val st = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
  val at = "attr2 = '2nd100001'"

  // This function compares the number of returned results.
  def compare(fs: String, step: Int, featureStore2: SimpleFeatureSource = fs2) = {
    val f = ECQL.toFilter(fs)
    val fc = filterCount(f)
    val q1 = queryCount(f, fs1)
    val q3 = queryCount(f, fs3)

    step match {
      case 1 => check(q3)
      case 2 =>
      case 3 => check(0)   // Feature source #2 should be empty
      case 4 => check(q3)
    }

    // Checks feature source #2's query count against the input.
    def check(count: Int) = {
      val q2 = queryCount(f, featureStore2)

      s"fs2 must get $count results from filter $fs" >> {
        q2 mustEqual count
      }
    }

    s"fc and fs1 get the same results from filter $fs" >> { fc mustEqual q1 }
    s"fs1 and fs3 get the same results from filter $fs" >> { q1 mustEqual q3 }
  }

  "all three queries" should {
    "work for all three features (after setup) " >> {
      compare(id, 1)
      compare(st, 1)
      compare(at, 1)
    }
  }

  // Delete one shared table feature to ensure that deleteSchema works.
  s"Removing ${sft2.getTypeName}" should {
    val sft2Scanner = ds.connector.createScanner(XZ2Index.getTableName(sft2.getTypeName, ds), ds.auths)
    val sft2RecordScanner = ds.connector.createScanner(RecordIndex.getTableName(sft2.getTypeName, ds), ds.auths)

    ds.removeSchema(sft2.getTypeName)

    // TODO: Add tests to measure what tables exist, etc.
    // TODO: test ds.getNames.

    // TODO: Observe that this kind of collection is empty.
    sft2Scanner.setRange(new org.apache.accumulo.core.data.Range())
    sft2Scanner.iterator
      .map(e => s"ST Key: ${e.getKey}")
      .filter(_.contains("feature2"))
      .take(10)
      .foreach(s => logger.debug(s))

    sft2RecordScanner.setRange(new org.apache.accumulo.core.data.Range())
    sft2RecordScanner.iterator.take(10).foreach { e => logger.debug(s"Record Key: ${e.getKey}") }

    s"result in FeatureStore named ${sft2.getTypeName} being gone" >> {
      ds.getNames.contains(sft2.getTypeName) must beFalse
    }
  }

  // Query again.
  "all three queries" should {
    "work for all three features (after delete) " >> {
      compare(id, 2)
      compare(st, 2)
      compare(at, 2)
    }
  }

  // Query again after recreating just the SFT for feature source 2.
  "all three queries" should {
    ds.createSchema(sft2)
    val newfs2 = ds.getFeatureSource(sft2.getTypeName)

    "work for all three features (after recreating the schema for SFT2) " >> {
      compare(id, 3, newfs2)
      compare(st, 3, newfs2)
      compare(at, 3, newfs2)
    }
  }

  // Query again after reingesting a feature source #2.
  "all three queries" should {
    val fs2ReIngested = getFeatureStore(ds, sft2, mediumData2)

    "work for all three features (after re-ingest) " >> {
      compare(id, 4, featureStore2 = fs2ReIngested)
      compare(st, 4, featureStore2 = fs2ReIngested)
      compare(at, 4, featureStore2 = fs2ReIngested)
    }
  }
}
