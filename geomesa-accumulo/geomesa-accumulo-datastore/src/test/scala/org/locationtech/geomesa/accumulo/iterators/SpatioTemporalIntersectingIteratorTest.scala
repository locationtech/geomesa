/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, IteratorSetting}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.junit.runner.RunWith
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.accumulo.GEOMESA_ITERATORS_VERSION
import org.locationtech.geomesa.accumulo.iterators.TestData._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.GenSeq
import scala.collection.JavaConversions._
import scala.util.{Random, Try}

@RunWith(classOf[JUnitRunner])
class SpatioTemporalIntersectingIteratorTest extends Specification with Logging {

  def getRandomSuffix: String = {
    val chars = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    (1 to 20).map(i => chars(Random.nextInt(chars.size))).mkString
  }

  def setupMockAccumuloTable(entries: GenSeq[Entry], tableName: String): Connector = {
    val mockInstance = new MockInstance()
    val c = mockInstance.getConnector(TEST_USER, new PasswordToken(Array[Byte]()))
    c.tableOperations.create(tableName)
    val bw = c.createBatchWriter(tableName, GeoMesaBatchWriterConfig())

    logger.debug(s"Add mutations to table $tableName.")
    entries.foreach { entry =>
      bw.addMutations(createObject(entry.id, entry.wkt, entry.dt))
    }

    logger.debug(s"Done adding mutations to table $tableName.")

    bw.flush()
    c
  }

  "Consistency Iterator" should {

    "verify consistency of table" in {
      val table = "consistentTest"
      val c = setupMockAccumuloTable(TestData.shortListOfPoints, table)
      val s = c.createScanner(table, TEST_AUTHORIZATIONS)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])
      cfg.addOption(GEOMESA_ITERATORS_VERSION, CURRENT_SCHEMA_VERSION.toString)
      s.addScanIterator(cfg)

      // validate the total number of query-hits
      s.iterator().size mustEqual 0
    }

    "verify inconsistency of table" in {
      val table = "inconsistentTest"
      val c = setupMockAccumuloTable(TestData.shortListOfPoints, table)
      val bd = c.createBatchDeleter(table, TEST_AUTHORIZATIONS, 2, GeoMesaBatchWriterConfig())
      bd.addScanIterator({
        val cfg = new IteratorSetting(100, "regex", classOf[RegExFilter])
        RegExFilter.setRegexs(cfg, ".*~1~.*", null, ".*\\|data\\|1", null, false)
        cfg
      })

      bd.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bd.delete()
      bd.flush()

      val s = c.createScanner(table, TEST_AUTHORIZATIONS)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])
      cfg.addOption(GEOMESA_ITERATORS_VERSION, CURRENT_SCHEMA_VERSION.toString)
      s.addScanIterator(cfg)

      // validate the total number of query-hits
      s.iterator().size mustEqual 1
    }
  }

  "Feature with a null ID" should {
    "not fail to insert" in {
      val c = Try(setupMockAccumuloTable(TestData.pointWithNoID, "nullIdTest"))
      c.isFailure must be equalTo false
    }
  }
}
