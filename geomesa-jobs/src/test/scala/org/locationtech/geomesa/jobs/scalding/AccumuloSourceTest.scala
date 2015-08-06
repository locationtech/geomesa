/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import com.twitter.scalding.{Hdfs, Read, Write}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.jobs.scalding.taps.{AccumuloScheme, AccumuloTap}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class AccumuloSourceTest extends Specification {

  val instance = new MockInstance("accumulo-source-test")
  val connector = instance.getConnector("user", new PasswordToken("pwd"))
  Seq("table_in", "table_out").foreach(t => Try(connector.tableOperations().create(t)))

  val input = AccumuloInputOptions(instance.getInstanceName,
                                   instance.getZooKeepers,
                                   "user",
                                   "pwd",
                                   "table_in")
  val output = AccumuloOutputOptions(instance.getInstanceName,
                                     instance.getZooKeepers,
                                     "user",
                                     "pwd",
                                     "table_out")

  "AccumuloSource" should {
    "create read and write taps" in {
      implicit val mode = Hdfs(true, new Configuration())
      val readTap = AccumuloSource(input).createTap(Read)
      val writeTap = AccumuloSource(output).createTap(Write)
      readTap must haveClass[AccumuloTap]
      writeTap must haveClass[AccumuloTap]
      readTap.getIdentifier mustNotEqual(writeTap.getIdentifier)
    }
  }

  "AccumuloTap" should {
    "create tables and check their existence" in {
      skipped("this doesn't work with mock accumulo - revisit if we start using mini accumulo")

      val inScheme = new AccumuloScheme(input.copy(table = "test_create_in"))
      val outScheme = new AccumuloScheme(output.copy(table = "test_create_out"))

      val conf = new JobConf()
      val readTap = new AccumuloTap(Read, inScheme)
      readTap.resourceExists(conf) mustEqual(false)
      readTap.createResource(conf)
      readTap.resourceExists(conf) mustEqual(true)
      connector.tableOperations().exists("test_create_in") mustEqual(true)

      val writeTap = new AccumuloTap(Write, outScheme)
      writeTap.resourceExists(conf) mustEqual(false)
      writeTap.createResource(conf)
      writeTap.resourceExists(conf) mustEqual(true)
      connector.tableOperations().exists("test_create_out") mustEqual(true)
    }
  }

}
