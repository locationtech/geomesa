/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.plugin.ui

import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.hadoop.io.Text
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class GeoMesaDataStoresPageTest extends Specification {

  sequential

  val table = "test"

  var tempDirectory: File = null
  var miniCluster: MiniAccumuloCluster = null
  var connector: Connector = null

  "GeoMesaDataStoresPage" should {

    step(startCluster())

    "scan metadata table accurately" in {

      connector.tableOperations.create(table)
      val splits = (0 to 99).map {
        s => "%02d".format(s)
      }.map(new Text(_))
      connector.tableOperations().addSplits(table, new java.util.TreeSet[Text](splits.asJava))

      val mutations = (0 to 149).map {
        i =>
          val mutation = new Mutation("%02d-row-%d".format(i%100, i))
          mutation.put("cf1", "cq1", s"value1-$i")
          mutation.put("cf1", "cq2", s"value2-$i")
          mutation.put("cf2", "cq3", s"value3-$i")
          mutation
      }

      val writer = connector.createBatchWriter(table, GeoMesaBatchWriterConfig())
      writer.addMutations(mutations.asJava)
      writer.flush()
      writer.close()

      // have to flush table in order for it to write to metadata table
      connector.tableOperations().flush(table, null, null, true)

      val metadata = GeoMesaDataStoresPage.getTableMetadata(connector,
                                                            "feature",
                                                            "test",
                                                             connector.tableOperations().tableIdMap().get("test"),
                                                            "test table")

      metadata.table must be equalTo "test"
      metadata.displayName must be equalTo "test table"
      metadata.numTablets should be equalTo 100
      metadata.numEntries should be equalTo 450
      metadata.numSplits should be equalTo 100
      // exact file size varies slightly between runs... not sure why
      Math.abs(metadata.fileSize - 0.026*1024*1024) should be lessThan 0.001*1024*1024
    }

    step(stopCluster())
  }

  def startCluster() = {
    tempDirectory = Files.createTempDir()
    miniCluster = new MiniAccumuloCluster(tempDirectory, "password")
    miniCluster.start()
    val instance = new ZooKeeperInstance(miniCluster.getInstanceName(), miniCluster.getZooKeepers())
    connector = instance.getConnector("root", new PasswordToken("password"))
  }

  def stopCluster() = {
    miniCluster.stop()
    if (!tempDirectory.delete()) {
      tempDirectory.deleteOnExit()
    }
  }

}
