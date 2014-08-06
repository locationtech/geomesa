/*
 *
 *  * Copyright 2014 Commonwealth Computer Research, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the License);
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an AS IS BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */


package geomesa.plugin.ui

import java.io.File

import com.google.common.io.Files
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector, ZooKeeperInstance}
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.hadoop.io.Text
import org.junit.runner.RunWith
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

      val writer = connector.createBatchWriter(table, new BatchWriterConfig)
      writer.addMutations(mutations.asJava)
      writer.flush()
      writer.close()

      // have to flush table in order for it to write to metadata table
      connector.tableOperations().flush(table, null, null, true)

      val metadata = GeoMesaDataStoresPage.getTableMetadata(connector,
                                                            "featureName",
                                                            "test",
                                                            connector.tableOperations().tableIdMap().get("test"))

      metadata.tableName must be equalTo "test"
      metadata.numTablets should be equalTo 100
      metadata.numEntries should be equalTo 450
      metadata.numSplits should be equalTo 100
      // exact file size varies slightly between runs... not sure why
      Math.abs(metadata.fileSize - 0.026) should be lessThan 0.001
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