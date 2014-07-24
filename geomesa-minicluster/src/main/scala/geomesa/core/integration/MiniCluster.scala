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

package geomesa.core.integration

import java.io._

import com.google.common.io.Files
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}
import geomesa.core.integration.data.{DataLoader, DataType}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.commons.cli.{BasicParser, Option, Options}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.cql2.CQL

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

object MiniCluster extends App with Logging {

  val options = new Options()
  options.addOption({
    val option = new Option("p", "port", true, "The port that the cluster will run on")
    option.setArgs(1)
    option.setArgName("port")
    option
  })
  options.addOption({
    val option = new Option("t", "type", true, "The type of data that will be loaded into the cluster")
    option.setArgs(1)
    option.setArgName("type")
    option.setRequired(true)
    option
  })
  options.addOption(new Option("q", "query", false, "Will run a query against loaded data for a sanity check"))

  val commands = new BasicParser().parse(options, args)

  val dataType = new DataType(commands.getOptionValue("t"))
  val port = commands.getOptionValue("p", "10099")

  val mc = new MiniCluster(dataType, port)

  var loop = true
  val thread = Thread.currentThread()

  sys.addShutdownHook {
    loop = false
    mc.stopCluster()
    thread.interrupt()
  }

  mc.startCluster()
  mc.loadData()

  logger.info(s"Instance: ${mc.miniCluster.getInstanceName} - ${mc.zooKeepers} - ${mc.user}/${mc.password}")
  logger.info(s"MiniCluster running with ${mc.dataType.sftName} data")

  if (commands.hasOption("q")) {
    mc.queryData()
  }

  while(loop) {
    Try(Thread.sleep(10000)) match {
      case Success(_) =>
      case Failure(e) => loop = false
    }
  }
}

/**
 * Class to manage a mini cluster instance
 *
 * @param dataType type of data contained in this instance
 * @param zooPort port the instance will be available on
 */
class MiniCluster(val dataType: DataType, val zooPort: String) extends Logging {

  val localhostPrefix = "localhost:"

  val user = "root"

  val password = "password"

  val zooKeepers: String = localhostPrefix + zooPort

  val confDirectory: File = Files.createTempDir()

  var miniCluster: MiniAccumuloCluster = new MiniAccumuloCluster(confDirectory, "password")

  var running = false

  logger.debug("Using directory '{}'", confDirectory.getAbsolutePath)
  logger.trace("Random zoo port '{}'", miniCluster.getZooKeepers)
  logger.debug("Using port '{}'", zooPort)

  for {
    fileName <- Array("conf/zoo.cfg", "conf/accumulo-site.xml")
    file = new File(confDirectory, fileName)
    oldPort = miniCluster.getZooKeepers.substring(localhostPrefix.length)
    source <- Try(Source.fromFile(file))
  } {
    val temp = new File(confDirectory, fileName + ".tmp")
    Try(new PrintWriter(new FileWriter(temp))).foreach { writer =>
      for { line <- source.getLines() } { writer.println(line.replace(oldPort, zooPort)) }
      Try(writer.close())
    }
    Try(source.close())
    temp.renameTo(file)
  }

  lazy val dataStore: AccumuloDataStore = {
    if (!running) {
      throw new RuntimeException("Can't get connector for a stopped instance")
    }
    val params = Map(
                      "instanceId"    -> miniCluster.getInstanceName(),
                      "zookeepers"    -> zooKeepers,
                      "user"          -> user,
                      "password"      -> password,
                      "auths"         -> "",
                      "visibilities"  -> "",
                      "tableName"     -> dataType.table)
    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
  }

  lazy val connector: Connector = {
    if (!running) {
      throw new RuntimeException("Can't get connector for a stopped instance")
    }
    val instance = new ZooKeeperInstance(miniCluster.getInstanceName(), zooKeepers)
    instance.getConnector(user, new PasswordToken(password))
  }

  /**
   * Starts the cluster
   */
  def startCluster(): Unit = {
    if (running) {
      logger.warn("Trying to start an already running instance")
    } else {
      logger.info("Starting mini cluster on port {}...", zooPort)
      miniCluster.start()
      logger.info("Started")
      running = true
    }
  }

  /**
   * Stops the cluster
   */
  def stopCluster(): Unit = {
    if (running) {
      logger.info("Stopping...")
      miniCluster.stop()
      if (!confDirectory.delete()) {
        confDirectory.deleteOnExit()
      }
      logger.info("Stopped")
      running = false
    } else {
      logger.info("Already stopped")
    }
  }

  /**
   * Loads data from tsv files into the mini cluster instance
   */
  def loadData(): Unit = {
    if (!running) {
      throw new RuntimeException("Can't load data into a stopped instance")
    }

    dataStore.createSchema(dataType.simpleFeatureType)

    val importer = new DataLoader(dataType)
    importer.loadData(dataStore)
  }

  /**
   * Diagnostic method - queries loaded data to verify something is there
   */
  def queryData(): Unit = {
    if (!running) {
      throw new RuntimeException("Can't verify data into a stopped instance")
    }

    logger.info("verifying data...")
    val query = new Query(dataType.sftName, CQL.toFilter("INCLUDE"))

    // get the feature store used to query the GeoMesa data
    val featureStore = dataStore.getFeatureSource(dataType.sftName).asInstanceOf[AccumuloFeatureStore]

    var count = 0
    // execute the query - iterate through the results to simulate a real use-case
    val features = featureStore.getFeatures(query).features()
    while(features.hasNext) {
      count = count + 1
      features.next()
    }
    logger.info(s"queried $count features")
  }
}
