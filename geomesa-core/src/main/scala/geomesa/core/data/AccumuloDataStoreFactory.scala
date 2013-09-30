/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package geomesa.core.data

import java.io.Serializable
import java.util.{Map => JMap}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.DataStoreFactorySpi
import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import AccumuloDataStoreFactory.params._
  // this is a pass-through required of the ancestor interface
  def createNewDataStore(params: JMap[String, Serializable]) = createDataStore(params)

  def createDataStore(params: JMap[String, Serializable]) = {
    val authsStr = authsParam.lookUp(params).asInstanceOf[String]

    val authorizations =
      if(authsStr == null || authsStr.size == 0)
        new Authorizations()
      else
        new Authorizations(authsStr.split(","): _*)

    // allow the user to specify an index-schema format in the parameters
    val indexSchemaFormat : String = indexSchemaFormatParam.lookUp(params) match {
      case null => null
      case s:String => {
        val trimmed = s.trim
        if (trimmed.length == 0) null else trimmed
      }
      case _ => throw new Exception("Invalid index-schema format parameter")
    }

    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    if (mapreduceParam.lookUp(params) != null && mapreduceParam.lookUp(params).asInstanceOf[String] == "true")
      new MapReduceAccumuloDataStore(buildAccumuloConnector(params), tableName,
                                      authorizations, params, indexSchemaFormat)
    else
      new AccumuloDataStore(buildAccumuloConnector(params), tableName,
                             authorizations, indexSchemaFormat)
  }

  lazy val mockInstance = new MockInstance()
  lazy val mockConnector = mockInstance.getConnector("user", "password")

  def buildAccumuloConnector(params: JMap[String,Serializable]): Connector = {
    val zookeepers = zookeepersParam.lookUp(params).asInstanceOf[String]
    val instance = instanceIdParam.lookUp(params).asInstanceOf[String]
    val user = userParam.lookUp(params).asInstanceOf[String]
    val password = passwordParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])

    if (useMock) mockConnector
    else new ZooKeeperInstance(instance, zookeepers).getConnector(user, password.getBytes)
  }

  override def getDisplayName = "Accumulo Feature Data Store"

  override def getDescription = "Feature Data store for accumulo"

  override def getParametersInfo =
    Array(instanceIdParam, zookeepersParam, userParam, passwordParam,
          authsParam, tableNameParam, indexSchemaFormatParam)

  def canProcess(params: JMap[String,Serializable]) = params.containsKey(instanceIdParam.key)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {
  object params {
    val instanceIdParam = new Param("instanceId", classOf[String], "The Accumulo Instance ID", true)
    val zookeepersParam = new Param("zookeepers", classOf[String], "Zookeepers", true)
    val userParam = new Param("user", classOf[String], "Accumulo user", true)
    val passwordParam = new Param("password", classOf[String], "Password", true)
    val authsParam = new Param("auths", classOf[String], "Accumulo authorizations", true)
    val tableNameParam = new Param("tableName", classOf[String], "The Accumulo Table Name", true)
    val indexSchemaFormatParam = new Param("indexSchemaFormat", classOf[String], "The feature-specific index-schema format", false)
    val mockParam = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
    val mapreduceParam = new Param("useMapReduce", classOf[String], "Use MapReduce ingest", false)
  }

  import params._

  def configureJob(job: Job, params: JMap[String, Serializable]): Job = {
    val conf = job.getConfiguration
    conf.set(ZOOKEEPERS, zookeepersParam.lookUp(params).asInstanceOf[String])
    conf.set(INSTANCE_ID, instanceIdParam.lookUp(params).asInstanceOf[String])
    conf.set(ACCUMULO_USER, userParam.lookUp(params).asInstanceOf[String])
    conf.set(ACCUMULO_PASS, passwordParam.lookUp(params).asInstanceOf[String])
    conf.set(TABLE, tableNameParam.lookUp(params).asInstanceOf[String])
    conf.set(AUTHS, authsParam.lookUp(params).asInstanceOf[String])
    job
  }

  def getMRAccumuloConnectionParams(conf: Configuration): JMap[String,AnyRef] =
    Map(
         zookeepersParam.key -> conf.get(ZOOKEEPERS),
         instanceIdParam.key -> conf.get(INSTANCE_ID),
         userParam.key -> conf.get(ACCUMULO_USER),
         passwordParam.key -> conf.get(ACCUMULO_PASS),
         tableNameParam.key -> conf.get(TABLE),
         authsParam.key -> conf.get(AUTHS),
         "useMapReduce" -> "true")
}