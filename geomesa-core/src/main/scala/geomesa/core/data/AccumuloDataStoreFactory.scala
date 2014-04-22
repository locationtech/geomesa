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
import org.apache.accumulo.core.client.security.tokens.PasswordToken

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

    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val connector =
      if(params.containsKey(connParam.key)) connParam.lookUp(params).asInstanceOf[Connector]
      else buildAccumuloConnector(params)

    val featureEncoding =
       if (featureEncParam.lookUp(params) != null)
         featureEncParam.lookUp(params).asInstanceOf[String] match {
           case "avro" => FeatureEncoding.AVRO
           case "text" => FeatureEncoding.TEXT
           case _ => throw new IllegalArgumentException("Feature type must be of type text or avro")
         }
       else FeatureEncoding.AVRO

    if (mapreduceParam.lookUp(params) != null && mapreduceParam.lookUp(params).asInstanceOf[String] == "true")
      if(idxSchemaParam.lookUp(params) != null)
        new MapReduceAccumuloDataStore(connector,
                                       tableName,
                                       authorizations,
                                       params,
                                       idxSchemaParam.lookUp(params).asInstanceOf[String],
                                       featureEncoding = featureEncoding)
      else
        new MapReduceAccumuloDataStore(connector, tableName, authorizations, params, featureEncoding = featureEncoding)
    else {
      if(idxSchemaParam.lookUp(params) != null)
        new AccumuloDataStore(connector,
                              tableName,
                              authorizations,
                              idxSchemaParam.lookUp(params).asInstanceOf[String],
                              featureEncoding = featureEncoding)
      else
        new AccumuloDataStore(connector, tableName, authorizations, featureEncoding = featureEncoding)
    }

  }

  def buildAccumuloConnector(params: JMap[String,Serializable]): Connector = {
    val zookeepers = zookeepersParam.lookUp(params).asInstanceOf[String]
    val instance = instanceIdParam.lookUp(params).asInstanceOf[String]
    val user = userParam.lookUp(params).asInstanceOf[String]
    val password = passwordParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])

    if (useMock) 
      new MockInstance(instance).getConnector(user, new PasswordToken(password.getBytes))
    else 
      new ZooKeeperInstance(instance, zookeepers).getConnector(user, new PasswordToken(password.getBytes))
  }

  override def getDisplayName = "Accumulo Feature Data Store"

  override def getDescription = "Feature Data store for accumulo"

  override def getParametersInfo =
    Array(instanceIdParam, zookeepersParam, userParam, passwordParam,
          authsParam, tableNameParam, idxSchemaParam)

  def canProcess(params: JMap[String,Serializable]) =
    params.containsKey(instanceIdParam.key) || params.containsKey(connParam.key)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {
  object params {
    val connParam         = new Param("connector", classOf[Connector], "The Accumulo connector", false)
    val instanceIdParam   = new Param("instanceId", classOf[String], "The Accumulo Instance ID", true)
    val zookeepersParam   = new Param("zookeepers", classOf[String], "Zookeepers", true)
    val userParam         = new Param("user", classOf[String], "Accumulo user", true)
    val passwordParam     = new Param("password", classOf[String], "Password", true)
    val authsParam        = new Param("auths", classOf[String], "Accumulo authorizations", true)
    val tableNameParam    = new Param("tableName", classOf[String], "The Accumulo Table Name", true)
    val idxSchemaParam    = new Param("indexSchemaFormat", classOf[String], "The feature-specific index-schema format", false)
    val mockParam         = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
    val mapreduceParam    = new Param("useMapReduce", classOf[String], "Use MapReduce ingest", false)
    val featureEncParam   = new Param("featureEncoding", classOf[String], "The feature encoding format (text or avro). Default is Avro", false, "avro")
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
    conf.set(FEATURE_ENCODING, featureEncParam.lookUp(params).asInstanceOf[String])
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
      featureEncParam.key -> conf.get(FEATURE_ENCODING),
      "useMapReduce" -> "true")
}