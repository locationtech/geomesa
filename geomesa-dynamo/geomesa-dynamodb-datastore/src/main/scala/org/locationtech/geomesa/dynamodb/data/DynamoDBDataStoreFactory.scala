/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.lang.{Long => JLong}
import java.util
import java.util.Collections

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}

class DynamoDBDataStoreFactory extends DataStoreFactorySpi {
  import DynamoDBDataStoreFactory._
  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    val catalog: String = CATALOG.lookUp(params).asInstanceOf[String]
    val ddb: DynamoDB = DYNAMODBAPI.lookUp(params).asInstanceOf[DynamoDB]
    val rcus: Long = Option(RCUS.lookUp(params)).getOrElse(1L).asInstanceOf[JLong]
    val wcus: Long = Option(WCUS.lookUp(params)).getOrElse(1L).asInstanceOf[JLong]

    DynamoDBDataStore(catalog, ddb, rcus, wcus)
  }

  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def getDisplayName: String = "DynamoDB (GeoMesa)"

  override def getDescription: String = "GeoMesa DynamoDB Data Store"

  override def getParametersInfo: Array[Param] = DynamoDBDataStoreFactory.PARAMS

  override def canProcess(params: util.Map[String, Serializable]): Boolean = canProcessDynamo(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: util.Map[Key, _] = Collections.emptyMap()
}

object DynamoDBDataStoreFactory {
  val CATALOG     = new Param("geomesa.dynamodb.catalog", classOf[String], "DynamoDB table name", true)
  val DYNAMODBAPI = new Param("geomesa.dynamodb.api", classOf[DynamoDB], "DynamoDB api instance", true)
  val RCUS        = new Param(DynamoDBDataStore.rcuKey, classOf[java.lang.Long], "DynamoDB read capacity units", false)
  val WCUS        = new Param(DynamoDBDataStore.wcuKey, classOf[java.lang.Long], "DynamoDB write capacity units", false)

  val PARAMS  = Array(CATALOG, DYNAMODBAPI, RCUS, WCUS)

  def canProcessDynamo(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey(CATALOG.key) && params.containsKey(DYNAMODBAPI.key)
  }
}