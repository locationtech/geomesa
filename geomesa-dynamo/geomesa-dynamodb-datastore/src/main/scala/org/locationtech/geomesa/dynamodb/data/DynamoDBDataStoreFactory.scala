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

  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = {
    createDataStore(params)
  }

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    val catalog: String = CATALOG.lookUp(params).asInstanceOf[String]
    val ddb: DynamoDB = DYNAMODBAPI.lookUp(params).asInstanceOf[DynamoDB]
    val rcus: Long = Option(CATALOG_RCUS.lookUp(params)).getOrElse(1L).asInstanceOf[JLong]
    val wcus: Long = Option(CATALOG_WCUS.lookUp(params)).getOrElse(1L).asInstanceOf[JLong]

    DynamoDBDataStore(catalog, ddb, rcus, wcus)
  }

  override def getDisplayName: String = "DynamoDB (GeoMesa)"

  override def getDescription: String = "GeoMesa DynamoDB Data Store"

  override def getParametersInfo: Array[Param] = DynamoDBDataStoreFactory.PARAMS

  override def canProcess(params: util.Map[String, Serializable]): Boolean = canProcessDynamo(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: util.Map[Key, _] = Collections.emptyMap()
}

object DynamoDBDataStoreFactory {
  val CATALOG = new Param("geomesa.dynamodb.catalog", classOf[String], "DynamoDB table name", true)
  val DYNAMODBAPI = new Param("geomesa.dynamodb.api", classOf[DynamoDB], "DynamoDB api instance", true)
  val CATALOG_RCUS =
    new Param(
      DynamoDBDataStore.RCU_Key,
      classOf[JLong],
      "DynamoDB read capacity units for catalog table",
      false)
  val CATALOG_WCUS =
    new Param(
      DynamoDBDataStore.WCU_Key,
      classOf[JLong],
      "DynamoDB write capacity units for catalog table",
      false)

  val PARAMS = Array(CATALOG, DYNAMODBAPI, CATALOG_RCUS, CATALOG_WCUS)

  def canProcessDynamo(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey(CATALOG.key) && params.containsKey(DYNAMODBAPI.key)
  }
}