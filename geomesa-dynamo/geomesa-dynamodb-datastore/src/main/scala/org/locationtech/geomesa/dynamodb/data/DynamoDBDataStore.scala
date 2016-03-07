/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Transaction
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentState}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.dynamo.core.{DynamoPrimaryKey, SchemaValidation}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class DynamoDBDataStore(val catalog: String, dynamoDB: DynamoDB, catalogPt: ProvisionedThroughput)
  extends ContentDataStore with SchemaValidation with LazyLogging {
  import DynamoDBDataStore._

  private val CATALOG_TABLE = catalog
  private val catalogTable: Table = getOrCreateCatalogTable(dynamoDB, CATALOG_TABLE, catalogPt.getReadCapacityUnits, catalogPt.getWriteCapacityUnits)

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft = Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { DynamoDBDataStore.getSchema(entry, catalogTable) }
    val table = dynamoDB.getTable(makeTableName(catalog, sft.getTypeName))
    new DynamoDBFeatureStore(entry, sft, table)
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    validatingCreateSchema(featureType, createSchemaImpl)
  }

  private def createSchemaImpl(featureType: SimpleFeatureType): Unit = {
    val name = featureType.getTypeName
    val rcu: Long = featureType.userData[Long](rcuKey).getOrElse(1L)
    val wcu: Long = featureType.userData[Long](wcuKey).getOrElse(1L)

    val tableDesc =
      new CreateTableRequest()
        .withTableName(makeTableName(catalog, name))
        .withKeySchema(featureKeySchema)
        .withAttributeDefinitions(featureAttributeDescriptions)
        .withProvisionedThroughput(new ProvisionedThroughput(rcu, wcu))

    // create the table
    val res = dynamoDB.createTable(tableDesc)
    res.waitForActive()

    // write the meta-data
    val metaEntry = createDDMMetaDataItem(name, featureType)
    catalogTable.putItem(metaEntry)
  }

  override def createTypeNames(): util.List[Name] = {
    catalogTable.scan().iterator().map { i => new NameImpl(i.getString(catalogKeyHash)) }.toList
  }

  override def createContentState(entry: ContentEntry): ContentState = {
    val sftTable = dynamoDB.getTable(makeTableName(catalog, entry.getTypeName))
    new DynamoDBContentState(entry, catalogTable, sftTable)
  }

  override def dispose(): Unit = if (dynamoDB != null) dynamoDB.shutdown()

  def updateProvisionedThroughput(name: String, pt: ProvisionedThroughput): Unit = {
    val tableName = makeTableName(catalog, name)
    logger.info("Attempting to Modify provisioned throughput for {}", tableName)
    try {
      val table = dynamoDB.getTable(tableName)
      table.updateTable(pt)
      table.waitForActive()
      logger.info(s"Updated table: $tableName to have ProvisionedThroughput: ${pt.toString}")
    } catch {
      case NonFatal(e) => logger.error(s"Unable to update table: $tableName", e)
    }
  }

}

object DynamoDBDataStore {
  val rcuKey = "geomesa.dynamodb.rcu"
  val wcuKey = "geomesa.dynamodb.wcu"

  val serId  = "ser"

  val geomesaKeyHash  = "dtgandz2"
  val geomesaKeyRange = "z3andID"

  val featureKeySchema = List(
      new KeySchemaElement().withAttributeName(geomesaKeyHash).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(geomesaKeyRange).withKeyType(KeyType.RANGE)
    )

  val featureAttributeDescriptions = List(
    new AttributeDefinition(geomesaKeyHash,  ScalarAttributeType.B),
    new AttributeDefinition(geomesaKeyRange, ScalarAttributeType.B)
  )

  val catalogKeyHash = "feature"
  val catalogSftAttributeName = "sft"

  val catalogKeySchema = List(new KeySchemaElement(catalogKeyHash, KeyType.HASH))
  val catalogAttributeDescriptions =  List(new AttributeDefinition(catalogKeyHash, ScalarAttributeType.S))

  def makeTableName(catalog: String, name: String): String = s"${catalog}_${name}_z3"

  def getSchema(entry: ContentEntry, catalogTable: Table): SimpleFeatureType  = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

  private def getOrCreateCatalogTable(dynamoDB: DynamoDB, table: String, rcus: Long = 1L, wcus: Long = 1L) = {
    val tables = dynamoDB.listTables().iterator()
    val ret = tables
      .find(_.getTableName == table)
      .getOrElse(
        dynamoDB.createTable(
          table,
          catalogKeySchema,
          catalogAttributeDescriptions,
          new ProvisionedThroughput(rcus, wcus)
        )
      )
    ret.waitForActive()
    ret
  }

  private def createDDMMetaDataItem(name: String, featureType: SimpleFeatureType): Item = {
    new Item().withPrimaryKey(catalogKeyHash, name).withString(catalogSftAttributeName, SimpleFeatureTypes.encodeType(featureType))
  }

  def apply(catalog: String, dynamoDB: DynamoDB, rcus: Long, wcus: Long): DynamoDBDataStore = {
    val pt = new ProvisionedThroughput(rcus, wcus)
    new DynamoDBDataStore(catalog, dynamoDB, pt)
  }

}

object DynamoDBPrimaryKey extends DynamoPrimaryKey