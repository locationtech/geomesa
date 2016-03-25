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
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentState}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.dynamo.core.SchemaValidation
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class DynamoDBDataStore(val catalog: String, dynamoDB: DynamoDB, catalogPt: ProvisionedThroughput)
  extends ContentDataStore with SchemaValidation with LazyLogging {

  import DynamoDBDataStore._

  private val CATALOG_TABLE = catalog
  private val catalogTable: Table = {
    getOrCreateCatalogTable(
      dynamoDB,
      CATALOG_TABLE,
      catalogPt.getReadCapacityUnits,
      catalogPt.getWriteCapacityUnits)
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    new DynamoDBFeatureStore(entry)
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
        .withTableName(makeSFTTableName(catalog, name))
        .withKeySchema(featureKeySchema)
        .withAttributeDefinitions(featureAttributeDescriptions)
        .withProvisionedThroughput(new ProvisionedThroughput(rcu, wcu))

    // create the table
    val res = dynamoDB.createTable(tableDesc)
    res.waitForActive()

    // write the meta-data
    val metaEntry = createMetaDataItem(name, featureType)
    catalogTable.putItem(metaEntry)
  }

  override def createTypeNames(): util.List[Name] = {
    getTypes.map(new NameImpl(_))
  }

  private def getTypes: List[String] = {
    catalogTable.scan().iterator().map(_.getString(catalogKeyHash)).toList
  }

  override def createContentState(entry: ContentEntry): ContentState = {
    val sftTable = dynamoDB.getTable(makeSFTTableName(catalog, entry.getTypeName))
    new DynamoDBContentState(entry, catalogTable, sftTable)
  }

  override def dispose(): Unit = if (dynamoDB != null) dynamoDB.shutdown()

  private def applyToSFTTableSafely[T](nameSFT: String)(func: => T) = {
    if (getTypes.contains(nameSFT)) {
      func
    } else {
      throw new Exception(s"No such SimpleFeatureType: $nameSFT in GeoMesa Catalog: $catalog")
    }
  }

  def getProvisionedThroughputSFT(nameSFT: String): ProvisionedThroughputDescription = {
    applyToSFTTableSafely(nameSFT){
      val tableName = makeSFTTableName(catalog, nameSFT)
      dynamoDB.getTable(tableName).getDescription.getProvisionedThroughput
    }
  }

  def setProvisionedThroughputSFT(nameSFT: String, pt: ProvisionedThroughput): Unit = {
    applyToSFTTableSafely(nameSFT){
      val tableName = makeSFTTableName(catalog, nameSFT)
      val table = dynamoDB.getTable(tableName)
      setPTforSFT(table, pt)
    }
  }

  private def setPTforSFT(table: Table, pt: ProvisionedThroughput): Unit = {
    table.updateTable(pt)
  }

  private def setPTforSFT(table: Table, rcus: Long, wcus: Long): Unit = {
    setPTforSFT(table, new ProvisionedThroughput(rcus, wcus))
  }

  private def setPTforSFT(nameSFT: String, rcus: Option[Long], wcus: Option[Long]): Unit = {
    val tableName = makeSFTTableName(catalog, nameSFT)
    val table = dynamoDB.getTable(tableName)
    val currentPT = table.getDescription.getProvisionedThroughput
    val r: Long = rcus.getOrElse(currentPT.getReadCapacityUnits)
    val w: Long = wcus.getOrElse(currentPT.getWriteCapacityUnits)
    setPTforSFT(table, r, w)
  }

  def setReadsSFT(nameSFT: String, rcus: Long): Unit = {
    applyToSFTTableSafely(nameSFT){
      setPTforSFT(nameSFT, Some(rcus), None)
    }
  }

  def setWritesSFT(nameSFT: String, wcus: Long): Unit = {
    applyToSFTTableSafely(nameSFT){
      setPTforSFT(nameSFT, None, Some(wcus))
    }
  }

  def setCatalogProvisionedThroughput(pt: ProvisionedThroughput): Unit = {
    catalogTable.updateTable(pt)
  }

  def setCatalogProvisionedThroughput(rcus: Long, wcus: Long): Unit = {
    setCatalogProvisionedThroughput(new ProvisionedThroughput(rcus, wcus))
  }

  def setCatalogReads(rcus: Long): Unit = {
    val currentPT = catalogTable.getDescription.getProvisionedThroughput
    setCatalogProvisionedThroughput(rcus, currentPT.getWriteCapacityUnits)
  }

  def setCatalogWrites(wcus: Long): Unit = {
    val currentPT = catalogTable.getDescription.getProvisionedThroughput
    setCatalogProvisionedThroughput(currentPT.getReadCapacityUnits, wcus)
  }

  def getCatalogProvisionedThroughput: ProvisionedThroughputDescription = {
    getCatalogDescription.getProvisionedThroughput
  }

  def getCatalogDescription: TableDescription = {
    catalogTable.getDescription
  }

}

object DynamoDBDataStore {
  val rcuKey = "geomesa.dynamodb.rcu"
  val wcuKey = "geomesa.dynamodb.wcu"

  val serId = "ser"

  val geomesaKeyHash = "dtgandz2"
  val geomesaKeyRange = "z3andID"

  val featureKeySchema = List(
    new KeySchemaElement().withAttributeName(geomesaKeyHash).withKeyType(KeyType.HASH),
    new KeySchemaElement().withAttributeName(geomesaKeyRange).withKeyType(KeyType.RANGE)
  )

  val featureAttributeDescriptions = List(
    new AttributeDefinition(geomesaKeyHash, ScalarAttributeType.B),
    new AttributeDefinition(geomesaKeyRange, ScalarAttributeType.B)
  )

  val catalogKeyHash = "feature"
  val catalogSftAttributeName = "sft"

  val catalogKeySchema = List(new KeySchemaElement(catalogKeyHash, KeyType.HASH))
  val catalogAttributeDescriptions = List(new AttributeDefinition(catalogKeyHash, ScalarAttributeType.S))

  def makeSFTTableName(catalog: String, name: String): String = s"${catalog}_${name}_z3"

  def getSchema(entry: ContentEntry, catalogTable: Table): SimpleFeatureType = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

  def apply(catalog: String, dynamoDB: DynamoDB, rcus: Long, wcus: Long): DynamoDBDataStore = {
    val pt = new ProvisionedThroughput(rcus, wcus)
    new DynamoDBDataStore(catalog, dynamoDB, pt)
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

  private def createMetaDataItem(name: String, featureType: SimpleFeatureType): Item = {
    new Item()
      .withPrimaryKey(catalogKeyHash, name)
      .withString(catalogSftAttributeName, SimpleFeatureTypes.encodeType(featureType))
  }

}
