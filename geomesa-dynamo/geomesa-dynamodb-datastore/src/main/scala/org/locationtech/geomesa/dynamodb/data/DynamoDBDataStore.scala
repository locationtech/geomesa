/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.lang.{Long => JLong}
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
    val tableName = featureType.getName
    val rcu: Long = featureType.userData[JLong](RCU_Key).getOrElse(Long.box(1)).toLong
    val wcu: Long = featureType.userData[JLong](WCU_Key).getOrElse(Long.box(1)).toLong

    val tableDesc =
      new CreateTableRequest()
        .withTableName(makeSFTTableName(catalog, tableName.toString))
        .withKeySchema(featureKeySchema)
        .withAttributeDefinitions(featureAttributeDescriptions)
        .withProvisionedThroughput(new ProvisionedThroughput(rcu, wcu))

    // create the table
    val res = dynamoDB.createTable(tableDesc)
    res.waitForActive()

    // write the meta-data
    val metaEntry = createMetaDataItem(tableName.getLocalPart, tableName.getNamespaceURI, featureType)
    catalogTable.putItem(metaEntry)
  }

  override def createTypeNames(): util.List[Name] = {
    catalogTable.scan().iterator().map(metaDataItemToSFTName).toList
  }

  private def getTypes: List[String] = {
    createTypeNames().map(_.toString).toList
  }

  override def createContentState(entry: ContentEntry): ContentState = {
    val sftTable = dynamoDB.getTable(makeSFTTableName(catalog, entry.getName.toString))
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
      dynamoDB.getTable(tableName).describe().getProvisionedThroughput
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

  def setProvisionedThroughputCatalog(pt: ProvisionedThroughput): Unit = {
    catalogTable.updateTable(pt)
  }

  def setProvisionedThroughputCatalog(rcus: Long, wcus: Long): Unit = {
    setProvisionedThroughputCatalog(new ProvisionedThroughput(rcus, wcus))
  }

  def setReadsCatalog(rcus: Long): Unit = {
    val currentPT = catalogTable.getDescription.getProvisionedThroughput
    setProvisionedThroughputCatalog(rcus, currentPT.getWriteCapacityUnits)
  }

  def setWritesCatalog(wcus: Long): Unit = {
    val currentPT = catalogTable.getDescription.getProvisionedThroughput
    setProvisionedThroughputCatalog(currentPT.getReadCapacityUnits, wcus)
  }

  def getProvisionedThroughputCatalog: ProvisionedThroughputDescription = {
    getDescriptionCatalog.getProvisionedThroughput
  }

  def getDescriptionCatalog: TableDescription = {
    catalogTable.describe()
  }

}

object DynamoDBDataStore {
  val namespaceGlobal = "global"

  val RCU_Key = "geomesa.dynamodb.sft.rcu"
  val WCU_Key = "geomesa.dynamodb.sft.wcu"

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
  val catalogNameSpaceAttributeName = "ns"
  val catalogSftAttributeName = "sft"

  val catalogKeySchema = List(
    new KeySchemaElement(catalogKeyHash, KeyType.HASH),
    new KeySchemaElement(catalogNameSpaceAttributeName, KeyType.RANGE)
  )
  val catalogAttributeDescriptions = List(
    new AttributeDefinition(catalogKeyHash, ScalarAttributeType.S),
    new AttributeDefinition(catalogNameSpaceAttributeName, ScalarAttributeType.S)
  )

  def makeSFTTableName(catalog: String, nameSFT: String): String = {
    val (ns, name) = SimpleFeatureTypes.buildTypeName(nameSFT)
    val namespace = Option(ns).getOrElse(namespaceGlobal)
    s"${catalog}_${namespace}_${name}_z3"
  }

  def getSchema(entry: ContentEntry, catalogTable: Table): SimpleFeatureType = {
    val name = entry.getName
    val item: Item = catalogTable.getItem(
      catalogKeyHash, name.getLocalPart,
      catalogNameSpaceAttributeName, name.getNamespaceURI
    )
    val sft: String = item.getString(catalogSftAttributeName)
    SimpleFeatureTypes.createType(name.toString, sft)
  }

  def apply(catalog: String, dynamoDB: DynamoDB, catalog_rcus: Long, catalog_wcus: Long): DynamoDBDataStore = {
    val catalog_pt = new ProvisionedThroughput(catalog_rcus, catalog_wcus)
    new DynamoDBDataStore(catalog, dynamoDB, catalog_pt)
  }

  def apply(catalog: String, dynamoDB: DynamoDB, catalog_pt: ProvisionedThroughput): DynamoDBDataStore = {
    new DynamoDBDataStore(catalog, dynamoDB, catalog_pt)
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

  private def createMetaDataItem(name: String, namespace: String, featureType: SimpleFeatureType): Item = {
    new Item()
      .withPrimaryKey(catalogKeyHash, name, catalogNameSpaceAttributeName, namespace)
      .withString(catalogSftAttributeName, SimpleFeatureTypes.encodeType(featureType))
  }

  private def metaDataItemToSFTName(i: Item): NameImpl = {
    new NameImpl(i.getString(catalogNameSpaceAttributeName), i.getString(catalogKeyHash))
  }

}
