/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable
import java.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{AbstractDataStoreFactory, DataStore, Transaction}
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class HBaseDataStore(conn: Connection,
                     catalog: String,
                     namespace: String = null) extends ContentDataStore {

  import HBaseDataStore._

  private val CATALOG_TABLE = TableName.valueOf(catalog)
  private val catalogTable = getOrCreateCatalogTable(conn, CATALOG_TABLE)
  private val schemaCQ = Bytes.toBytes("schema")

  setNamespaceURI(namespace)

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val name = featureType.getTypeName

    // create the z3 index
    val z3TableName = getZ3TableName(featureType)
    if (!conn.getAdmin.tableExists(z3TableName)) {
      val desc = new HTableDescriptor(z3TableName)
      Seq(DATA_FAMILY_NAME).foreach { f => desc.addFamily(new HColumnDescriptor(f)) }
      conn.getAdmin.createTable(desc)
    }

    // write the meta-data
    val row = Bytes.toBytes(name)
    val encodedSFT = Bytes.toBytes(SimpleFeatureTypes.encodeType(featureType))

    val schema = new Put(row).addColumn(META_FAMILY_NAME, schemaCQ, encodedSFT)
    catalogTable.put(schema)
  }

  def getZ3TableName(sft: SimpleFeatureType) = TableName.valueOf(s"${sft.getTypeName}_z3")
  def getZ3Table(sft: SimpleFeatureType) = conn.getTable(getZ3TableName(sft))

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft =
      Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { getSchema(entry) }
    new HBaseFeatureSource(entry, null, sft)
  }

  def getSchema(entry: ContentEntry) = {
    val get = new Get(Bytes.toBytes(entry.getTypeName)).addColumn(META_FAMILY_NAME, schemaCQ)
    val result = catalogTable.get(get)
    val spec = Bytes.toString(result.getValue(META_FAMILY_NAME, schemaCQ))
    SimpleFeatureTypes.createType(entry.getTypeName, spec)
  }

  override def createTypeNames(): util.List[Name] = {
    // read types from catalog
    val scan = new Scan().addColumn(META_FAMILY_NAME, schemaCQ)
    val scanner = catalogTable.getScanner(scan)
    scanner.iterator().map { r => Bytes.toString(r.getRow) }.map { n => new NameImpl(getNamespaceURI, n) }.toList
  }
}

class HBaseDataStoreFactory extends AbstractDataStoreFactory {

  import HBaseDataStore._

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, java.lang.Boolean.TRUE)

  override def createDataStore(map: util.Map[String, Serializable]): DataStore = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    val catalog = BIGTABLENAMEPARAM.lookUp(map).asInstanceOf[String]
    val namespace =  NAMESPACE_PARAM.lookUp(map).asInstanceOf[String]
    new HBaseDataStore(conn, catalog, namespace)
  }

  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    val catalog = BIGTABLENAMEPARAM.lookUp(map).asInstanceOf[String]
    val namespace =  NAMESPACE_PARAM.lookUp(map).asInstanceOf[String]
    getOrCreateCatalogTable(conn, TableName.valueOf(catalog))
    new HBaseDataStore(conn, catalog, namespace)
  }

  override def getDisplayName: String = "HBase (GeoMesa)"

  override def getDescription: String = "GeoMesa HBase DataStore"

  override def getParametersInfo: Array[Param] = Array(BIGTABLENAMEPARAM, NAMESPACE_PARAM)
}

object HBaseDataStore {

  val BIGTABLENAMEPARAM = new Param("bigtable.table.name", classOf[String], "Table name", true)
  val NAMESPACE_PARAM    = new Param("namespace", classOf[String], "Namespace", false)

  val META_FAMILY_NAME = Bytes.toBytes("M")
  val META_FAMILY = new HColumnDescriptor(META_FAMILY_NAME)
  val DATA_FAMILY_NAME = Bytes.toBytes("D")

  def getOrCreateCatalogTable(conn: Connection, CATALOG_TABLE: TableName) = {
    val admin = conn.getAdmin
    if (!admin.tableExists(CATALOG_TABLE)) {
      val desc = new HTableDescriptor(CATALOG_TABLE)
      desc.addFamily(META_FAMILY)
      admin.createTable(desc)
    }
    conn.getTable(CATALOG_TABLE)
  }

}