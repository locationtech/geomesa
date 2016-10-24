/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.locationtech.geomesa.hbase.utils.EmptyScanner
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}

import scala.collection.JavaConversions._

class HBaseBackedMetadata[T](connection: Connection, catalog: TableName, serializer: MetadataSerializer[T])
    extends GeoMesaMetadata[T] with LazyLogging {

  import HBaseBackedMetadata._

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  protected val metaDataCache =
    CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(key: (String, String)): Option[T] = scanEntry(key._1, key._2)
      }
    )

  // warning: only access in a synchronized fashion
  private var tableExists = {
    val admin = connection.getAdmin
    try { admin.tableExists(catalog) } finally { admin.close() }
  }
  lazy private val table = connection.getTable(catalog)

  override def getFeatureTypes: Array[String] = {
    val scanner = scan(None)
    try {
      scanner.iterator().map(r => getTypeName(r.getRow)).toArray.distinct
    } finally {
      scanner.close()
    }
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    if (!cache) {
      metaDataCache.invalidate((typeName, key))
    }
    metaDataCache.get((typeName, key))
  }

  override def insert(typeName: String, key: String, value: T): Unit = insert(typeName, Map(key -> value))

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    ensureTableExists()

    val puts = kvPairs.map { case (k, v) =>
      // note: side effect in .map - pre-fetch into the cache
      // note: don't use putAll, it breaks guava 11 compatibility
      metaDataCache.put((typeName, k), Option(v))
      new Put(getRowKey(typeName, k)).addColumn(ColumnFamily, ColumnQualifier, serializer.serialize(typeName, k, v))
    }
    table.put(puts.toList)
  }

  override def invalidateCache(typeName: String, key: String): Unit = metaDataCache.invalidate((typeName, key))

  override def remove(typeName: String, key: String): Unit = {
    if (synchronized(tableExists)) {
      table.delete(new Delete(getRowKey(typeName, key)).addColumn(ColumnFamily, ColumnQualifier))
      // also remove from the cache
      metaDataCache.invalidate((typeName, key))
    } else {
      logger.debug(s"Trying to delete '$typeName:$key' from '$catalog' but table does not exist")
    }
  }

  /**
   * Handles deleting metadata from the catalog by using the Range obtained from the METADATA_TAG and typeName
   * and setting that as the Range to be handled and deleted by Accumulo's BatchDeleter
   *
   * @param typeName the name of the table to query and delete from
   */
  override def delete(typeName: String): Unit = {
    if (synchronized(tableExists)) {
      val prefix = Bytes.toBytes(s"$typeName~")
      val scanner = scan(Some(prefix))
      try {
        val deletes = scanner.iterator.map(r => new Delete(r.getRow)).toList
        table.delete(deletes)
      } finally {
        scanner.close()
      }
    } else {
      logger.debug(s"Trying to delete type '$typeName' from '$catalog' but table does not exist")
    }
    metaDataCache.asMap.toMap.keys.filter(_._1 == typeName).foreach(metaDataCache.invalidate)
  }

  /**
   * Reads a single key/value from the underlying table
   *
   * @param typeName simple feature type name
   * @param key key
   * @return value, if it exists
   */
  private def scanEntry(typeName: String, key: String): Option[T] = {
    for {
      result <- get(getRowKey(typeName, key))
      bytes  <- Option(result.getValue(ColumnFamily, ColumnQualifier))
      value  <- Option(serializer.deserialize(typeName, key, bytes))
    } yield {
      value
    }
  }

  protected def scan(prefix: Option[Array[Byte]]): ResultScanner =
    if (synchronized(tableExists)) {
      val scan = new Scan()
      prefix.foreach(scan.setRowPrefixFilter)
      table.getScanner(scan)
    } else {
      EmptyScanner
    }

  protected def get(row: Array[Byte]): Option[Result] =
    if (synchronized(tableExists)) {
      val result = table.get(new Get(row).addColumn(ColumnFamily, ColumnQualifier))
      if (result.isEmpty) { None } else { Some(result) }
    } else {
      None
    }

  private def ensureTableExists(): Unit = synchronized {
    if (!tableExists) {
      val admin = connection.getAdmin
      try {
        if (!admin.tableExists(catalog)) {
          val descriptor = new HTableDescriptor(catalog)
          descriptor.addFamily(ColumnFamilyDescriptor)
          admin.createTable(descriptor)
        }
      } finally {
        admin.close()
      }
      tableExists = true
    }
  }

  private def getTypeName(row: Array[Byte]): String = {
    val all = Bytes.toString(row)
    all.substring(0, all.indexOf("~"))
  }

  private def getRowKey(typeName: String, key: String): Array[Byte] = Bytes.toBytes(s"$typeName~$key")
}

object HBaseBackedMetadata {
  val ColumnFamily = Bytes.toBytes("m")
  val ColumnFamilyDescriptor = new HColumnDescriptor(ColumnFamily)
  val ColumnQualifier = Bytes.toBytes("v")
}

class HBaseStringSerializer extends MetadataSerializer[String] {
  override def serialize(typeName: String, key: String, value: String): Array[Byte] = Bytes.toBytes(value)
  override def deserialize(typeName: String, key: String, value: Array[Byte]): String = Bytes.toString(value)
}
