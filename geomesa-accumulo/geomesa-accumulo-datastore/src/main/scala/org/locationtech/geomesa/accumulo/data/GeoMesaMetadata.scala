/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{Connector, Scanner}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.AccumuloBackedMetadata._
import org.locationtech.geomesa.accumulo.util.{EmptyScanner, GeoMesaBatchWriterConfig}

import scala.collection.JavaConversions._

/**
 * GeoMesa Metadata/Catalog abstraction using key/value String pairs storing
 * them on a per-typeName basis
 */
trait GeoMesaMetadata[T] {

  /**
   * Returns existing simple feature types
   *
   * @return simple feature type names
   */
  def getFeatureTypes: Array[String]

  /**
   * Insert a value - any existing value under the given key will be overwritten
   *
   * @param typeName simple feature type name
   * @param key key
   * @param value value
   */
  def insert(typeName: String, key: String, value: T): Unit

  /**
   * Insert multiple values at once - may be more efficient than single inserts
   *
   * @param typeName simple feature type name
   * @param kvPairs key/values
   */
  def insert(typeName: String, kvPairs: Map[String, T]): Unit

  /**
    * Delete a key
    *
    * @param typeName simple feature type name
    * @param key key
    */
  def remove(typeName: String, key: String): Unit

  /**
   * Reads a value
   *
   * @param typeName simple feature type name
   * @param key key
   * @param cache may return a cached value if true, otherwise may use a slower lookup
   * @return value, if present
   */
  def read(typeName: String, key: String, cache: Boolean = true): Option[T]

  /**
   * Reads a value. Throws an exception if value is missing
   *
   * @param typeName simple feature type name
   * @param key key
   * @return value
   */
  def readRequired(typeName: String, key: String): T =
    read(typeName, key).getOrElse {
      throw new RuntimeException(s"Unable to find required metadata property for $typeName:$key")
    }

  /**
   * Deletes all values associated with a given feature type
   *
   * @param typeName simple feature type name
   */
  def delete(typeName: String)
}

object GeoMesaMetadata {

  // Metadata keys
  val ATTRIBUTES_KEY      = "attributes"
  val VERSION_KEY         = "version"
  val SCHEMA_ID_KEY       = "id"

  val ATTR_IDX_TABLE_KEY  = "tables.idx.attr.name"
  val RECORD_TABLE_KEY    = "tables.record.name"
  val Z2_TABLE_KEY        = "tables.z2.name"
  val Z3_TABLE_KEY        = "tables.z3.name"
  val QUERIES_TABLE_KEY   = "tables.queries.name"
  @deprecated
  val ST_IDX_TABLE_KEY    = "tables.idx.st.name"

  val STATS_GENERATION_KEY   = "stats-date"
  val STATS_INTERVAL_KEY     = "stats-interval"
}

trait HasGeoMesaMetadata[T] {
  def metadata: GeoMesaMetadata[T]
}

trait MetadataSerializer[T] {
  def serialize(typeName: String, key: String, value: T): Array[Byte]
  def deserialize(typeName: String, key: String, value: Array[Byte]): T
}

object MetadataStringSerializer extends MetadataSerializer[String] {
  def serialize(typeName: String, key: String, value: String): Array[Byte] = {
    if (value == null) Array.empty else value.getBytes(StandardCharsets.UTF_8)
  }
  def deserialize(typeName: String, key: String, value: Array[Byte]): String = {
    if (value.isEmpty) null else new String(value, StandardCharsets.UTF_8)
  }
}

class AccumuloBackedMetadata[T](connector: Connector, catalogTable: String, serializer: MetadataSerializer[T])
    extends GeoMesaMetadata[T] with LazyLogging {

  import GeoMesaMetadata._

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  private val metaDataCache =
    CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(key: (String, String)): Option[T] = scanEntry(key._1, key._2)
      }
    )

  private val metadataBWConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  // warning: only access in a synchronized fashion
  private var tableExists = connector.tableOperations().exists(catalogTable)

  /**
   * Scans metadata rows and pulls out the different feature types in the table
   *
   * @return
   */
  override def getFeatureTypes: Array[String] = {
    val scanner = createScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just one cf so we only get 1 hit per feature
    // use attributes as it's the only thing that's been there through all geomesa versions
    scanner.fetchColumnFamily(new Text(ATTRIBUTES_KEY))
    try {
      scanner.map(kv => getTypeNameFromMetadataRowKey(kv.getKey.getRow.toString)).toArray
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
    val insert = new Mutation(getMetadataRowKey(typeName))
    kvPairs.foreach { case (k,v) =>
      val value = new Value(serializer.serialize(typeName, k, v))
      insert.put(new Text(k), EMPTY_COLQ, value)
    }
    val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
    writer.addMutation(insert)
    writer.close()
    // also pre-fetch into the cache
    val toCache = kvPairs.map(kv => (typeName, kv._1) -> Option(kv._2))
    metaDataCache.putAll(toCache)
  }

  override def remove(typeName: String, key: String): Unit = {
    if (synchronized(tableExists)) {
      val delete = new Mutation(getMetadataRowKey(typeName))
      delete.putDelete(new Text(key), EMPTY_COLQ)
      val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
      writer.addMutation(delete)
      writer.close()
      // also remove from the cache
      metaDataCache.invalidate((typeName, key))
    } else {
      logger.warn(s"Trying to delete '$typeName:$key' from '$catalogTable' but table does not exist")
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
      val range = new Range(getMetadataRowKey(typeName))
      val deleter = connector.createBatchDeleter(catalogTable, AccumuloVersion.getEmptyAuths, 1, metadataBWConfig)
      deleter.setRanges(List(range))
      deleter.delete()
      deleter.close()
    } else {
      logger.warn(s"Trying to delete type '$typeName' from '$catalogTable' but table does not exist")
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
    val scanner = createScanner
    scanner.setRange(new Range(getMetadataRowKey(typeName)))
    scanner.fetchColumn(new Text(key), EMPTY_COLQ)
    try {
      val entries = scanner.iterator
      if (!entries.hasNext) { None } else {
        Option(serializer.deserialize(typeName, key, entries.next.getValue.get()))
      }
    } finally {
      scanner.close()
    }
  }

  /**
   * Create an Accumulo Scanner to the Catalog table to query Metadata for this store
   */
  private def createScanner: Scanner =
    if (synchronized(tableExists)) {
      connector.createScanner(catalogTable, AccumuloVersion.getEmptyAuths)
    } else {
      EmptyScanner
    }

  private def ensureTableExists(): Unit = synchronized {
    if (!tableExists) {
      AccumuloVersion.ensureTableExists(connector, catalogTable)
      tableExists = true
    }
  }
}

object AccumuloBackedMetadata {

  private val METADATA_TAG     = "~METADATA"
  private val METADATA_TAG_END = s"$METADATA_TAG~~"

  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  /**
   * Reads the feature name from a given metadata row key
   *
   * @param rowKey row from accumulo
   * @return simple feature type name
   */
  def getTypeNameFromMetadataRowKey(rowKey: String): String = {
    val MetadataRowKeyRegex(typeName) = rowKey
    typeName
  }

  /**
   * Creates the row id for a metadata entry
   *
   * @param typeName simple feature type name
   * @return
   */
  private def getMetadataRowKey(typeName: String) = new Text(METADATA_TAG + "_" + typeName)
}
