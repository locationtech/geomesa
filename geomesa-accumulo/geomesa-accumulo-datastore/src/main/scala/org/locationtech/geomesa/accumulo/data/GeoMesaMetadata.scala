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

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchWriter, Connector, Scanner}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
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
    * Invalidates any cached value for the given key
    *
    * @param typeName simple feature type name
    * @param key key
    */
  def invalidateCache(typeName: String, key: String): Unit

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

abstract class AccumuloBackedMetadata[T](connector: Connector,
                                         catalogTable: String,
                                         serializer: MetadataSerializer[T],
                                         rowEncoding: AccumuloMetadataRow)
    extends GeoMesaMetadata[T] with LazyLogging {

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  protected val metaDataCache =
    Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(key: (String, String)): Option[T] = scanEntry(key._1, key._2)
      }
    )

  protected val metadataBWConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  // warning: only access in a synchronized fashion
  private var tableExists = connector.tableOperations().exists(catalogTable)

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    if (!cache) {
      metaDataCache.invalidate((typeName, key))
    }
    metaDataCache.get((typeName, key))
  }

  override def insert(typeName: String, key: String, value: T): Unit = insert(typeName, Map(key -> value))

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    ensureTableExists()
    val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
    kvPairs.foreach { case (k, v) =>
      val insert = new Mutation(rowEncoding.getRowKey(typeName, k))
      val value = new Value(serializer.serialize(typeName, k, v))
      val (cf, cq) = rowEncoding.getColumns(k)
      insert.put(cf, cq, value)
      writer.addMutation(insert)
      // also pre-fetch into the cache
      // note: don't use putAll, it breaks guava 11 compatibility
      metaDataCache.put((typeName, k), Option(v))
    }
    writer.close()
  }

  override def invalidateCache(typeName: String, key: String): Unit = metaDataCache.invalidate((typeName, key))

  override def remove(typeName: String, key: String): Unit = {
    if (synchronized(tableExists)) {
      val delete = new Mutation(rowEncoding.getRowKey(typeName, key))
      val (cf, cq) = rowEncoding.getColumns(key)
      delete.putDelete(cf, cq)
      val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
      writer.addMutation(delete)
      writer.close()
      // also remove from the cache
      metaDataCache.invalidate((typeName, key))
    } else {
      logger.debug(s"Trying to delete '$typeName:$key' from '$catalogTable' but table does not exist")
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
      val range = rowEncoding.getRange(typeName)
      val deleter = connector.createBatchDeleter(catalogTable, AccumuloVersion.getEmptyAuths, 1, metadataBWConfig)
      deleter.setRanges(List(range))
      deleter.delete()
      deleter.close()
    } else {
      logger.debug(s"Trying to delete type '$typeName' from '$catalogTable' but table does not exist")
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
    scanner.setRange(Range.exact(rowEncoding.getRowKey(typeName, key)))
    val (cf, cq) = rowEncoding.getColumns(key)
    scanner.fetchColumn(cf, cq)
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
  protected def createScanner: Scanner =
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


trait AccumuloMetadataRow {

  /**
    * Gets the row key for a given entry
    *
    * @param typeName simple feature type name
    * @param key entry key
    * @return
    */
  def getRowKey(typeName: String, key: String): Text

  /**
    * Gets the columns used to store a given entry
    *
    * @param key entry key
    * @return
    */
  def getColumns(key: String): (Text, Text)

  /**
    * Gets a range that covers every entry under the given feature type
    *
    * @param typeName simple feature type name
    * @return
    */
  def getRange(typeName: String): Range

  /**
    * Gets the schema name from a row key
    *
    * @param row row
    * @return
    */
  def getTypeName(row: Text): String
}

class SingleRowAccumuloMetadata[T](connector: Connector, catalogTable: String, serializer: MetadataSerializer[T])
    extends AccumuloBackedMetadata[T](connector, catalogTable, serializer, SingleRowAccumuloMetadata) {

  import SingleRowAccumuloMetadata._

  override def getFeatureTypes: Array[String] = {
    val scanner = createScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just one cf so we only get 1 hit per feature
    // use attributes as it's the only thing that's been there through all geomesa versions
    scanner.fetchColumnFamily(new Text(GeoMesaMetadata.ATTRIBUTES_KEY))
    try {
      scanner.map(e => SingleRowAccumuloMetadata.getTypeName(e.getKey.getRow)).toArray
    } finally {
      scanner.close()
    }
  }
}

object SingleRowAccumuloMetadata extends AccumuloMetadataRow {

  private val METADATA_TAG     = "~METADATA"
  private val METADATA_TAG_END = s"$METADATA_TAG~~"
  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  override def getRowKey(typeName: String, key: String): Text = new Text(METADATA_TAG + "_" + typeName)

  override def getColumns(key: String): (Text, Text) = (new Text(key), EMPTY_COLQ)

  override def getRange(typeName: String): Range = new Range(METADATA_TAG + "_" + typeName)

  override def getTypeName(row: Text): String = {
    val MetadataRowKeyRegex(typeName) = row.toString
    typeName
  }
}

class MultiRowAccumuloMetadata[T](connector: Connector, catalogTable: String, serializer: MetadataSerializer[T])
    extends AccumuloBackedMetadata[T](connector, catalogTable, serializer, MultiRowAccumuloMetadata) {

  override def getFeatureTypes: Array[String] = {
    val scanner = createScanner
    scanner.setRange(new Range("", "~"))
    try {
      scanner.map(e => MultiRowAccumuloMetadata.getTypeName(e.getKey.getRow)).toArray.distinct
    } finally {
      scanner.close()
    }
  }

  /**
    * Migrate a table from the old single-row metadata to the new
    *
    * @param typeName simple feature type name
    */
  def migrate(typeName: String): Unit = {
    val scanner = createScanner
    var writer: BatchWriter = null
    try {
      scanner.setRange(SingleRowAccumuloMetadata.getRange(typeName))
      val iterator = scanner.iterator
      if (iterator.hasNext) {
        writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
      }
      iterator.foreach { entry =>
        val key = entry.getKey.getColumnFamily.toString
        // insert for the mutli-table format
        val insert = new Mutation(MultiRowAccumuloMetadata.getRowKey(typeName, key))
        val (cf, cq) = MultiRowAccumuloMetadata.getColumns(key)
        insert.put(cf, cq, entry.getValue)
        // delete for the old entry
        val delete = new Mutation(entry.getKey.getRow)
        delete.putDelete(entry.getKey.getColumnFamily, entry.getKey.getColumnQualifier)

        writer.addMutations(Seq(insert, delete))
      }
    } finally {
      scanner.close()
      if (writer != null) {
        writer.close()
      }
    }
    metaDataCache.invalidateAll()
  }
}

object MultiRowAccumuloMetadata extends AccumuloMetadataRow {

  override def getRowKey(typeName: String, key: String): Text = new Text(s"$typeName~$key")

  override def getColumns(key: String): (Text, Text) = (EMPTY_TEXT, EMPTY_TEXT)

  override def getRange(typeName: String): Range = Range.prefix(s"$typeName~")

  override def getTypeName(row: Text): String = Text.decode(row.getBytes, 0, row.find("~"))
}
