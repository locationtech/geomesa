/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchWriter, Connector, Scanner}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.util.{EmptyScanner, GeoMesaBatchWriterConfig}
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}

import scala.collection.JavaConversions._

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
