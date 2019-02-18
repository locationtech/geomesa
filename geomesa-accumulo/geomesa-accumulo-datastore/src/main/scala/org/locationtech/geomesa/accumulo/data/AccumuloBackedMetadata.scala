/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.index.metadata.{CachedLazyBinaryMetadata, GeoMesaMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging}

import scala.collection.JavaConversions._

class AccumuloBackedMetadata[T](val connector: Connector, val catalog: String, val serializer: MetadataSerializer[T])
    extends GeoMesaMetadata[T] with CachedLazyBinaryMetadata[T] {

  protected val config: BatchWriterConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(2)

  private val empty = new Text()

  private var writerCreated = false

  lazy private val writer = synchronized {
    if (writerCreated) {
      throw new IllegalStateException("Trying to write using a closed instance")
    }
    writerCreated = true
    connector.createBatchWriter(catalog, config)
  }

  override protected def checkIfTableExists: Boolean = connector.tableOperations().exists(catalog)

  override protected def createTable(): Unit = AccumuloVersion.createTableIfNeeded(connector, catalog)

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (k, v) =>
      val m = new Mutation(k)
      m.put(empty, empty, new Value(v))
      writer.addMutation(m)
    }
    writer.flush()
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    val ranges = rows.map(r => Range.exact(new Text(r)))
    val deleter = connector.createBatchDeleter(catalog, AccumuloVersion.getEmptyAuths, 1, config)
    deleter.setRanges(ranges)
    deleter.delete()
    deleter.close()
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val scanner = connector.createScanner(catalog, AccumuloVersion.getEmptyAuths)
    scanner.setRange(Range.exact(new Text(row)))
    try {
      val iter = scanner.iterator
      if (iter.hasNext) {
        Some(iter.next.getValue.get)
      } else {
        None
      }
    } finally {
      scanner.close()
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    // ensure we don't scan any single-row encoded values
    val range = prefix.map(p => Range.prefix(new Text(p))).getOrElse(new Range("", "~"))
    val scanner = connector.createScanner(catalog, AccumuloVersion.getEmptyAuths)
    scanner.setRange(range)
    CloseableIterator(scanner.iterator.map(r => (r.getKey.getRow.copyBytes, r.getValue.get)), scanner.close())
  }

  override def close(): Unit = synchronized {
    if (writerCreated) {
      CloseWithLogging(writer)
    } else {
      writerCreated = true // indicate that we've closed and don't want to open any more resources
    }
  }
}

/**
  * Old single row metadata kept around for back-compatibility
  */
class SingleRowAccumuloMetadata[T](metadata: AccumuloBackedMetadata[T]) {

  // if the table doesn't exist, we assume that we don't ever need to check it for old-encoded rows
  private val tableExists = metadata.connector.tableOperations().exists(metadata.catalog)

  def getFeatureTypes: Array[String] = {
    if (!tableExists) { Array.empty } else {
      val scanner = metadata.connector.createScanner(metadata.catalog, AccumuloVersion.getEmptyAuths)
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

  /**
    * Migrate a table from the old single-row metadata to the new
    *
    * @param typeName simple feature type name
    */
  def migrate(typeName: String): Unit = {
    if (tableExists) {
      val scanner = metadata.connector.createScanner(metadata.catalog, AccumuloVersion.getEmptyAuths)
      val writer = metadata.connector.createBatchWriter(metadata.catalog, GeoMesaBatchWriterConfig())
      try {
        scanner.setRange(SingleRowAccumuloMetadata.getRange(typeName))
        scanner.iterator.foreach { entry =>
          val key = entry.getKey.getColumnFamily.toString
          metadata.insert(typeName, key, metadata.serializer.deserialize(typeName, entry.getValue.get))
          // delete for the old entry
          val delete = new Mutation(entry.getKey.getRow)
          delete.putDelete(entry.getKey.getColumnFamily, entry.getKey.getColumnQualifier)
          writer.addMutation(delete)
        }
      } finally {
        CloseQuietly(scanner)
        if (writer != null) {
          CloseQuietly(writer)
        }
      }
    }
  }
}

object SingleRowAccumuloMetadata {

  private val METADATA_TAG = "~METADATA"
  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  def getRange(typeName: String): Range = new Range(METADATA_TAG + "_" + typeName)

  def getTypeName(row: Text): String = {
    val MetadataRowKeyRegex(typeName) = row.toString
    typeName
  }
}
