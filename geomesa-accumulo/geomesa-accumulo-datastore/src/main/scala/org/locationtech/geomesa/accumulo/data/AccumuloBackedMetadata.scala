/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableUtils}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, KeyValueStoreMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging}

class AccumuloBackedMetadata[T](val connector: Connector, val table: String, val serializer: MetadataSerializer[T])
    extends KeyValueStoreMetadata[T] {

  import scala.collection.JavaConverters._

  private val config = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(2)

  private val empty = new Text()

  private var writerCreated = false

  private lazy val writer = synchronized {
    if (writerCreated) {
      throw new IllegalStateException("Trying to write using a closed instance")
    }
    writerCreated = true
    connector.createBatchWriter(table, config)
  }

  override protected def checkIfTableExists: Boolean = connector.tableOperations().exists(table)

  override protected def createTable(): Unit = TableUtils.createTableIfNeeded(connector, table)

  override protected def createEmptyBackup(timestamp: String): AccumuloBackedMetadata[T] =
    new AccumuloBackedMetadata(connector, s"${table}_${timestamp}_bak", serializer)

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (k, v) =>
      val m = new Mutation(k)
      m.put(empty, empty, new Value(v))
      writer.addMutation(m)
    }
    writer.flush()
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    val ranges = rows.map(r => Range.exact(new Text(r))).asJava
    val deleter = connector.createBatchDeleter(table, Authorizations.EMPTY, 1, config)
    deleter.setRanges(ranges)
    deleter.delete()
    deleter.close()
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val scanner = connector.createScanner(table, Authorizations.EMPTY)
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
    val scanner = connector.createScanner(table, Authorizations.EMPTY)
    scanner.setRange(range)
    CloseableIterator(scanner.iterator.asScala.map(r => (r.getKey.getRow.copyBytes, r.getValue.get)), scanner.close())
  }

  override def close(): Unit = synchronized {
    if (writerCreated) {
      CloseWithLogging(writer)
    } else {
      writerCreated = true // indicate that we've closed and don't want to open any more resources
    }
  }
}

object AccumuloBackedMetadata {

  /**
    * Old single row metadata kept around for back-compatibility
    */
  class SingleRowAccumuloMetadata[T](metadata: AccumuloBackedMetadata[T]) {

    import scala.collection.JavaConverters._

    // if the table doesn't exist, we assume that we don't ever need to check it for old-encoded rows
    private val tableExists = metadata.connector.tableOperations().exists(metadata.table)

    def getFeatureTypes: Array[String] = {
      if (!tableExists) { Array.empty } else {
        val scanner = metadata.connector.createScanner(metadata.table, Authorizations.EMPTY)
        // restrict to just one cf so we only get 1 hit per feature
        // use attributes as it's the only thing that's been there through all geomesa versions
        scanner.fetchColumnFamily(new Text(GeoMesaMetadata.AttributesKey))
        try {
          scanner.iterator.asScala.map(e => SingleRowAccumuloMetadata.getTypeName(e.getKey.getRow)).toArray
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
        val scanner = metadata.connector.createScanner(metadata.table, Authorizations.EMPTY)
        val writer = metadata.connector.createBatchWriter(metadata.table, GeoMesaBatchWriterConfig())
        try {
          scanner.setRange(SingleRowAccumuloMetadata.getRange(typeName))
          scanner.iterator.asScala.foreach { entry =>
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

    private val MetadataTag = "~METADATA"
    private val MetadataRowKeyRegex = (MetadataTag + """_(.*)""").r

    def getRange(typeName: String): Range = new Range(s"${MetadataTag}_$typeName")

    def getTypeName(row: Text): String = {
      val MetadataRowKeyRegex(typeName) = row.toString
      typeName
    }
  }
}
