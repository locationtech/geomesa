/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableManager}
import org.locationtech.geomesa.index.metadata.{KeyValueStoreMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

class AccumuloBackedMetadata[T](
    val client: AccumuloClient,
    val table: String,
    val serializer: MetadataSerializer[T],
    consistency: Option[ConsistencyLevel] = None
  ) extends KeyValueStoreMetadata[T] {

  import scala.collection.JavaConverters._

  // note: accumulo client is closed by the owning data store

  private val config = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(2)

  private val empty = new Text()

  override protected def checkIfTableExists: Boolean = client.tableOperations().exists(table)

  override protected def createTable(): Unit = new TableManager(client).ensureTableExists(table)

  override protected def createEmptyBackup(timestamp: String): AccumuloBackedMetadata[T] =
    new AccumuloBackedMetadata(client, s"${table}_${timestamp}_bak", serializer, consistency)

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    WithClose(client.createBatchWriter(table, config)) { writer =>
      rows.foreach { case (k, v) =>
        val m = new Mutation(k)
        m.put(empty, empty, new Value(v))
        writer.addMutation(m)
      }
    }
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    val ranges = rows.map(r => Range.exact(new Text(r))).asJava
    WithClose(client.createBatchDeleter(table, Authorizations.EMPTY, 1, config)) { deleter =>
      deleter.setRanges(ranges)
      deleter.delete()
    }
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    WithClose(client.createScanner(table, Authorizations.EMPTY)) { scanner =>
      scanner.setRange(Range.exact(new Text(row)))
      consistency.foreach(scanner.setConsistencyLevel)
      val iter = scanner.iterator
      if (iter.hasNext) {
        Some(iter.next.getValue.get)
      } else {
        None
      }
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    // ensure we don't scan any single-row encoded values
    val range = prefix.map(p => Range.prefix(new Text(p))).getOrElse(new Range("", "~"))
    val scanner = client.createScanner(table, Authorizations.EMPTY)
    scanner.setRange(range)
    consistency.foreach(scanner.setConsistencyLevel)
    CloseableIterator(scanner.iterator.asScala.map(r => (r.getKey.getRow.copyBytes, r.getValue.get)), scanner.close())
  }

  @deprecated("Use `client`")
  def connector: AccumuloClient = client
}
