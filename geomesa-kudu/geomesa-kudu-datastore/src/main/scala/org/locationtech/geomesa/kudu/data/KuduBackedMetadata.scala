/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.util.Collections

import org.apache.kudu.ColumnSchema.Encoding
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduPredicate}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

class KuduBackedMetadata[T](val client: KuduClient, val catalog: String, val serializer: MetadataSerializer[T])
    extends CachedLazyMetadata[T] {

  import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner

  import scala.collection.JavaConverters._

  lazy private val table = client.openTable(catalog)

  override protected def checkIfTableExists: Boolean = client.tableExists(catalog)

  override protected def createTable(): Unit = {
    val sftCol = new ColumnSchema.ColumnSchemaBuilder("type", Type.STRING)
        .encoding(Encoding.PREFIX_ENCODING)
        .compressionAlgorithm(ColumnConfiguration.compression())
        .key(true)
        .build()

    val keyCol = new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
        .encoding(Encoding.PLAIN_ENCODING)
        .compressionAlgorithm(ColumnConfiguration.compression())
        .key(true)
        .build()

    val valueCol = new ColumnSchema.ColumnSchemaBuilder("value", Type.BINARY)
        .encoding(Encoding.PLAIN_ENCODING)
        .compressionAlgorithm(ColumnConfiguration.compression())
        .build()

    val schema = new Schema(Seq(sftCol, keyCol, valueCol).asJava)

    val options = new CreateTableOptions().addHashPartitions(Collections.singletonList(sftCol.getName), 2)

    // default replication is 3, but for small clusters that haven't been
    // updated using the default will throw an exception
    val servers = client.listTabletServers.getTabletServersCount
    if (servers < 3) {
      options.setNumReplicas(servers)
    }

    client.createTable(catalog, schema, options)
  }

  override protected def write(typeName: String, rows: Seq[(String, Array[Byte])]): Unit = {
    val session = client.newSession()
    try {
      rows.foreach { case (key, value) =>
        val insert = table.newUpsert()
        insert.getRow.addString(0, typeName)
        insert.getRow.addString(1, key)
        insert.getRow.addBinary(2, value)
        session.apply(insert)
      }
    } finally {
      session.close()
    }
  }

  override protected def delete(typeName: String, keys: Seq[String]): Unit = {
    val session = client.newSession()
    try {
      keys.foreach { key =>
        val delete = table.newDelete()
        delete.getRow.addString(0, typeName)
        delete.getRow.addString(1, key)
        session.apply(delete)
      }
    } finally {
      session.close()
    }
  }

  override protected def scanValue(typeName: String, key: String): Option[Array[Byte]] = {
    import KuduPredicate.newComparisonPredicate
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    val scanner = client.newScannerBuilder(table)
        .setProjectedColumnIndexes(Collections.singletonList(2))
        .addPredicate(newComparisonPredicate(table.getSchema.getColumnByIndex(0), ComparisonOp.EQUAL, typeName))
        .addPredicate(newComparisonPredicate(table.getSchema.getColumnByIndex(1), ComparisonOp.EQUAL, key))
        .build()
    WithClose(scanner.iterator)(_.headOption.map(_.getBinaryCopy(0)))
  }

  override protected def scanValues(typeName: String, prefix: String): CloseableIterator[(String, Array[Byte])] = {
    import KuduPredicate.newComparisonPredicate

    val builder = client.newScannerBuilder(table).setProjectedColumnIndexes(Seq[Integer](0, 1, 2).asJava)
    builder.addPredicate(newComparisonPredicate(table.getSchema.getColumnByIndex(0), ComparisonOp.EQUAL, typeName))

    val iter = builder.build().iterator.map(r => (r.getString(1), r.getBinaryCopy(2)))
    if (prefix == null || prefix.isEmpty) { iter } else {
      iter.filter { case (k, _) => k.startsWith(prefix) }
    }
  }

  override protected def scanKeys(): CloseableIterator[(String, String)] = {
    val builder = client.newScannerBuilder(table).setProjectedColumnIndexes(Seq[Integer](0, 1).asJava)
    builder.build().iterator.map(r => (r.getString(0), r.getString(1)))
  }

  override def close(): Unit = {} // client gets closed by datastore dispose
}
