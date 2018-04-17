/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.nio.charset.StandardCharsets
import java.util.Collections

import com.google.common.primitives.Bytes
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduPredicate}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.kudu.data.KuduBackedMetadata.KuduMetadataAdapter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose

class KuduBackedMetadata[T](val client: KuduClient, val catalog: String, val serializer: MetadataSerializer[T])
    extends CachedLazyMetadata[T] with KuduMetadataAdapter

object KuduBackedMetadata {

  trait KuduMetadataAdapter extends MetadataAdapter {

    this: CachedLazyMetadata[_] =>

    import StandardCharsets.UTF_8

    def client: KuduClient
    def catalog: String

    lazy private val table = client.openTable(catalog)

    lazy private val typeNameSeparatorByte = {
      val bytes = typeNameSeparator.toString.getBytes(UTF_8)
      require(bytes.length == 1, "Expected single byte for type name separator")
      bytes.head
    }

    override protected def checkIfTableExists: Boolean = client.tableExists(catalog)

    override protected def createTable(): Unit = {
      import scala.collection.JavaConverters._

      val columns = Seq(
        new ColumnSchema.ColumnSchemaBuilder("type", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build()
      ).asJava

      val options = new CreateTableOptions().addHashPartitions(Collections.singletonList("type"), 2)

      // default replication is 3, but for small clusters that haven't been
      // updated using the default will throw an exception
      val servers = client.listTabletServers.getTabletServersCount
      if (servers < 3) {
        options.setNumReplicas(servers)
      }

      client.createTable(catalog, new Schema(columns), options)
    }

    override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
      val session = client.newSession()
      try {
        rows.foreach { case (key, value) =>
          val (sft, k) = split(key)
          val insert = table.newUpsert()
          insert.getRow.addString(0, sft)
          insert.getRow.addString(1, k)
          insert.getRow.addString(2, new String(value, UTF_8))
          session.apply(insert)
        }
      } finally {
        session.close()
      }
    }

    override protected def delete(row: Array[Byte]): Unit = delete(Seq(row))

    override protected def delete(rows: Seq[Array[Byte]]): Unit = {
      val session = client.newSession()
      try {
        rows.foreach { key =>
          val (sft, k) = split(key)
          val delete = table.newDelete()
          delete.getRow.addString(0, sft)
          delete.getRow.addString(1, k)
          session.apply(delete)
        }
      } finally {
        session.close()
      }
    }

    override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
      import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

      val (sft, key) = split(row)
      val scanner = client.newScannerBuilder(table)
          .setProjectedColumnIndexes(Collections.singletonList(2))
          .addPredicate(KuduPredicate.newComparisonPredicate(table.getSchema.getColumnByIndex(0), ComparisonOp.EQUAL, sft))
          .addPredicate(KuduPredicate.newComparisonPredicate(table.getSchema.getColumnByIndex(1), ComparisonOp.EQUAL, key))
          .build()
      WithClose(scanner.iterator)(_.headOption.map(_.getString(0).getBytes(UTF_8)))
    }

    override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[Array[Byte]] = {
      import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner

      import scala.collection.JavaConverters._

      val builder = client.newScannerBuilder(table).setProjectedColumnIndexes(Seq[Integer](0, 1).asJava)
      prefix.foreach { p =>
        val (sft, key) = split(p)
        builder.addPredicate(KuduPredicate.newComparisonPredicate(table.getSchema.getColumnByIndex(0), ComparisonOp.EQUAL, sft))
        if (key != null && key.length > 0) {
          val end = new String(ByteArrays.rowFollowingPrefix(key.getBytes(UTF_8)), UTF_8)
          val col = table.getSchema.getColumnByIndex(1)
          builder.addPredicate(KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, key))
          builder.addPredicate(KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, end))
        }
      }

      builder.build().iterator.map { row =>
        val sft = row.getString(0).getBytes(UTF_8)
        val key = row.getString(1).getBytes(UTF_8)
        Bytes.concat(sft, Array(typeNameSeparatorByte), key)
      }
    }

    override def close(): Unit = {} // client gets closed by datastore dispose

    private def split(row: Array[Byte]): (String, String) = {
      val i = row.indexOf(typeNameSeparatorByte)
      if (i == -1) {
        (new String(row, UTF_8), null)
      } else {
        (new String(row, 0, i, UTF_8), new String(row, i + 1, row.length - (i + 1), UTF_8))
      }
    }
  }
}
