/***********************************************************************
 * Copyright (c) 2017-2025 IBM
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.locationtech.geomesa.index.api.IndexAdapter
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class CassandraBackedMetadata[T](val session: CqlSession, val catalog: String, val serializer: MetadataSerializer[T])
    extends TableBasedMetadata[T] {

  // note: session gets closed by datastore dispose

  override protected def checkIfTableExists: Boolean = {
    session.getKeyspace
      .flatMap(session.getMetadata.getKeyspace)
      .flatMap(_.getTable(catalog))
      .isPresent
  }

  override protected def createTable(): Unit =
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (sft text, key text, value text, PRIMARY KEY ((sft), key))")

  override protected def createEmptyBackup(timestamp: String): CassandraBackedMetadata[T] = {
    val table = {
      val full = s"${catalog}_${timestamp}_bak"
      if (full.lengthCompare(CassandraIndexAdapter.TableNameLimit) <= 0) { full } else {
        IndexAdapter.truncateTableName(full, CassandraIndexAdapter.TableNameLimit)
      }
    }
    new CassandraBackedMetadata(session, table, serializer)
  }

  override protected def write(typeName: String, rows: Seq[(String, Array[Byte])]): Unit = {
    rows.foreach { case (key, value) =>
      session.execute(s"INSERT INTO $catalog (sft, key, value) VALUES (?, ?, ?)",
        typeName, key, new String(value, StandardCharsets.UTF_8))
    }
  }

  override protected def delete(typeName: String, keys: Seq[String]): Unit = {
    keys.foreach { key =>
      val query = QueryBuilder.deleteFrom(catalog)
        .whereColumn("sft").isEqualTo(QueryBuilder.literal(typeName))
        .whereColumn("key").isEqualTo(QueryBuilder.literal(key))
        .build()
      session.execute(query)
    }
  }

  override protected def scanValue(typeName: String, key: String): Option[Array[Byte]] = {
    val query = QueryBuilder.selectFrom(catalog).column("value")
      .whereColumn("sft").isEqualTo(QueryBuilder.literal(typeName))
      .whereColumn("key").isEqualTo(QueryBuilder.literal(key))
      .build()
    val rows = session.execute(query).all().asScala
    if (rows.length < 1) { None } else {
      Some(rows.head.getString("value").getBytes(StandardCharsets.UTF_8))
    }
  }

  override protected def scanValues(typeName: String, prefix: String): CloseableIterator[(String, Array[Byte])] = {
    val select = QueryBuilder.selectFrom(catalog).columns("key", "value")
      .whereColumn("sft").isEqualTo(QueryBuilder.literal(typeName))
      .build()
    val iter = session.execute(select).all().iterator.asScala.map { row =>
      (row.getString("key"), row.getString("value").getBytes(StandardCharsets.UTF_8))
    }
    if (prefix == null || prefix.isEmpty) {
      CloseableIterator(iter)
    } else {
      CloseableIterator(iter.filter { case (k, _) => k.startsWith(prefix) })
    }
  }

  override protected def scanKeys(): CloseableIterator[(String, String)] = {
    val select = QueryBuilder.selectFrom(catalog).columns("sft", "key").build()
    val values = session.execute(select).all().iterator.asScala.map(row => (row.getString("sft"), row.getString("key")))
    CloseableIterator(values)
  }
}
