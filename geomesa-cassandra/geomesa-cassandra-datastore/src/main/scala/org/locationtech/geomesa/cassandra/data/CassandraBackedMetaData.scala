/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.nio.charset.StandardCharsets

import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.primitives.Bytes
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

class CassandraBackedMetadata[T](val session: Session, val catalog: String, val serializer: MetadataSerializer[T])
    extends CachedLazyMetadata[T] with CassandraMetadataAdapter

trait CassandraMetadataAdapter extends MetadataAdapter {

  this: CachedLazyMetadata[_] =>

  import StandardCharsets.UTF_8

  def session: Session
  def catalog: String

  lazy val typeNameSeparatorByte = {
    val bytes = typeNameSeparator.toString.getBytes(UTF_8)
    require(bytes.length == 1, "Expected single byte for type name separator")
    bytes.head
  }

  override protected def checkIfTableExists: Boolean = {
    val m = session.getCluster.getMetadata
    val km = m.getKeyspace(session.getLoggedKeyspace)
    km.getTable(catalog) != null
  }

  override protected def createTable(): Unit =
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (sft text, key text, value text, PRIMARY KEY ((sft), key))")

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (key, value) =>
      val (sft, k) = split(key)
      session.execute(s"INSERT INTO $catalog (sft, key, value) VALUES (?, ?, ?)", sft, k, new String(value, UTF_8))
    }
  }

  override protected def delete(row: Array[Byte]): Unit = {
    val (sft, key) = split(row)
    val query = QueryBuilder.delete().from(catalog).where(QueryBuilder.eq("sft", sft)).and(QueryBuilder.eq("key", key))
    session.execute(query)
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = rows.foreach(delete)

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val (sft, key) = split(row)
    val query = QueryBuilder.select("value").from(catalog)
    query.where(QueryBuilder.eq("sft", sft)).and(QueryBuilder.eq("key", key))
    val rows = session.execute(query).all()
    if (rows.length < 1) { None } else {
      Some(rows.head.getString("value").getBytes(UTF_8))
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val query = QueryBuilder.select("sft", "key").from(catalog)
    prefix.foreach { p =>
      val (sft, key) = split(p)
      query.where(QueryBuilder.eq("sft", sft))
      if (key != null && key.length > 0) {
        val end = new String(IndexAdapter.rowFollowingPrefix(key.getBytes(UTF_8)), UTF_8)
        query.where(QueryBuilder.gte("key", key)).and(QueryBuilder.lt("key", end))
      }
    }
    val bytes = session.execute(query).all().map { row =>
      val sft = row.getString("sft").getBytes(UTF_8)
      val key = row.getString("key").getBytes(UTF_8)
      Bytes.concat(sft, Array(typeNameSeparatorByte), key)
    }
    CloseableIterator(bytes.iterator)
  }

  override def close(): Unit = {} // session gets closed by datastore dispose

  private def split(row: Array[Byte]): (String, String) = {
    val i = row.indexOf(typeNameSeparatorByte)
    if (i == -1) {
      (new String(row, UTF_8), null)
    } else {
      (new String(row, 0, i, UTF_8), new String(row, i + 1, row.length - (i + 1), UTF_8))
    }
  }
}
