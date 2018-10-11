/***********************************************************************
 * Copyright (c) 2017-2018 IBM
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.nio.charset.StandardCharsets

import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata.ScanQuery
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

class CassandraBackedMetadata[T](val session: Session, val catalog: String, val serializer: MetadataSerializer[T])
    extends CachedLazyMetadata[T] {

  override protected def checkIfTableExists: Boolean = {
    val m = session.getCluster.getMetadata
    val km = m.getKeyspace(session.getLoggedKeyspace)
    km.getTable(catalog) != null
  }

  override protected def createTable(): Unit =
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (sft text, key text, value text, PRIMARY KEY ((sft), key))")

  override protected def write(typeName: String, rows: Seq[(String, Array[Byte])]): Unit = {
    rows.foreach { case (key, value) =>
      session.execute(s"INSERT INTO $catalog (sft, key, value) VALUES (?, ?, ?)",
        typeName, key, new String(value, StandardCharsets.UTF_8))
    }
  }

  override protected def delete(typeName: String, keys: Seq[String]): Unit = {
    keys.foreach { key =>
      val query = QueryBuilder.delete().from(catalog).where(QueryBuilder.eq("sft", typeName)).and(QueryBuilder.eq("key", key))
      session.execute(query)
    }
  }

  override protected def scanValue(typeName: String, key: String): Option[Array[Byte]] = {
    val query = QueryBuilder.select("value").from(catalog)
    query.where(QueryBuilder.eq("sft", typeName)).and(QueryBuilder.eq("key", key))
    val rows = session.execute(query).all()
    if (rows.length < 1) { None } else {
      Some(rows.head.getString("value").getBytes(StandardCharsets.UTF_8))
    }
  }

  override protected def scanValues(query: Option[ScanQuery]): CloseableIterator[(String, String, Array[Byte])] = {
    val select = QueryBuilder.select("sft", "key", "value").from(catalog)
    query.foreach(q => select.where(QueryBuilder.eq("sft", q.typeName)))
    val values = session.execute(select).all().iterator.map { row =>
      (row.getString("sft"), row.getString("key"), row.getString("value").getBytes(StandardCharsets.UTF_8))
    }
    query.flatMap(_.prefix) match {
      case None => CloseableIterator(values)
      case Some(prefix) => CloseableIterator(values.filter(_._2.startsWith(prefix)))
    }
  }

  override def close(): Unit = {} // session gets closed by datastore dispose
}
