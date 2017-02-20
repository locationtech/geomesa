/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.cassandra.data

import java.nio.charset.StandardCharsets

import com.datastax.driver.core.Session
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.metadata._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class CassandraBackedMetaData[T](val session: Session, val catalog: String, val serializer: MetadataSerializer[T])
    extends CachedLazyMetadata[T] with CassandraMetadataAdapter

trait CassandraMetadataAdapter extends MetadataAdapter {

  def session: Session
  def catalog: String

  override protected def checkIfTableExists: Boolean = {
    try {
      session.execute(s"select table_name from system_schema.tables where keyspace_name = '${session.getLoggedKeyspace}' and table_name = '$catalog'").nonEmpty
    } catch {
      case NonFatal(_) => false
    }
  }

  override protected def createTable(): Unit = {
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (key text, value text, PRIMARY KEY (key))")
  }

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (key, value) =>
      session.execute(s"INSERT INTO $catalog (key, value) VALUES (?, ?)", wrap(key), wrap(value))
    }
  }

  override protected def delete(row: Array[Byte]): Unit = {
    session.execute(s"DELETE FROM $catalog WHERE key = ?", wrap(row))
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = rows.foreach(delete)

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val rows = session.execute(s"SELECT value FROM $catalog WHERE key = ?", wrap(row)).all()
    if (rows.length < 1) { None } else {
      Some(rows.head.getString("value").getBytes(StandardCharsets.UTF_8))
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val rows = prefix match {
      case None    => session.execute(s"SELECT key FROM $catalog").all()
      case Some(p) =>
        val start = wrap(p)
        val end = wrap(IndexAdapter.rowFollowingPrefix(p))
        session.execute(s"SELECT key FROM $catalog WHERE key >= ? and key < ?", start, end).all()
    }
    CloseableIterator(rows.map(_.getString("key").getBytes(StandardCharsets.UTF_8)).iterator)
  }

  override def close(): Unit = {} // session gets closed by datastore dispose

  private def wrap(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
}