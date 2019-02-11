/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.accumulo

import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.blob.api.{Blob, BlobStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.AuditProvider

import scala.util.control.NonFatal

class AccumuloBlobStoreImpl(val connector: Connector,
                            val blobTableName: String,
                            val authProvider: AuthorizationsProvider,
                            val auditProvider: AuditProvider,
                            val bwConf: BatchWriterConfig ) extends BlobStore with LazyLogging {

  import scala.collection.JavaConverters._

  AccumuloVersion.createTableIfNeeded(connector, blobTableName)

  protected val bw = connector.createBatchWriter(blobTableName, bwConf)
  protected val tableOps = connector.tableOperations()

  private val empty = new Text()

  override def get(id: String): Blob = {
    val scanner = connector.createScanner(
      blobTableName,
      new Authorizations(authProvider.getAuthorizations.asScala: _*)
    )
    try {
      scanner.setRange(new Range(new Text(id)))
      val iter = scanner.iterator()
      if (iter.hasNext) {
        val next = iter.next()
        new Blob(id, next.getKey.getColumnQualifier.toString, next.getValue.get)
      } else {
        null
      }
    } finally {
      scanner.close()
    }
  }

  override def put(id: String, localName: String, bytes: Array[Byte]): Unit = {
    val m = new Mutation(id)
    m.put(empty, new Text(localName), new Value(bytes))
    bw.addMutation(m)
    bw.flush()
  }

  override def deleteBlob(id: String): Unit = {
    val bd = connector.createBatchDeleter(
      blobTableName,
      new Authorizations(authProvider.getAuthorizations.asScala: _*),
      bwConf.getMaxWriteThreads,
      bwConf)
    try {
      bd.setRanges(Collections.singletonList(new Range(new Text(id))))
      bd.delete()
    } finally {
      bd.close()
    }
  }

  override def close(): Unit = {
    bw.close()
  }

  override def deleteBlobStore(): Unit = {
    try {
      tableOps.delete(blobTableName)
    } catch {
      case NonFatal(e) => logger.error("Error when deleting BlobStore", e)
    }
  }
}

object AccumuloBlobStoreImpl {

  val blobSuffix: String = "_blob"

}
