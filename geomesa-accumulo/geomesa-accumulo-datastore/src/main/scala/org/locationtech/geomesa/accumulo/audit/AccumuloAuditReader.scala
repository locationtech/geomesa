/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.audit.AuditReader
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.time.ZonedDateTime

/**
 * An audit reader
 *
 * @param client accumulo client - note: assumed to be shared and not cleaned up on closed
 * @param table table containing audit records
 * @param authProvider auth provider
 * @param consistency scan consistency level
 */
class AccumuloAuditReader(
    client: AccumuloClient,
    table: String,
    authProvider: AuthorizationsProvider,
    consistency: Option[ConsistencyLevel] = None
  ) extends AuditReader {

  import scala.collection.JavaConverters._

  def this(ds: AccumuloDataStore) =
    this(ds.client, ds.config.auditWriter.table, ds.config.authProvider, ds.config.queries.consistency)

  @volatile
  private var tableExists: Boolean = client.tableOperations().exists(table)

  override def getQueryEvents(typeName: String, dates: (ZonedDateTime, ZonedDateTime)): CloseableIterator[QueryEvent] = {
    if (!checkTable) { CloseableIterator.empty } else {
      val scanner = client.createScanner(table, new Authorizations(authProvider.getAuthorizations.asScala.toSeq: _*))
      consistency.foreach(scanner.setConsistencyLevel)
      AccumuloQueryEventTransform.iterator(scanner, typeName, dates)
    }
  }

  override def close(): Unit = {}

  private def checkTable: Boolean = {
    if (tableExists) {
      true
    } else if (client.tableOperations().exists(table)) {
      tableExists = true
      true
    } else {
      false
    }
  }
}
