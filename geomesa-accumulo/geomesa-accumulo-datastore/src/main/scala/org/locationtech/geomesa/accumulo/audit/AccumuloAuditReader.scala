/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.audit.AuditReader
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.{CloseableIterator, IsSynchronized, MaybeSynchronized, NotSynchronized}

import java.time.ZonedDateTime

/**
 * An audit reader
 *
 * @param client accumulo client - note: assumed to be shared and not cleaned up on closed
 * @param table table containing audit records
 * @param authProvider auth provider
 */
class AccumuloAuditReader(client: AccumuloClient, table: String, authProvider: AuthorizationsProvider) extends AuditReader {

  import scala.collection.JavaConverters._

  def this(ds: AccumuloDataStore) = this(ds.connector, ds.config.auditWriter.table, ds.config.authProvider)

  private val tableExists: MaybeSynchronized[Boolean] =
    if (client.tableOperations().exists(table)) { new NotSynchronized(true) } else { new IsSynchronized(false) }

  override def getQueryEvents(typeName: String, dates: (ZonedDateTime, ZonedDateTime)): CloseableIterator[QueryEvent] = {
    if (!checkTable) { CloseableIterator.empty } else {
      val scanner = client.createScanner(table, new Authorizations(authProvider.getAuthorizations.asScala.toSeq: _*))
      AccumuloQueryEventTransform.iterator(scanner, typeName, dates)
    }
  }

  override def close(): Unit = {}

  private def checkTable: Boolean = {
    if (tableExists.get) {
      true
    } else if (client.tableOperations().exists(table)) {
      tableExists.set(true, false)
      true
    } else {
      false
    }
  }
}
