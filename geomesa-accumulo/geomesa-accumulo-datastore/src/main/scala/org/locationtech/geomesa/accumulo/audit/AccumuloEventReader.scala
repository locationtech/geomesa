/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.time.ZonedDateTime

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.utils.audit.AuditedEvent
import org.locationtech.geomesa.utils.collection.{IsSynchronized, MaybeSynchronized, NotSynchronized}


/**
  * Manages reading of usage stats
  */
class AccumuloEventReader(connector: Connector, table: String) {

  private val tableExists: MaybeSynchronized[Boolean] =
    if (connector.tableOperations().exists(table)) { new NotSynchronized(true) } else { new IsSynchronized(false) }

  def query[T <: AuditedEvent](typeName: String,
                               dates: (ZonedDateTime, ZonedDateTime),
                               auths: Authorizations)
                              (implicit transform: AccumuloEventTransform[T]): Iterator[T] = {
    if (!checkTable) { Iterator.empty } else {
      val scanner = connector.createScanner(table, auths)
      val rangeStart = s"$typeName~${dates._1.format(AccumuloEventTransform.dateFormat)}"
      val rangeEnd = s"$typeName~${dates._2.format(AccumuloEventTransform.dateFormat)}"
      scanner.setRange(new Range(rangeStart, rangeEnd))
      transform.iterator(scanner)
    }
  }

  private def checkTable: Boolean = {
    if (tableExists.get) {
      true
    } else if (connector.tableOperations().exists(table)) {
      tableExists.set(true, false)
      true
    } else {
      false
    }
  }
}
