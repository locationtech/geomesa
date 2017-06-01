/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval
import org.locationtech.geomesa.utils.audit.AuditedEvent


/**
  * Manages reading of usage stats
  */
class AccumuloEventReader(connector: Connector, table: String) {

  private var tableExists = false

  def query[T <: AuditedEvent](typeName: String,
                               dates: Interval,
                               auths: Authorizations)
                              (implicit transform: AccumuloEventTransform[T]): Iterator[T] = {
    if (!checkTable) {
      Iterator.empty
    } else {
      val scanner = connector.createScanner(table, auths)
      val rangeStart = s"$typeName~${AccumuloEventTransform.dateFormat.print(dates.getStart)}"
      val rangeEnd = s"$typeName~${AccumuloEventTransform.dateFormat.print(dates.getEnd)}"
      scanner.setRange(new Range(rangeStart, rangeEnd))
      transform.iterator(scanner)
    }
  }

  private def checkTable: Boolean = synchronized {
    if (!tableExists) {
      tableExists = connector.tableOperations().exists(table)
    }
    tableExists
  }
}
