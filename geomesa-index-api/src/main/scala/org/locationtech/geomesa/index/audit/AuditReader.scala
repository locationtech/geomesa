/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.io.Closeable
import java.time.ZonedDateTime

/**
 * Reads an audited event
 */
trait AuditReader extends Closeable {

  /**
   * Retrieves stored events
   *
   * @param typeName simple feature type name
   * @param dates dates to retrieve stats for
   * @return iterator of events
   */
  def getQueryEvents(typeName: String, dates: (ZonedDateTime, ZonedDateTime)): CloseableIterator[QueryEvent]
}

