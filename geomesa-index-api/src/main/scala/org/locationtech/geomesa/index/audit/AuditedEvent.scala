/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.geoserver.ViewParams

import java.util.Collections

/**
  * Basic trait for any 'event' that we may want to audit. Ties it to a particular data store, schema type name
  * and date
  */
sealed trait AuditedEvent {

  /**
   * User who triggered the event
   *
   * @return
   */
  def user: String

  /**
    * Simple feature type name that triggered the event
    *
    * @return
    */
  def typeName: String

  /**
    * Start date of event, in millis since the Java epoch
    *
    * @return
    */
  def start: Long

  /**
   * End date of event, in millis since the Java epoch
   *
   * @return
   */
  def end: Long
}

object AuditedEvent {

  case class QueryEvent(
    storeType: String,
    typeName: String,
    user:     String,
    filter:   String,
    hints:    java.util.Map[String, String],
    metadata: java.util.Map[String, AnyRef],
    start:    Long,
    end:      Long,
    planTime: Long,
    scanTime: Long,
    hits:     Long
  ) extends AuditedEvent

  object QueryEvent {
    def apply(
        storeType: String,
        typeName: String,
        user:     String,
        filter:   Filter,
        hints:    Hints,
        start:    Long,
        end:      Long,
        planTime: Long,
        scanTime: Long,
        hits:     Long
      ): QueryEvent = {
      val filterString = filterToString(filter)
      val readableHints = ViewParams.getReadableHints(hints)
      val metadata = Collections.emptyMap[String, AnyRef]()
      QueryEvent(storeType, typeName, user, filterString, readableHints, metadata, start, end, planTime, scanTime, hits)
    }
  }
}
