/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

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
    * Date of event, in millis since the Java epoch
    *
    * @return
    */
  def date: Long

  /**
   * Has the event been marked as deleted?
   *
   * @return
   */
//  def deleted: Boolean
}

object AuditedEvent {

  case class QueryEvent(
    storeType: String,
    typeName: String,
    date:     Long,
    user:     String,
    filter:   String,
    hints:    String,
    planTime: Long,
    scanTime: Long,
    hits:     Long
  ) extends AuditedEvent
}
