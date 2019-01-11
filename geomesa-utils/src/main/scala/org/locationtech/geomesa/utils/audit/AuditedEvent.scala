/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit

import java.io.Closeable
import java.time.ZonedDateTime

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
  * Basic trait for any 'event' that we may want to audit. Ties it to a particular data store, schema type name
  * and date
  */
trait AuditedEvent {

  /**
    * Underlying data store type that triggered the event - e.g. 'accumulo', 'hbase', 'kafka'
    *
    * @return
    */
  def storeType: String

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
}

/**
  * An event that can be soft-deleted
  */
trait DeletableEvent extends AuditedEvent {

  /**
    * Has the event been marked as deleted?
    *
    * @return
    */
  def deleted: Boolean
}

/**
  * Writes an audited event
  */
trait AuditWriter extends Closeable {

  /**
    * Writes an event asynchronously
    *
    * @param event event to write
    * @tparam T event type
    */
  def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit
}

/**
  * Reads an audited event
  */
trait AuditReader extends Closeable {

  /**
    * Retrieves stored events
    *
    * @param typeName simple feature type name
    * @param dates dates to retrieve stats for
    * @tparam T event type
    * @return iterator of events
    */
  def getEvents[T <: AuditedEvent](typeName: String,
                                   dates: (ZonedDateTime, ZonedDateTime))
                                  (implicit ct: ClassTag[T]): Iterator[T]
}

/**
  * Implemented AuditWriter by logging events as json
  */
trait AuditLogger extends AuditWriter with LazyLogging {

  private val gson: Gson = new GsonBuilder().serializeNulls().create()

  override def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit =
    logger.debug(gson.toJson(event))

  override def close(): Unit = {}
}

object AuditLogger extends AuditLogger