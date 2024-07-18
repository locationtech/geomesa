/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.audit.AuditProvider

import java.io.Closeable
import scala.concurrent.Future

/**
 * Writes an audited event
 *
 * @param storeType store type
 * @param auditProvider audit provider
 */
abstract class AuditWriter(storeType: String, auditProvider: AuditProvider) extends Closeable {

  /**
   * Writes a query event asynchronously
   *
   * @param typeName type name
   * @param filter filter
   * @param hints query hints
   * @param planTime time spent planning, in millis
   * @param scanTime time spent planning, in millis
   * @param hits number of results
   */
  def writeQueryEvent(typeName: String, filter: Filter, hints: Hints, planTime: Long, scanTime: Long, hits: Long): Future[Unit] = {
    val user = auditProvider.getCurrentUserId
    val filterString = filterToString(filter)
    val hintsString = ViewParams.getReadableHints(hints)
    write(QueryEvent(storeType, typeName, System.currentTimeMillis(), user, filterString, hintsString, planTime, scanTime, hits))
  }

  protected def write(event: QueryEvent): Future[Unit]
}

object AuditWriter {

  /**
   * Implemented AuditWriter by logging events as json
   *
   * @param storeType store type
   * @param auditProvider audit provider
   */
  class AuditLogger(storeType: String, auditProvider: AuditProvider)
    extends AuditWriter(storeType, auditProvider) with StrictLogging {

    private val gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create()

    override protected def write(event: QueryEvent): Future[Unit] = Future.successful(logger.debug(gson.toJson(event)))

    override def close(): Unit = {}
  }
}
