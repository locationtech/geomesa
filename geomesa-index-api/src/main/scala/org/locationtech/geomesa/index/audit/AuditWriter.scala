/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.Logger
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.audit.AuditProvider

import java.io.Closeable
import java.util.Collections
import scala.concurrent.Future

/**
 * Writes an audited event
 *
 * @param storeType store type
 * @param auditProvider audit provider
 */
abstract class AuditWriter(val storeType: String, val auditProvider: AuditProvider) extends Closeable {

  /**
   * Writes a query event asynchronously
   *
   * @param typeName type name
   * @param user user making the query
   * @param filter query filter
   * @param hints query hints
   * @param plans query plans being executed
   * @param startTime time query started, in millis since the epoch
   * @param endTime time query completed, in millis since the epoch
   * @param planTime time spent planning the query execution, in millis
   * @param scanTime time spent executing the query, in millis
   * @param hits number of results
   * @return
   */
  def writeQueryEvent(
      typeName: String,
      user: String,
      filter: Filter,
      hints: Hints,
      plans: Seq[QueryPlan[_]],
      startTime: Long,
      endTime: Long,
      planTime: Long,
      scanTime: Long,
      hits: Long): Future[Unit]

  override def close(): Unit = {}
}

object AuditWriter {

  val Gson: Gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create()

  private val logger = Logger(classOf[AuditWriter])

  /**
   * Log a query event
   *
   * @param event event
   */
  def log(event: QueryEvent): Unit = logger.debug(Gson.toJson(event))

  /**
   * Implemented AuditWriter by logging events as json
   *
   * @param storeType store type
   * @param auditProvider audit provider
   */
  class AuditLogger(storeType: String, auditProvider: AuditProvider)
      extends AuditWriter(storeType, auditProvider) {
    override def writeQueryEvent(
        typeName: String,
        user: String,
        filter: Filter,
        hints: Hints,
        plans: Seq[QueryPlan[_]],
        startTime: Long,
        endTime: Long,
        planTime: Long,
        scanTime: Long,
        hits: Long): Future[Unit] = {
      Future.successful {
        logger.debug(Gson.toJson(QueryEvent(storeType, typeName, user, filter, hints, startTime, endTime, planTime, scanTime, hits)))
      }
    }
  }
}
