/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.analytics

import org.joda.time.Interval
import org.joda.time.format.ISODateTimeFormat
import org.json4s.{DefaultFormats, Formats, JValue}
import org.locationtech.geomesa.accumulo.audit.SerializedQueryEvent
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.web.core.GeoMesaDataStoreServlet
import org.scalatra.json.NativeJsonSupport
import org.scalatra.{BadRequest, Ok}

import scala.util.Try

class StatsEndpoint(val persistence: FilePersistence) extends GeoMesaDataStoreServlet with NativeJsonSupport {

  override def root: String = "stats"

  override protected implicit def jsonFormats: Formats = DefaultFormats

  private val dtFormat = ISODateTimeFormat.dateTime().withZoneUTC()
  private val binHintString = QueryEvent.keyToString(QueryHints.BIN_TRACK)

  before() {
    contentType = formats("json")
  }

  // filter out the internal 'deleted' flag
  protected override def transformResponseBody(body: JValue): JValue = body.removeField {
    case ("deleted", _) => true
    case _          => false
  }

  /**
   * Retrieves stored query stats.
   *
   * params:
   *   'typeName' - simple feature type to query
   *   'dates' - date range for results - format is 2015-11-01T00:00:00.000Z/2015-12-5T00:00:00.000Z
   *   'who' - (optional) user to filter on
   */
  get("/:alias/queries/?") {
    try {
      withDataStore((ds) => {
        val sft = params.get("typeName").orNull
        // corresponds to 2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z - same as used by geotools
        val dates = params.get("dates").flatMap(d => Try(d.split("/").map(dtFormat.parseDateTime)).toOption).orNull
        val binTry = Try(params.get("bin").map(_.toBoolean))
        if (sft == null || dates == null || dates.length != 2 || binTry.isFailure) {
          val reason = new StringBuilder
          if (sft == null) {
            reason.append("typeName not specified. ")
          }
          if (dates == null || dates.length != 2) {
            reason.append("date not specified or invalid. ")
          }
          if (binTry.isFailure) {
            reason.append("bin is not a valid Boolean. ")
          }
          BadRequest(reason = reason.toString())
        } else {
          val reader = ds.config.audit.get._1
          val interval = new Interval(dates(0), dates(1))
          // note: json response doesn't seem to handle iterators directly, have to convert to iterable
          val iter = reader.getEvents[QueryEvent](sft, interval)
          // we do the user filtering here, instead of in the tservers - revisit if performance becomes an issue
          // 'user' appears to be reserved by scalatra
          val filter: (QueryEvent) => Boolean = (params.get("who"), binTry.get) match {
            case (None, None)            => (s) => !s.deleted
            case (Some(user), None)      => (s) => !s.deleted && filterByUser(s, user)
            case (None, Some(bin))       => (s) => !s.deleted && filterByBin(s, bin)
            case (Some(user), Some(bin)) => (s) => !s.deleted && filterByUser(s, user) && filterByBin(s, bin)
          }
          iter.filter(filter).toIterable
        }
      })
    } catch {
      case e: Exception => handleError(s"Error reading queries:", e)
    }
  }

  delete("/:alias/queries/?") {
    try {
      withDataStore((ds) => {
        val sft = params.get("typeName").orNull
        // corresponds to 2015-11-01T00:00:00.000Z/2015-12-5T00:00:00.000Z - same as used by geotools
        val dates = params.get("dates").flatMap(d => Try(d.split("/").map(dtFormat.parseDateTime)).toOption).orNull
        if (sft == null || dates == null || dates.length != 2) {
          val reason = new StringBuilder
          if (sft == null) {
            reason.append("typeName not specified. ")
          }
          if (dates == null || dates.length != 2) {
            reason.append("date not specified or invalid. ")
          }
          BadRequest(reason = reason.toString())
        } else {
          val reader = ds.config.audit.get._1
          val interval = new Interval(dates(0), dates(1))
          // note: json response doesn't seem to handle iterators directly, have to convert to iterable
          val iter = reader.getEvents[SerializedQueryEvent](sft, interval)
          // we do the user filtering here, instead of in the tservers - revisit if performance becomes an issue
          // 'user' appears to be reserved by scalatra
          val filter: (SerializedQueryEvent) => Boolean = params.get("who") match {
            case None => (s) => !s.deleted
            case Some(user) => (s) => s.user == user && !s.deleted
          }
          iter.filter(filter).foreach { s =>
            reader.writeEvent(s.copy(deleted = true))
          }
          Ok()
        }
      })
    } catch {
      case e: Exception => handleError(s"Error reading queries:", e)
    }
  }

  private def filterByUser(stat: QueryEvent, user: String): Boolean = stat.user == user
  private def filterByBin(stat: QueryEvent, bin: Boolean): Boolean = stat.hints.contains(binHintString) == bin
}
