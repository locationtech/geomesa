/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.security

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import org.json4s.{DefaultFormats, Formats, JValue}
import org.locationtech.geomesa.accumulo.audit.SerializedQueryEvent
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.cache.PropertiesPersistence
import org.locationtech.geomesa.web.core.GeoMesaDataStoreServlet
import org.scalatra.json.NativeJsonSupport
import org.scalatra.{BadRequest, Ok}

import scala.util.Try

@deprecated
class QueryAuditEndpoint(val persistence: PropertiesPersistence)
    extends GeoMesaDataStoreServlet with NativeJsonSupport {

  import QueryAuditEndpoint._

  override def root: String = "stats"

  override protected implicit def jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  // filter out the internal 'deleted' flag
  protected override def transformResponseBody(body: JValue): JValue = body.removeField {
    case ("deleted", _) => true
    case _              => false
  }

  /**
   * Retrieves stored query stats.
   *
   * params:
   *   'typeName' - simple feature type to query
   *   'dates' - date range for results - format is 2015-11-01T00:00:00.000Z/2015-12-5T00:00:00.000Z
   *   'who' - (optional) user to filter on
    *  'bin' - (optional) only return BIN queries
    *  'arrow' - (optional) only return arrow queries
   */
  get("/:alias/queries/?") {
    try {
      withDataStore((ds: AccumuloDataStore) => {
        val sft = params.get("typeName").orNull
        // corresponds to 2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z - same as used by geotools
        val dates = params.get("dates").flatMap(d => Try(d.split("/").map(ZonedDateTime.parse(_, DtFormat))).toOption).orNull
        val binTry = Try(params.get("bin").map(_.toBoolean))
        val arrowTry = Try(params.get("arrow").map(_.toBoolean))
        if (sft == null || dates == null || dates.length != 2 || binTry.isFailure || arrowTry.isFailure) {
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
          if (arrowTry.isFailure) {
            reason.append("arrow is not a valid Boolean. ")
          }
          BadRequest(body = reason.toString())
        } else {
          val reader = ds.config.audit.get._1
          val interval = (dates(0), dates(1))
          // note: json response doesn't seem to handle iterators directly, have to convert to iterable
          val iter = reader.getEvents[QueryEvent](sft, interval)
          // we do the user filtering here, instead of in the tservers - revisit if performance becomes an issue
          // 'user' appears to be reserved by scalatra
          val typeFilter: Option[(QueryEvent) => Boolean] = (arrowTry.get, binTry.get) match {
            case (None, None) => None
            case (Some(true), Some(true)) => Some((s) => filterByArrow(s, arrow = true) || filterByBin(s, bin = true))
            case (Some(true), _) => Some((s) => filterByArrow(s, arrow = true))
            case (_, Some(true)) => Some((s) => filterByBin(s, bin = true))
            case (Some(false), Some(false)) => Some((s) => filterByArrow(s, arrow = false) && filterByBin(s, bin = false))
            case (Some(false), None) => Some((s) => filterByArrow(s, arrow = false))
            case (None, Some(false)) => Some((s) => filterByBin(s, bin = false))
          }
          val filter: (QueryEvent) => Boolean = (params.get("who"), typeFilter) match {
            case (None, None)       => (s) => !s.deleted
            case (Some(u), None)    => (s) => !s.deleted && filterByUser(s, u)
            case (None, Some(f))    => (s) => !s.deleted && f.apply(s)
            case (Some(u), Some(f)) => (s) => !s.deleted && filterByUser(s, u) && f.apply(s)
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
      withDataStore((ds: AccumuloDataStore) => {
        val sft = params.get("typeName").orNull
        // corresponds to 2015-11-01T00:00:00.000Z/2015-12-5T00:00:00.000Z - same as used by geotools
        val dates = params.get("dates").flatMap(d => Try(d.split("/").map(ZonedDateTime.parse(_, DtFormat))).toOption).orNull
        if (sft == null || dates == null || dates.length != 2) {
          val reason = new StringBuilder
          if (sft == null) {
            reason.append("typeName not specified. ")
          }
          if (dates == null || dates.length != 2) {
            reason.append("date not specified or invalid. ")
          }
          BadRequest(body = reason.toString())
        } else {
          val reader = ds.config.audit.get._1
          val interval = (dates(0), dates(1))
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
}

object QueryAuditEndpoint {

  private val DtFormat = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)

  private val ArrowHintString = ViewParams.hintToString(QueryHints.ARROW_ENCODE)
  private val BinHintString = ViewParams.hintToString(QueryHints.BIN_TRACK)
  private val OldBinHintString = "BIN_TRACK_KEY"

  private def filterByUser(stat: QueryEvent, user: String): Boolean = stat.user == user

  private def filterByBin(stat: QueryEvent, bin: Boolean): Boolean =
    (stat.hints.contains(BinHintString) || stat.hints.contains(OldBinHintString)) == bin

  private def filterByArrow(stat: QueryEvent, arrow: Boolean): Boolean =
    stat.hints.contains(ArrowHintString) == arrow
}