/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.analytics

import org.geotools.data.DataStoreFinder
import org.joda.time.Interval
import org.joda.time.format.ISODateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.stats.{QueryStat, QueryStatReader}
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.web.core.GeoMesaDataStoreServlet
import org.scalatra.BadRequest
import org.scalatra.json.NativeJsonSupport

import scala.collection.JavaConversions._
import scala.util.Try

class StatsEndpoint(val persistence: FilePersistence) extends GeoMesaDataStoreServlet with NativeJsonSupport {

  override def root: String = "stats"

  override def defaultFormat: Symbol = 'json
  override protected implicit def jsonFormats: Formats = DefaultFormats

  private val dtFormat = ISODateTimeFormat.dateTime().withZoneUTC()

  before() {
    contentType = formats(format)
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
      val ds = DataStoreFinder.getDataStore(datastoreParams).asInstanceOf[AccumuloDataStore]
      val sft = params.get("typeName").orNull
      // corresponds to 2015-11-01T00:00:00.000Z/2015-12-5T00:00:00.000Z - same as used by geotools
      val dates = params.get("dates").flatMap(d => Try(d.split("/").map(dtFormat.parseDateTime)).toOption).orNull
      if (ds == null || sft == null || dates == null || dates.length != 2) {
        val reason = new StringBuilder
        if (ds == null) {
          reason.append("Could not load data store using the provided parameters. ")
        }
        if (sft == null) {
          reason.append("typeName not specified. ")
        }
        if (dates == null || dates.length != 2) {
          reason.append("date not specified or invalid. ")
        }
        BadRequest(reason = reason.toString())
      } else {
        val getTable = (typeName: String) => {
          ds.getStatTable(new QueryStat(typeName, 0L, "", "", "", 0L, 0L, 0))
        }
        val reader = new QueryStatReader(ds.connector, getTable)
        val interval = new Interval(dates(0), dates(1))
        // json response doesn't seem to handle iterators directly, have to convert to iterable
        val iter = reader.query(sft, interval, ds.authProvider.getAuthorizations).toIterable
        // we do the user filtering here, instead of in the tservers - revisit if performance becomes an issue
        // 'user' appears to be reserved by scalatra
        params.get("who") match {
          case None => iter
          case Some(user) => iter.filter(_.user == user)
        }
      }
    } catch {
      case e: Exception => handleError(s"Error reading queries:", e)
    }
  }
}
