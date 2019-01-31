/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.security

import java.time.{Instant, ZoneOffset}
import java.util.Properties

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.iterators.user.WholeRowIterator
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, Formats, _}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, AccumuloEventWriter}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.cache.PropertiesPersistence
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.io.WithClose
import org.scalatra.test.specs2.MutableScalatraSpec
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryAuditEndpointTest extends TestWithDataStore with MutableScalatraSpec {

  sequential

  override val spec: String = "name:String,dtg:Date,*geom:Point:srid=4326"

  override def additionalDsParams(): Map[String, Any] = Map(AccumuloDataStoreParams.AuditQueriesParam.key -> true)

  private var startDate: String = _
  private var endDate: String = _

  val queries = Seq(
    new Query(sftName),
    { val q = new Query(sftName, ECQL.toFilter("bbox(geom,-10,-15,10,15)")); q.getHints.put(QueryHints.ARROW_ENCODE, true); q },
    { val q = new Query(sftName, ECQL.toFilter("bbox(geom,-10,-15,10,15) and dtg during 2017-01-01T00:00:00.000Z/2017-01-02T00:00:00.000Z")); q.getHints.put(QueryHints.BIN_TRACK, "name"); q }
  )
  var events: Seq[QueryEvent] = _

  step {
    startDate = GeoToolsDateFormat.format(Instant.now().minusSeconds(100).atZone(ZoneOffset.UTC))

    // turn up the write interval from the default 5 seconds
    AccumuloEventWriter.WriteInterval.threadLocalValue.set("100ms")

    // run some queries so we have data
    queries.foreach(ds.getFeatureReader(_, Transaction.AUTO_COMMIT).close())

    // wait for the async auditor to write out the queries
    val start = System.currentTimeMillis()
    val table = s"${catalog}_queries"
    var written = false
    while (!written && System.currentTimeMillis() - start < 5000) {
      import scala.collection.JavaConversions._
      Thread.sleep(100)
      if (ds.connector.tableOperations().exists(table)) {
        WithClose(ds.connector.createScanner(table, new Authorizations)) { scanner =>
          scanner.addScanIterator(new IteratorSetting(25, classOf[WholeRowIterator]))
          written = scanner.toSeq.length == queries.length
        }
      }
    }
    if (!written) {
      failure(s"Didn't see audited queries after 5 seconds")
    }

    endDate = GeoToolsDateFormat.format(Instant.now().plusSeconds(100).atZone(ZoneOffset.UTC))

    events = queries.map { query =>
      QueryEvent(
        AccumuloAuditService.StoreType,
        sftName,
        0L, // will vary, we'll overwrite in comparisons
        "unknown",
        filterToString(query.getFilter),
        ViewParams.getReadableHints(query),
        0L, // will vary, we'll overwrite in comparisons
        0L, // will vary, we'll overwrite in comparisons
        0L
      )
    }
  }

  // pre-load our mock connection parameters so the data store is available in the servlet
  val persistence = new PropertiesPersistence() {
    override protected def persist(properties: Properties): Unit = {}
    override protected def load(properties: Properties): Unit = {
      properties.put(s"ds.test.${AccumuloDataStoreParams.InstanceIdParam.key}", mockInstanceId)
      properties.put(s"ds.test.${AccumuloDataStoreParams.ZookeepersParam.key}", mockZookeepers)
      properties.put(s"ds.test.${AccumuloDataStoreParams.UserParam.key}", mockUser)
      properties.put(s"ds.test.${AccumuloDataStoreParams.PasswordParam.key}", mockPassword)
      properties.put(s"ds.test.${AccumuloDataStoreParams.CatalogParam.key}", catalog)
      properties.put(s"ds.test.${AccumuloDataStoreParams.MockParam.key}", "true")
    }
  }

  addServlet(new QueryAuditEndpoint(persistence), "/*")

  implicit val formats: Formats = DefaultFormats

  // note: zero-out the variable parts of the entry so we can compare to our expected queries
  def toListResponseList(body: String): List[QueryEvent] =
    parse(body).asInstanceOf[JArray].children.map(_.extract[QueryEvent]).map(_.copy(date = 0L, planTime = 0L, scanTime = 0L))

  "QueryAuditEndpoint" should {
    "check for required fields" in {
      get("/test/queries") {
        status mustEqual 400
        response.body must contain("typeName not specified")
        response.body must contain("date not specified or invalid")
      }
      get("/test/queries", ("typeName", sftName)) {
        status mustEqual 400
        response.body must contain("date not specified or invalid")
      }
      get("/test/queries", ("dates", "2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z")) {
        status mustEqual 400
        response.body must contain("typeName not specified")
      }
      get("/test/queries", ("typeName", sftName), ("dates", "2015/2016")) {
        status mustEqual 400
        response.body must contain("date not specified or invalid")
      }
      get("/test/queries", ("typeName", sftName), ("dates", "2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z"), ("arrow", "foo")) {
        status mustEqual 400
        response.body must contain("arrow is not a valid Boolean")
      }
      get("/test/queries", ("typeName", sftName), ("dates", "2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z"), ("bin", "foo")) {
        status mustEqual 400
        response.body must contain("bin is not a valid Boolean")
      }
    }
    "return empty list of queries" in {
      get("/test/queries", ("typeName", sftName), ("dates", "2015-11-01T00:00:00.000Z/2015-12-05T00:00:00.000Z")) {
        status mustEqual 200
        toListResponseList(body) mustEqual List.empty
      }
    }
    "return queries filtered by date" in {
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events
      }
    }
    "return queries filtered by arrow" in {
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "true")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.slice(1, 2)
      }
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "false")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.take(1) ++ events.drop(2)
      }
    }
    "return queries filtered by bin" in {
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("bin", "true")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.drop(2)
      }
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("bin", "false")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.take(2)
      }
    }
    "return queries filtered by arrow + bin" in {
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "true"), ("bin", "true")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.drop(1)
      }
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "true"), ("bin", "false")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.slice(1, 2)
      }
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "false"), ("bin", "true")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.drop(2)
      }
      get("/test/queries", ("typeName", sftName), ("dates", s"$startDate/$endDate"), ("arrow", "false"), ("bin", "false")) {
        status mustEqual 200
        toListResponseList(body) mustEqual events.take(1)
      }
    }
  }

  step {
    AccumuloEventWriter.WriteInterval.threadLocalValue.remove()
  }
}
