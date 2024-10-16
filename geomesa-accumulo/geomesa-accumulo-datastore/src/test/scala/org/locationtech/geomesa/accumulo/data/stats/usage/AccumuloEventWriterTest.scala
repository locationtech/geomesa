/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import org.apache.accumulo.core.security.Authorizations
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditReader, AccumuloAuditWriter}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.runner.JUnitRunner

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Collections

@RunWith(classOf[JUnitRunner])
class AccumuloEventWriterTest extends TestWithDataStore {

  val featureName = "stat_writer_test"

  val auths = new Authorizations()

  "StatWriter" should {

    "write query stats asynchronously" in {
      // normally this is done by create schema
      ds.adapter.ensureNamespaceExists(catalog)
      // disable scheduled run, and call it manually
      AccumuloAuditWriter.WriteInterval.threadLocalValue.set("12 hours")
      val writer = try {
        new AccumuloAuditWriter(ds.connector, catalog, new AuditProvider {
          override def configure(params: java.util.Map[String, _]): Unit = {}
          override def getCurrentUserId: String = "user1"
          override def getCurrentUserDetails: java.util.Map[AnyRef, AnyRef] = null
        }, enabled = true)
      } finally {
        AccumuloAuditWriter.WriteInterval.threadLocalValue.remove()
      }
      val statReader = new AccumuloAuditReader(ds.connector, catalog, ds.config.authProvider)

      writer.writeQueryEvent(featureName, "user", ECQL.toFilter("IN('query1')"), new Hints(QueryHints.QUERY_INDEX, "z3"), Seq.empty, 0L, 10L, 101L, 201L, 11)
      writer.writeQueryEvent(featureName, "user", ECQL.toFilter("IN('query2')"), new Hints(QueryHints.ARROW_ENCODE, true), Seq.empty, 0L, 10L, 102L, 202L, 12)
      writer.writeQueryEvent(featureName, "user", ECQL.toFilter("IN('query3')"), new Hints(QueryHints.BIN_TRACK, "trackId"), Seq.empty, 0L, 10L, 102L, 202L, 12)

      val dates = (DateParsing.Epoch, ZonedDateTime.now(ZoneOffset.UTC).plusMinutes(1))
      val unwritten = WithClose(statReader.getQueryEvents(featureName, dates))(_.toList)
      unwritten must not(beNull)
      unwritten must beEmpty

      // write the queued stats
      writer.run()

      val written = WithClose(statReader.getQueryEvents(featureName, dates))(_.toList)
      written must not(beNull)
      written must haveSize(3)
    }
  }
}
