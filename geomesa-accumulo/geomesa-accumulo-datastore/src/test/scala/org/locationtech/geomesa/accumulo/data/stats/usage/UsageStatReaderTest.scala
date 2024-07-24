/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloContainer
import org.locationtech.geomesa.accumulo.audit._
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.security.DefaultAuthorizationsProvider
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class UsageStatReaderTest extends Specification {

  val featureName = "stat_reader_test"

  lazy val client = AccumuloContainer.Container.client()
  // noinspection TypeAnnotation
  lazy val writer = new AccumuloAuditWriter(client, "UsageStatReaderTest_queries", new ParamsAuditProvider, enabled = true) {
    private val timestamps = new ThreadLocal[Long]()
    def writeWithTimestamp(filter: Filter, hints: Hints, planTime: Long, scanTime: Long, hits: Long, timestamp: Long): Future[Unit] = {
      timestamps.set(timestamp)
      try { writeQueryEvent(featureName, filter, hints, planTime, scanTime, hits) } finally {
        timestamps.remove()
      }
    }
    override protected def write(event: QueryEvent): Future[Unit] = super.write(event.copy(date = timestamps.get()))
  }
  lazy val reader = new AccumuloAuditReader(client, writer.table, new DefaultAuthorizationsProvider())

  step {
    writer.writeWithTimestamp(ECQL.toFilter("IN('query1')"), new Hints(QueryHints.QUERY_INDEX, "z3"), 101L, 201L, 11,
      DateParsing.parseMillis("2014-07-26T13:20:01Z"))
    writer.writeWithTimestamp(ECQL.toFilter("IN('query2')"), new Hints(QueryHints.ARROW_ENCODE, true), 102L, 202L, 12,
      DateParsing.parseMillis("2014-07-26T14:20:01Z"))
    writer.writeWithTimestamp(ECQL.toFilter("IN('query3')"), new Hints(QueryHints.BIN_TRACK, "trackId"), 102L, 202L, 12,
      DateParsing.parseMillis("2014-07-27T13:20:01Z"))
    writer.run()
  }

  "QueryStatReader" should {

    "query all stats in order" in {
      val dates = (ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
      val queries = WithClose(reader.getQueryEvents(featureName, dates))(_.toList)

      queries must haveSize(3)
      queries(0).filter mustEqual "IN ('query1')"
      queries(1).filter mustEqual "IN ('query2')"
      queries(2).filter mustEqual "IN ('query3')"
    }

    "query by day" in {
      val s = DateParsing.parse("2014-07-26T00:00:00Z")
      val e = DateParsing.parse("2014-07-26T23:59:59Z")
      val queries = WithClose(reader.getQueryEvents(featureName, (s, e)))(_.toList)

      queries must haveSize(2)
      queries(0).filter mustEqual "IN ('query1')"
      queries(1).filter mustEqual "IN ('query2')"
    }

    "query by hour" in {
      val s = DateParsing.parse("2014-07-26T13:00:00Z")
      val e = DateParsing.parse("2014-07-26T13:59:59Z")
      val queries = WithClose(reader.getQueryEvents(featureName, (s, e)))(_.toList)

      queries must haveSize(1)
      queries(0).filter mustEqual "IN ('query1')"
    }
  }

  step {
    reader.close()
    writer.close()
    client.close()
  }
}
