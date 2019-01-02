/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.audit._
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UsageStatReaderTest extends Specification {

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_reader_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  implicit val transform: AccumuloEventTransform[QueryEvent] = AccumuloQueryEventTransform

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val auths = new Authorizations()

  val writer = new AccumuloEventWriter(connector, statsTable)

  def writeStat(stats: Seq[QueryEvent], tableName: String): Unit = {
    stats.foreach(writer.queueStat(_))
    writer.run()
  }

  "QueryStatReader" should {

    val stats = Seq(
      QueryEvent(AccumuloAuditService.StoreType, featureName, DateParsing.parseMillis("2014-07-26T13:20:01Z"), "user1", "query1", "hint1=true", 101L, 201L, 11),
      QueryEvent(AccumuloAuditService.StoreType, featureName, DateParsing.parseMillis("2014-07-26T14:20:01Z"), "user1", "query2", "hint2=true", 102L, 202L, 12),
      QueryEvent(AccumuloAuditService.StoreType, featureName, DateParsing.parseMillis("2014-07-27T13:20:01Z"), "user1", "query3", "hint3=true", 102L, 202L, 12)
    )
    writeStat(stats, statsTable)

    val reader = new AccumuloEventReader(connector, statsTable)

    "query all stats in order" in {
      val dates = (ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
      val queries = reader.query[QueryEvent](featureName, dates, auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 3
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
      list(2).filter mustEqual "query3"
    }

    "query by day" in {
      val s = DateParsing.parse("2014-07-26T00:00:00Z")
      val e = DateParsing.parse("2014-07-26T23:59:59Z")
      val queries = reader.query[QueryEvent](featureName, (s, e), auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 2
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
    }

    "query by hour" in {
      val s = DateParsing.parse("2014-07-26T13:00:00Z")
      val e = DateParsing.parse("2014-07-26T13:59:59Z")
      val queries = reader.query[QueryEvent](featureName, (s, e), auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 1
      list(0).filter mustEqual "query1"
    }
  }

}
