/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, AccumuloEventReader, AccumuloEventWriter, AccumuloQueryEventTransform}
import org.locationtech.geomesa.index.audit.QueryEvent
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UsageStatReaderTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_reader_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  implicit val transform = AccumuloQueryEventTransform

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val auths = new Authorizations()

  val writer = new AccumuloEventWriter(connector, statsTable)

  def writeStat(stats: Seq[QueryEvent], tableName: String) = {
    stats.foreach(writer.queueStat(_))
    writer.run()
  }

  "QueryStatReader" should {

    val stats = Seq(
      QueryEvent(AccumuloAuditService.StoreType, featureName, df.parseMillis("2014.07.26 13:20:01"), "user1", "query1", "hint1=true", 101L, 201L, 11),
      QueryEvent(AccumuloAuditService.StoreType, featureName, df.parseMillis("2014.07.26 14:20:01"), "user1", "query2", "hint2=true", 102L, 202L, 12),
      QueryEvent(AccumuloAuditService.StoreType, featureName, df.parseMillis("2014.07.27 13:20:01"), "user1", "query3", "hint3=true", 102L, 202L, 12)
    )
    writeStat(stats, statsTable)

    val reader = new AccumuloEventReader(connector, statsTable)

    "query all stats in order" in {
      val dates = new Interval(0, System.currentTimeMillis())
      val queries = reader.query[QueryEvent](featureName, dates, auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 3
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
      list(2).filter mustEqual "query3"
    }

    "query by day" in {
      val s = df.parseDateTime("2014.07.26 00:00:00")
      val e = df.parseDateTime("2014.07.26 23:59:59")
      val queries = reader.query[QueryEvent](featureName, new Interval(s, e), auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 2
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
    }

    "query by hour" in {
      val s = df.parseDateTime("2014.07.26 13:00:00")
      val e = df.parseDateTime("2014.07.26 13:59:59")
      val queries = reader.query[QueryEvent](featureName, new Interval(s, e), auths)

      queries must not(beNull)

      val list = queries.toList

      list.size mustEqual 1
      list(0).filter mustEqual "query1"
    }
  }

}
