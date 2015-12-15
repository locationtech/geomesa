/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.stats

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, TableNotFoundException}
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatWriterTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_writer_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  val auths = new Authorizations()

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  // mock class we can extend with statwriter
  class MockWriter(c: Connector) {
    val connector = c
    def getStatTable(stat: Stat) = statsTable
  }

  val statReader = new QueryStatReader(connector, (_: String) => statsTable)

  "StatWriter" should {

    "write query stats asynchronously" in {
      skipped("concurrency issues cause intermittent failures- GEOMESA-323")
      val writer = new MockWriter(connector) with StatWriter

      writer.writeStat(QueryStat(featureName,
                                 df.parseMillis("2014.07.26 13:20:01"),
                                 "user1",
                                 "query1",
                                 "hint1=true",
                                 101L,
                                 201L,
                                 11))
      writer.writeStat(QueryStat(featureName,
                                 df.parseMillis("2014.07.26 14:20:01"),
                                 "user1",
                                 "query2",
                                 "hint2=true",
                                 102L,
                                 202L,
                                 12))
      writer.writeStat(QueryStat(featureName,
                                 df.parseMillis("2014.07.27 13:20:01"),
                                 "user1",
                                 "query3",
                                 "hint3=true",
                                 102L,
                                 202L,
                                 12))

      try {
        val unwritten = statReader.query(featureName, new Interval(0, df.parseMillis("2014.07.29 00:00:00")), auths).toList
        unwritten must not beNull;
        unwritten.size mustEqual 0
      } catch {
        case e: TableNotFoundException => // table doesn't exist yet, since no stats are written
      }

      // this should write the queued stats
      StatWriter.run()

      val written = statReader.query(featureName, new Interval(0, df.parseMillis("2014.07.29 00:00:00")), auths).toList

      written must not beNull;
      written.size mustEqual 3
    }
  }
}
