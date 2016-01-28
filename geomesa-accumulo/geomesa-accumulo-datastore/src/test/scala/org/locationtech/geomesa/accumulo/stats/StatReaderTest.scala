/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.stats

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.stats.StatWriter.{StatToWrite, TableInstance}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatReaderTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_reader_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val auths = new Authorizations()

  def writeStat(stats: Seq[Stat], tableName: String) =
    StatWriter.write(stats.map(s => StatToWrite(s, TableInstance(connector, tableName))))

  "QueryStatReader" should {

    val stats = Seq(
      QueryStat(featureName, df.parseMillis("2014.07.26 13:20:01"), "user1", "query1", "hint1=true", 101L, 201L, 11),
      QueryStat(featureName, df.parseMillis("2014.07.26 14:20:01"), "user1", "query2", "hint2=true", 102L, 202L, 12),
      QueryStat(featureName, df.parseMillis("2014.07.27 13:20:01"), "user1", "query3", "hint3=true", 102L, 202L, 12)
    )
    writeStat(stats, statsTable)

    val reader = new QueryStatReader(connector, (_: String) => statsTable)

    "query all stats in order" in {
      val queries = reader.query(featureName, new Interval(0, System.currentTimeMillis()), auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 3
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
      list(2).filter mustEqual "query3"
    }

    "query by day" in {
      val s = df.parseDateTime("2014.07.26 00:00:00")
      val e = df.parseDateTime("2014.07.26 23:59:59")
      val queries = reader.query(featureName, new Interval(s, e), auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 2
      list(0).filter mustEqual "query1"
      list(1).filter mustEqual "query2"
    }

    "query by hour" in {
      val s = df.parseDateTime("2014.07.26 13:00:00")
      val e = df.parseDateTime("2014.07.26 13:59:59")
      val queries = reader.query(featureName, new Interval(s, e), auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 1
      list(0).filter mustEqual "query1"
    }
  }

}
