/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
class AccumuloEventWriterTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_writer_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  val auths = new Authorizations()

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val statReader = new AccumuloEventReader(connector, statsTable)

  "StatWriter" should {

    "write query stats asynchronously" in {
      val writer = new AccumuloEventWriter(connector, statsTable)
      implicit val transform = AccumuloQueryEventTransform

      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  df.parseMillis("2014.07.26 13:20:01"),
                                  "user1",
                                  "query1",
                                  "hint1=true",
                                  101L,
                                  201L,
                                  11))
      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  df.parseMillis("2014.07.26 14:20:01"),
                                  "user1",
                                  "query2",
                                  "hint2=true",
                                  102L,
                                  202L,
                                  12))
      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  df.parseMillis("2014.07.27 13:20:01"),
                                  "user1",
                                  "query3",
                                  "hint3=true",
                                  102L,
                                  202L,
                                  12))

      val dates = new Interval(0, df.parseMillis("2014.07.29 00:00:00"))
      val unwritten = statReader.query[QueryEvent](featureName, dates, auths).toList
      unwritten must not(beNull)
      unwritten.size mustEqual 0

      // this should write the queued stats
      writer.run()

      val written = statReader.query[QueryEvent](featureName, dates, auths).toList

      written must not(beNull)
      written.size mustEqual 3
    }
  }
}
