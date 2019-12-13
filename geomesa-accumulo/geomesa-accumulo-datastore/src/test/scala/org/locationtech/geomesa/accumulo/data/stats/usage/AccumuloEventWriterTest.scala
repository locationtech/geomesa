/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, AccumuloEventReader, AccumuloEventWriter, AccumuloQueryEventTransform}
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloEventWriterTest extends Specification {

  val catalogTable = "geomesa_catalog"
  val featureName = "stat_writer_test"
  val statsTable = s"${catalogTable}_${featureName}_queries"

  val auths = new Authorizations()

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val statReader = new AccumuloEventReader(connector, statsTable)

  "StatWriter" should {

    "write query stats asynchronously" in {
      val writer = new AccumuloEventWriter(connector, statsTable)
      implicit val transform: AccumuloQueryEventTransform.type = AccumuloQueryEventTransform

      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  DateParsing.parseMillis("2014-07-26T13:20:01Z"),
                                  "user1",
                                  "query1",
                                  "hint1=true",
                                  101L,
                                  201L,
                                  11))
      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  DateParsing.parseMillis("2014-07-26T14:20:01Z"),
                                  "user1",
                                  "query2",
                                  "hint2=true",
                                  102L,
                                  202L,
                                  12))
      writer.queueStat(QueryEvent(AccumuloAuditService.StoreType,
                                  featureName,
                                  DateParsing.parseMillis("2014-07-27T13:20:01Z"),
                                  "user1",
                                  "query3",
                                  "hint3=true",
                                  102L,
                                  202L,
                                  12))

      val dates = (DateParsing.Epoch, DateParsing.parse("2014-07-29T00:00:00Z"))
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
