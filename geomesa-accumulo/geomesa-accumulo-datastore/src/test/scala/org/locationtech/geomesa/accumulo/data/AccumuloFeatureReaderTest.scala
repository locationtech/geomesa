/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditWriter, ParamsAuditProvider}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.audit.AuditedEvent
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureReaderTest extends Specification with TestWithFeatureType {

  override def spec = s"name:String,dtg:Date,*geom:Point"

  val features = (0 until 100).map { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.setAttribute(0, s"name$i")
    sf.setAttribute(1, f"2010-05-07T${i % 24}%02d:01:00.000Z")
    sf.setAttribute(2, s"POINT(${i % 10} ${i % 5})")
    sf
  }

  step {
    addFeatures(features)
  }

  val filter = ECQL.toFilter("bbox(geom, -10, -10, 10, 10) and dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z")

  def dataStoreWithAudit(events: ArrayBuffer[AuditedEvent]) =
    new AccumuloDataStore(ds.connector, ds.config.copy(auditWriter = new MockAuditWriter(events)))

  class MockAuditWriter(events: ArrayBuffer[AuditedEvent])
    extends AccumuloAuditWriter(null, "", new ParamsAuditProvider, enabled = false) {
    override def writeQueryEvent(
        typeName: String,
        user: String,
        filter: Filter,
        hints: Hints,
        plans: Seq[QueryPlan[_]],
        startTime: Long,
        endTime: Long,
        planTime: Long,
        scanTime: Long,
        hits: Long): Future[Unit] = {
      Future.successful(events.append(QueryEvent("accumulo-vector", typeName, user, filter, hints, startTime, endTime, planTime, scanTime, hits)))
    }
  }

  "AccumuloFeatureReader" should {
    "be able to run without stats" in {
      val query = new Query(sftName, filter)

      var count = 0
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 100
    }

    "be able to collect stats" in {
      val events = ArrayBuffer.empty[AuditedEvent]
      val query = new Query(sftName, filter)

      var count = 0
      val reader = dataStoreWithAudit(events).getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 100
      events must haveLength(1)
      events.head must beAnInstanceOf[QueryEvent]
      events.head.asInstanceOf[QueryEvent].hits mustEqual 100
    }

    "be able to count bin results in stats" in {
      val events = ArrayBuffer.empty[AuditedEvent]

      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE, 10)

      var count = 0
      val reader = dataStoreWithAudit(events).getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(100)
      events must haveLength(1)
      events.head must beAnInstanceOf[QueryEvent]
      events.head.asInstanceOf[QueryEvent].hits mustEqual 100
    }

    "be able to limit features" in {
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      var count = 0
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 10
    }

    "be able to limit features in bin results" in {
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE, 10)
      query.setMaxFeatures(10)

      var count = 0
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { count += reader.next().getAttribute(0).asInstanceOf[Array[Byte]].length }
      reader.close()

      count mustEqual 10 * 16 // 16 bytes per bin record
    }

    "be able to limit features and collect stats" in {
      val events = ArrayBuffer.empty[AuditedEvent]
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      var count = 0
      val reader = dataStoreWithAudit(events).getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 10
      events must haveLength(1)
      events.head must beAnInstanceOf[QueryEvent]
      events.head.asInstanceOf[QueryEvent].hits mustEqual 10
    }

    "be able to limit features in bin results and collect stats" in {
      val events = ArrayBuffer.empty[AuditedEvent]
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE, 10)
      query.setMaxFeatures(10)

      var count = 0
      val reader = dataStoreWithAudit(events).getFeatureReader(query, Transaction.AUTO_COMMIT)
      while (reader.hasNext) { count += reader.next().getAttribute(0).asInstanceOf[Array[Byte]].length }
      reader.close()

      count mustEqual 10 * 16 // 16 bytes per bin record
      events must haveLength(1)
      events.head must beAnInstanceOf[QueryEvent]
      events.head.asInstanceOf[QueryEvent].hits must beLessThan(20L)
    }
  }
}
