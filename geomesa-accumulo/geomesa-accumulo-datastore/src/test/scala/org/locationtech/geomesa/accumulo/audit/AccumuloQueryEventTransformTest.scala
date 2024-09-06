/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.api.data.Query
import org.geotools.filter.text.cql2.CQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloContainer
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.util.Collections

@RunWith(classOf[JUnitRunner])
class AccumuloQueryEventTransformTest extends Specification {

  import scala.collection.JavaConverters._

  lazy val connector = AccumuloContainer.Container.client()

  val table = "AccumuloQueryEventTransformTest"

  step {
    connector.tableOperations().create(table)
  }

  "AccumuloQueryEventTransform" should {
    "Convert from and to mutations" in {
      val event = QueryEvent(
        StoreType, // note: this isn't actually stored
        "type-name",
        "user",
        "filter",
        java.util.Map.of("HINT_KEY", "hint-value"),
        java.util.Map.of("metadata", java.util.List.of("v1", "v2")),
        System.currentTimeMillis() - 10,
        System.currentTimeMillis(),
        Long.MaxValue - 100,
        Long.MaxValue - 200,
        Long.MaxValue - 300
      )

      WithClose(connector.createBatchWriter(table, new BatchWriterConfig())) { writer =>
        writer.addMutation(AccumuloQueryEventTransform.toMutation(event))
      }
      val restored = WithClose(connector.createScanner(table, new Authorizations)) { reader =>
        reader.setRange(new org.apache.accumulo.core.data.Range("type-name", "type-name~~"))
        AccumuloQueryEventTransform.toEvent(reader.asScala)
      }

      restored mustEqual event
    }

    "Convert from old format mutations" in {
      WithClose(connector.createBatchWriter(table, new BatchWriterConfig())) { writer =>
        val mutation = new Mutation(s"old-type-name~${DateParsing.formatMillis(1000L, AccumuloEventTransform.DateFormat)}")
        val cf = new Text("0000")
        mutation.put(cf, new Text("user"),         new Value("user".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("queryFilter"),  new Value("INCLUDE".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("queryHints"),   new Value("DENSITY_WIDTH=500, DENSITY_HEIGHT=500, unknown_hint=ANY".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("timePlanning"), new Value("100ms".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("timeScanning"), new Value("200ms".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("timeTotal"),    new Value("300ms".getBytes(StandardCharsets.UTF_8)))
        mutation.put(cf, new Text("hits"),         new Value("101".getBytes(StandardCharsets.UTF_8)))
        writer.addMutation(mutation)
      }
      val restored = WithClose(connector.createScanner(table, new Authorizations)) { reader =>
        reader.setRange(new org.apache.accumulo.core.data.Range("old-type-name", "old-type-name~~"))
        AccumuloQueryEventTransform.toEvent(reader.asScala)
      }

      val expectedHints = java.util.Map.of("DENSITY_WIDTH", "500", "DENSITY_HEIGHT", "500", "unknown_hint", "ANY")
      restored mustEqual
        QueryEvent(StoreType, "old-type-name", "user", "INCLUDE", expectedHints, Collections.emptyMap(), -1L, 1000, 100, 200, 101)
    }

    "convert hints to readable string" in {
      val query = new Query("test", CQL.toFilter("INCLUDE"))
      val env = new ReferencedEnvelope()
      query.getHints.put(QueryHints.DENSITY_BBOX, env)
      query.getHints.put(QueryHints.DENSITY_WIDTH, 500)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, 500)

      val hints = ViewParams.getReadableHints(query).asScala

      hints must contain("DENSITY_BBOX" -> env.toString)
      hints must contain("DENSITY_WIDTH" -> "500")
      hints must contain("DENSITY_HEIGHT" -> "500")
    }
  }

  step {
    connector.close()
  }
}
