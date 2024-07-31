/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.security.Authorizations
import org.geotools.api.data.Query
import org.geotools.filter.text.cql2.CQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloContainer
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloQueryEventTransformTest extends Specification {

  import scala.collection.JavaConverters._

  lazy val connector = AccumuloContainer.Container.client()

  "AccumuloQueryEventTransform" should {
    "Convert from and to mutations" in {
      val event = QueryEvent(
        StoreType, // note: this isn't actually stored
        "type-name",
        System.currentTimeMillis(),
        "user",
        "filter",
        "hints",
        Long.MaxValue - 100,
        Long.MaxValue - 200,
        Long.MaxValue - 300
      )

      connector.tableOperations().create("AccumuloQueryEventTransformTest")

      WithClose(connector.createBatchWriter("AccumuloQueryEventTransformTest", new BatchWriterConfig())) { writer =>
        writer.addMutation(AccumuloQueryEventTransform.toMutation(event))
      }
      val restored = WithClose(connector.createScanner("AccumuloQueryEventTransformTest", new Authorizations)) { reader =>
        AccumuloQueryEventTransform.toEvent(reader.asScala)
      }

      restored mustEqual event
    }

    "convert hints to readable string" in {
      val query = new Query("test", CQL.toFilter("INCLUDE"))
      val env = new ReferencedEnvelope()
      query.getHints.put(QueryHints.DENSITY_BBOX, env)
      query.getHints.put(QueryHints.DENSITY_WIDTH, 500)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, 500)

      val hints = ViewParams.getReadableHints(query)

      hints must contain(s"DENSITY_BBOX=$env")
      hints must contain("DENSITY_WIDTH=500")
      hints must contain("DENSITY_HEIGHT=500")
    }
  }

  step {
    connector.close()
  }
}
