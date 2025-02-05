/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer.dbcp2

import io.micrometer.core.instrument.binder.commonspool2.CommonsObjectPool2Metrics
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import org.specs2.mutable.Specification

class MetricsDataSourceTest extends Specification {

  import scala.collection.JavaConverters._

  "MetricsDataSource" should {
    "register metrics for connection pooling" in {
      val registry = new CompositeMeterRegistry
      try {
        new CommonsObjectPool2Metrics().bindTo(registry)
        val ds = new MetricsDataSource()
        try {
          registry.getMeters.size() mustEqual 0
          ds.registerJmx()
          eventually(registry.getMeters.size() must beGreaterThan(0))
          registry.getMeters.asScala.count(_.getId.getName.startsWith("commons.pool2.")) must beGreaterThan(0)
        } finally {
          ds.close()
        }
      } finally {
        registry.close()
      }
    }
  }
}
