/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.net.{ServerSocket, URL}
import java.util.concurrent.atomic.AtomicInteger
import scala.io.{Codec, Source}

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreMetricsTest extends Specification with TestWithFeatureType with LazyLogging {

  override val spec = "name:String,age:Int,weight:Double,dtg:Date,*geom:Point:srid=4326"

  lazy val features = Seq.tabulate(5) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, i + 0.1, s"2018-01-02T00:0$i:01.000Z", "POINT (45 55)")
  }

  lazy private val port = getFreePort

  lazy private val metricsLog = new AtomicInteger(0)

  override def extraParams: Map[String, String] = Map(
    AccumuloDataStoreParams.MetricsRegistryParam.key -> "prometheus",
    AccumuloDataStoreParams.MetricsRegistryConfigParam.key -> s"port = $port",
  )

  step {
    addFeatures(features)
  }

  "AccumuloDataStore" should {

    "expose jvm metrics" in {
      val metrics = readMetrics()
      metrics must contain(beMatching(s"""^jvm_buffer_memory_used_bytes\\{.*\\} (0E[0-9.]+|[1-9][0-9.E]+)$$"""))
    }

    "expose metrics on write times" in {
      val metrics = readMetrics()
      metrics must contain(beMatching(s"""^geomesa_write_appends_seconds_count\\{.*catalog="$catalog".*store="accumulo".*\\} 5$$"""))
      metrics must contain(beMatching(s"""^geomesa_write_flushes_seconds_count\\{.*catalog="$catalog".*store="accumulo".*\\} 1$$"""))
    }

    "expose metrics on query times" in {
      val filter = ECQL.toFilter("bbox(geom,-179,-89,179,89)")
      val query = new Query(sft.getTypeName, filter)
      val results = SelfClosingIterator(fs.getFeatures(query).features).toList
      results must not(beEmpty)
      val metrics = readMetrics()
      metrics must contain(beMatching(s"""^geomesa_query_planning_seconds_bucket\\{.*catalog="$catalog".*store="accumulo".*\\} [1-9]+$$"""))
      metrics must contain(beMatching(s"""^geomesa_query_execution_seconds_bucket\\{.*catalog="$catalog".*store="accumulo".*\\} [1-9]+$$"""))
    }
  }

  private def readMetrics(): Seq[String] = {
    val lines = WithClose(Source.fromURL(new URL(s"http://localhost:$port/metrics"))(Codec.UTF8))(_.getLines().toList)
    logger.whenDebugEnabled {
      val i = metricsLog.getAndIncrement()
      lines.foreach(line => logger.debug(s"$i $line"))
    }
    lines
  }

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }
}
