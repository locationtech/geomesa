/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils

import java.net.{ServerSocket, URI}
import javax.sql.DataSource
import scala.concurrent.duration.DurationInt
import scala.io.{Codec, Source}

class PartitionedPostgisDataStoreMetricsTest extends TestWithSharedPostgisContainer {

  lazy val metricsPort = getFreePort

  lazy val sft = SimpleFeatureTypes.createType("metrics", spec)

  lazy val features = Seq.tabulate(10) { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("name", java.util.List.of(s"name$i", s"alt$i"))
    builder.set("age", i)
    builder.set("props", s"""{"age": $i, "names": ["name$i"]}""") // note: formatted so that the compare with sql works
    builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
    builder.set("geom", WKTUtils.read(s"POINT(0 $i)"))
    val sf = builder.buildFeature(s"fid$i")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  def readMetrics(): Seq[String] = {
    val metrics = WithClose(Source.fromURL(new URI(s"http://localhost:$metricsPort/metrics").toURL)(Codec.UTF8))(_.getLines().toList)
    logger.whenDebugEnabled(metrics.foreach(m => logger.debug(m)))
    metrics
  }

  "PartitionedPostgisDataStore" should {

    "support metrics" in {
      val params =
        this.params ++ Map("geomesa.metrics.registry" -> "prometheus", "geomesa.metrics.registry.config" -> s"port = $metricsPort")
      WithClose(datastore(params)) { ds =>
        val dataSource = ds.getDataSource.unwrap(classOf[DataSource])
        dataSource must beAnInstanceOf[MetricsDataSource]

        testSchema(ds, "metrics") { sft =>
          writeFeatures(ds, sft)
          // read some data
          WithClose(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)) { reader =>
            CloseableIterator(reader).toList
          }
          val tagsRegex = s"""\\{.*name="${dataSource.asInstanceOf[MetricsDataSource].jmxName}".*\\}"""
          eventually(60, 2.seconds) {
            val metrics = readMetrics()
            metrics must contain(beMatching(s"""^commons_pool2_created_objects_total$tagsRegex 1\\.0$$"""))
            metrics must contain(beMatching(s"""^postgres_rows_inserted_total\\{application="geomesa",database="postgres"\\} \\d+\\.0$$"""))
          }
        }
      }
    }
  }

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }
}
