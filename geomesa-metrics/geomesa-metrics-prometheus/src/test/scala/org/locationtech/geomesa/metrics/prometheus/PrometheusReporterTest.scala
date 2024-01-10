/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.prometheus

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.metrics.core.ReporterFactory
import org.locationtech.geomesa.metrics.prometheus.PrometheusReporterFactory.{PrometheusReporter, PushgatewayReporter}
import org.locationtech.geomesa.utils.io.WithClose
import org.mortbay.jetty.{Request, Server}
import org.mortbay.jetty.handler.AbstractHandler
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class PrometheusReporterTest extends Specification {

  "Prometheus reporter" should {
    "expose metrics over http" in {
      val conf = ConfigFactory.parseString("{ type = prometheus, port = 0 }") // use zero for random port
      val registry = new MetricRegistry()
      val reporter = ReporterFactory(conf, registry)
      try {
        reporter must beAnInstanceOf[PrometheusReporter]
        val port = reporter.asInstanceOf[PrometheusReporter].getPort

        registry.counter("foo").inc(10)

        val metrics = ArrayBuffer.empty[String]
        val url = new URL(s"http://localhost:$port/metrics")
        WithClose(new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) { is =>
          var line = is.readLine()
          while (line != null) {
            metrics += line
            line = is.readLine()
          }
        }

        metrics must contain("foo 10.0")
      } finally {
        reporter.close()
      }
    }

    "expose metrics over http with custom suffix" in {
      val conf = ConfigFactory.parseString("{ type = prometheus, port = 0, suffix = bar }") // use zero for random port
      val registry = new MetricRegistry()
      val reporter = ReporterFactory(conf, registry)
      try {
        reporter must beAnInstanceOf[PrometheusReporter]
        val port = reporter.asInstanceOf[PrometheusReporter].getPort

        registry.counter("foo").inc(10)

        val metrics = ArrayBuffer.empty[String]
        val url = new URL(s"http://localhost:$port/metrics")
        WithClose(new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) { is =>
          var line = is.readLine()
          while (line != null) {
            metrics += line
            line = is.readLine()
          }
        }

        metrics must contain("foobar 10.0")
      } finally {
        reporter.close()
      }
    }

    "push metrics to a gateway" in {
      val handler = new PgHandler()
      val jetty = new Server(0)
      jetty.setHandler(handler)
      try {
        jetty.start()
        val port = jetty.getConnectors()(0).getLocalPort
        val conf =
          ConfigFactory.parseString(
            s"""{ type = prometheus-pushgateway, gateway = "http://localhost:$port", job-name = job1 }""")
        val registry = new MetricRegistry()
        val reporter = ReporterFactory(conf, registry)
        try {
          reporter must beAnInstanceOf[PushgatewayReporter]
          registry.counter("foo").inc(10)
        } finally {
          reporter.close()
        }
        handler.requests.keys must contain("/metrics/job/job1")
        val metrics = handler.requests("/metrics/job/job1")
        metrics must contain("foo 10.0")
      } finally {
        jetty.stop()
      }
    }

    "push metrics to a gateway with suffix" in {
      val handler = new PgHandler()
      val jetty = new Server(0)
      jetty.setHandler(handler)
      try {
        jetty.start()
        val port = jetty.getConnectors()(0).getLocalPort
        val conf =
          ConfigFactory.parseString(
            s"""{ type = prometheus-pushgateway, gateway = "http://localhost:$port", job-name = job1, suffix = bar }""")
        val registry = new MetricRegistry()
        val reporter = ReporterFactory(conf, registry)
        try {
          reporter must beAnInstanceOf[PushgatewayReporter]
          registry.counter("foo").inc(10)
        } finally {
          reporter.close()
        }
        handler.requests.keys must contain("/metrics/job/job1")
        val metrics = handler.requests("/metrics/job/job1")
        metrics must contain("foobar 10.0")
      } finally {
        jetty.stop()
      }
    }
  }

  class PgHandler extends AbstractHandler {
    val requests: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    override def handle(s: String, req: HttpServletRequest, resp: HttpServletResponse, i: Int): Unit = {
      val is = req.getInputStream
      try { requests += req.getPathInfo -> IOUtils.toString(is, StandardCharsets.UTF_8) } finally {
        is.close()
      }
      resp.setStatus(HttpServletResponse.SC_OK)
      req.asInstanceOf[Request].setHandled(true)
    }
  }
}
