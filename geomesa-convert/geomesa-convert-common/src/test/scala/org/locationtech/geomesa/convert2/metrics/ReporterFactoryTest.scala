/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import com.codahale.metrics.{ConsoleReporter, MetricRegistry, Slf4jReporter}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReporterFactoryTest extends Specification with LazyLogging {

  "ReporterFactory" should {
    "load console configs" in {
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type = "console"
          |  units = "MILLISECONDS"
          |}
        """.stripMargin
      )
      val registry = new MetricRegistry()
      val reporter = ReporterFactory(conf, registry)
      try {
        reporter must beAnInstanceOf[ConsoleReporter]
      } finally {
        reporter.close()
      }
    }
    "load slf configs" in {
      val conf = ConfigFactory.parseString(
        """
          |{
          |  type           = "slf4j"
          |  logger         = "org.locationtech.geomesa.convert2.metrics.ReporterFactoryTest"
          |  level          = "INFO"
          |  rate-units     = "SECONDS"
          |  duration-units = "MILLISECONDS"
          |  interval       = "10 seconds"
          |}
        """.stripMargin
      )
      val registry = new MetricRegistry()
      val reporter = ReporterFactory(conf, registry)
      try {
        reporter must beAnInstanceOf[Slf4jReporter]
      } finally {
        reporter.close()
      }
    }
  }
}
