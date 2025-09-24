/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.metrics.micrometer.MicrometerSetup.MetricsConfig
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetricsConfigTest extends Specification {

  "MetricsConfig" should {
    "parse configs" in {
      val reg =
        """{
           |  type = "prometheus"
           |  enabled = false
           |  rename = true
           |  common-tags = { "application" = "test" }
           |  port = 9090
           |}
           |""".stripMargin
      val instr =
        """{
          |  classloader  = { enabled = true, tags = {} }
          |  memory       = { enabled = true, tags = {} }
          |  gc           = { enabled = false, tags = {} }
          |  processor    = { enabled = true, tags = { "processor" = "foo" } }
          |  threads      = { enabled = true, tags = {} }
          |}
          |""".stripMargin
      val conf = ConfigFactory.parseString(
        s"""{
           |  enabled = true
           |  instrumentations = $instr
           |  registries = {
           |    prometheus = $reg
           |  }
           |}
           |""".stripMargin)
      val config = MetricsConfig(conf)
      config.registries must haveSize(1)
      config.registries.get("prometheus") must beSome
      config.registries("prometheus") mustEqual ConfigFactory.parseString(reg)
      config.instrumentations mustEqual ConfigFactory.parseString(instr)
    }

    "parse deprecated configs" in {
      val reg =
        """
          |{
          |  type = "prometheus"
          |  enabled = false
          |  rename = true
          |  common-tags = { "application" = "test" }
          |  port = 9090
          |}
          |""".stripMargin
      val instr =
        """{
          |  classloader  = { enabled = true, tags = {} }
          |  memory       = { enabled = true, tags = {} }
          |  gc           = { enabled = false, tags = {} }
          |  processor    = { enabled = true, tags = { "processor" = "foo" } }
          |  threads      = { enabled = true, tags = {} }
          |}
          |""".stripMargin
      val conf = ConfigFactory.parseString(
        s"""{
           |  enabled = true
           |  instrumentations = $instr
           |  registries = [
           |    $reg
           |  ]
           |}
           |""".stripMargin)
      val config = MetricsConfig(conf)
      config.registries must haveSize(1)
      config.registries.get("0") must beSome
      config.registries("0") mustEqual ConfigFactory.parseString(reg)
      config.instrumentations mustEqual ConfigFactory.parseString(instr)
    }
  }
}
