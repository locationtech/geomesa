/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.config

import java.nio.file.Files

import com.codahale.metrics.ganglia.GangliaReporter
import com.codahale.metrics.graphite.GraphiteReporter
import com.codahale.metrics.{ConsoleReporter, MetricRegistry, Slf4jReporter}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.metrics.reporters.{AccumuloReporter, DelimitedFileReporter}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetricsConfigTest extends Specification {

  val folder = Files.createTempDirectory("geomesa-metrics-config").toFile
  val config = ConfigFactory.load("config-test")
      .withValue("geomesa.metrics.reporters.delimited-text.output",
        ConfigValueFactory.fromAnyRef(folder.getAbsolutePath))

  "MetricsConfig" should {
    "configure console reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.console").atKey("console")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[ConsoleReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure slf4j reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.slf4j").atKey("slf4j")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[Slf4jReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure delimited text reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.delimited-text").atKey("delimited-text")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[DelimitedFileReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure ganglia reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.ganglia").atKey("ganglia")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[GangliaReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure graphite reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.graphite").atKey("graphite")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[GraphiteReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure accumulo reporter" >> {
      val c = config.getConfig(s"${MetricsConfig.ConfigPath}.accumulo").atKey("accumulo")
      val reporters = MetricsConfig.reporters(c, new MetricRegistry, None, start = false)
      try {
        reporters must haveLength(1)
        reporters.head must beAnInstanceOf[AccumuloReporter]
      } finally {
        reporters.foreach(_.stop())
      }
    }

    "configure multiple reporters" >> {
      val reporters = MetricsConfig.reporters(config, new MetricRegistry, start = false)
      try {
        reporters must haveLength(6)
        reporters.map(_.getClass) must containTheSameElementsAs(Seq(classOf[ConsoleReporter],
          classOf[Slf4jReporter], classOf[DelimitedFileReporter], classOf[GangliaReporter],
          classOf[GraphiteReporter], classOf[AccumuloReporter]))
      } finally {
        reporters.foreach(_.stop())
      }
    }
  }

  step {
    FileUtils.deleteDirectory(folder) // recursive delete
  }
}
