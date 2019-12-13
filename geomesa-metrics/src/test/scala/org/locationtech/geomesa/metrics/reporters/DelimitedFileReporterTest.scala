/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.reporters

import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics._
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class DelimitedFileReporterTest extends Specification {

  sequential

  val registry = new MetricRegistry()
  val folder = Files.createTempDirectory("geomesa-metrics").toFile
  val reporter = DelimitedFileReporter.forRegistry(registry)
      .aggregate(true)
      .withTabs()
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(folder.getAbsolutePath)

  def read(metric: String, name: String): Seq[Array[String]] = {
    val source = Source.fromFile(new File(folder, s"$metric.tsv"))
    try {
      source.getLines().toList.map(_.split("\t")).filter(_.apply(1) == name).map(_.drop(2)) // drop name and timestamp
    } finally {
      source.close()
    }
  }

  "DelimitedFileReporter" should {
    "report gauges" >> {
      val name = "mygauge"

      registry.register(name, new Gauge[String] {
        override def getValue: String = "value1"
      })

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = read("gauges", name)

      entries must haveLength(1)
      entries.head must haveLength(1)
      entries.head(0) mustEqual "value1"
    }

    "report counters" >> {
      val name = "mycounter"

      val metric = registry.counter(name)
      metric.inc(10)

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = read("counters", name)

      entries must haveLength(1)
      entries.head must haveLength(1)
      entries.head(0) mustEqual "10"
    }

    "report histograms" >> {
      val name = "myhistogram"

      val metric = registry.histogram(name)
      (0 until 10).foreach(metric.update)

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = read("histograms", name)

      entries must haveLength(1)
      entries.head must haveLength(11)

      entries.head(0).toDouble mustEqual 10.0 // count
      entries.head(1).toDouble mustEqual 0.0 // min
      entries.head(2).toDouble mustEqual 9.0 // max
      entries.head(3).toDouble mustEqual 4.5 // mean
      entries.head(4).toDouble mustEqual 2.87 // std dev
      entries.head(5).toDouble mustEqual 5.0 // median
      entries.head(6).toDouble mustEqual 7.0 // 75th
      entries.head(7).toDouble mustEqual 9.0 // 95th
      entries.head(8).toDouble mustEqual 9.0 // 98th
      entries.head(9).toDouble mustEqual 9.0 // 99th
      entries.head(10).toDouble mustEqual 9.0 // 999th
    }

    "report meters" >> {
      val name = "mymeter"

      val tick = new AtomicLong(0)
      val clock = new Clock { override def getTick: Long = tick.get * 1000 } // tick is in nanos - we use millis

      val metric = registry.register(name, new Meter(clock))
      (0 until 10).foreach { i => tick.addAndGet(i); metric.mark() }

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = read("meters", name)

      entries must haveLength(1)
      entries.head must haveLength(6)

      entries.head(0).toDouble mustEqual 10.0
      entries.head(1).toDouble mustEqual 222222.22
      entries.head(2).toDouble mustEqual 0.0
      entries.head(3).toDouble mustEqual 0.0
      entries.head(4).toDouble mustEqual 0.0
      entries.head(5) mustEqual "events/second"
    }

    "report timers" >> {
      val name = "mytimer"

      val tick = new AtomicLong(0)
      val clock = new Clock { override def getTick: Long = tick.get * 1000000 } // tick is in nanos - we use seconds

      val metric = registry.register(name, new Timer(new SlidingWindowReservoir(100), clock))
      (0 until 10).foreach { i =>
        tick.addAndGet(i)
        val c = metric.time()
        tick.addAndGet(i)
        c.stop()
      }

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = read("timers", name)

      entries must haveLength(1)
      entries.head must haveLength(17)

      entries.head(0).toDouble mustEqual 10.0
      entries.head(1).toDouble mustEqual 0.0
      entries.head(2).toDouble mustEqual 9.0
      entries.head(3).toDouble mustEqual 4.5
      entries.head(4).toDouble mustEqual 3.03
      entries.head(5).toDouble mustEqual 4.5
      entries.head(6).toDouble mustEqual 7.25
      entries.head(7).toDouble mustEqual 9.0
      entries.head(8).toDouble mustEqual 9.0
      entries.head(9).toDouble mustEqual 9.0
      entries.head(10).toDouble mustEqual 9.0
      entries.head(11).toDouble mustEqual 111.11
      entries.head(12).toDouble mustEqual 0.0
      entries.head(13).toDouble mustEqual 0.0
      entries.head(14).toDouble mustEqual 0.0
      entries.head(15) mustEqual "calls/second"
      entries.head(16) mustEqual "milliseconds"
    }
  }

  step {
    reporter.stop()
    FileUtils.deleteDirectory(folder) // recursive delete of contents
  }
}
