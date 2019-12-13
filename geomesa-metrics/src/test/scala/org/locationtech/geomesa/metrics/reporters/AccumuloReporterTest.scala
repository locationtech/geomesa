/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.reporters

import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.junit.runner.RunWith
import org.locationtech.geomesa.metrics.reporters.AccumuloReporter.Keys
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloReporterTest extends Specification {

  sequential

  val registry = new MetricRegistry()
  val connector = new MockInstance("AccumuloReporterTest").getConnector("root", new PasswordToken(""))
  val reporter = AccumuloReporter.forRegistry(registry)
      .mock(true)
      .writeToTable("metrics")
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build("AccumuloReporterTest", "", "root", "")

  def scan(prefix: String): List[Entry[Key, Value]] = {
    val scanner = connector.createScanner("metrics", new Authorizations)
    scanner.setRange(org.apache.accumulo.core.data.Range.prefix(prefix))
    try {
      scanner.toList
    } finally {
      scanner.close()
    }
  }

  def entry(entries: List[Entry[Key, Value]], cf: Text): Option[Double] =
    entries.find(_.getKey.getColumnFamily == cf).map(_.getValue.toString.toDouble)

  "AccumuloReporter" should {
    "report gauges" >> {
      val name = "mygauge"

      registry.register(name, new Gauge[String] {
        override def getValue: String = "value1"
      })

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = scan(name)

      entries must haveLength(1)
      entries.head.getKey.getRow.toString must startWith(name)
      entries.head.getKey.getColumnFamily mustEqual Keys.value
      entries.head.getValue.toString mustEqual "value1"
    }

    "report counters" >> {
      val name = "mycounter"

      val metric = registry.counter(name)
      metric.inc(10)

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = scan(name)

      entries must haveLength(1)
      entries.head.getKey.getColumnFamily mustEqual Keys.count
      entries.head.getValue.toString mustEqual "10"
    }

    "report histograms" >> {
      val name = "myhistogram"

      val metric = registry.histogram(name)
      (0 until 10).foreach(metric.update)

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = scan(name)

      entries must haveLength(11)
      forall(entries)(_.getKey.getRow.toString mustEqual name)

      entry(entries, Keys.count) must beSome(10.0)
      entry(entries, Keys.max) must beSome(9.0)
      entry(entries, Keys.mean) must beSome(4.5)
      entry(entries, Keys.min) must beSome(0.0)
      entry(entries, Keys.stddev) must beSome(2.872281)
      entry(entries, Keys.p50) must beSome(5.0)
      entry(entries, Keys.p75) must beSome(7.0)
      entry(entries, Keys.p95) must beSome(9.0)
      entry(entries, Keys.p98) must beSome(9.0)
      entry(entries, Keys.p99) must beSome(9.0)
      entry(entries, Keys.p999) must beSome(9.0)
    }

    "report meters" >> {
      val name = "mymeter"

      var tick: Long = 0
      val clock = new Clock { override def getTick: Long = tick * 1000 } // tick is in nanos - we use millis

      val metric = registry.register(name, new Meter(clock))
      (0 until 10).foreach { i => tick += i; metric.mark() }

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = scan(name)

      entries must haveLength(6)
      forall(entries)(_.getKey.getRow.toString mustEqual name)

      entry(entries, Keys.count) must beSome(10.0)
      entry(entries, Keys.mean_rate) must beSome(222222.222222)
      entry(entries, Keys.m1_rate) must beSome(0.0)
      entry(entries, Keys.m5_rate) must beSome(0.0)
      entry(entries, Keys.m15_rate) must beSome(0.0)
      entries.find(_.getKey.getColumnFamily == Keys.rate_unit).map(_.getValue.toString) must beSome("events/second")
    }

    "report timers" >> {
      val name = "mytimer"

      var tick: Long = 0
      val clock = new Clock { override def getTick: Long = tick * 1000 } // tick is in nanos - we use millis

      val metric = registry.register(name, new Timer(new ExponentiallyDecayingReservoir(), clock))
      (0 until 10).foreach { i =>
        tick += i
        val c = metric.time()
        tick += i
        c.stop()
      }

      reporter.report()
      reporter.flush()
      registry.remove(name)

      val entries = scan(name)

      entries must haveLength(17)
      forall(entries)(_.getKey.getRow.toString mustEqual name)

      entry(entries, Keys.count) must beSome(10.0)
      entry(entries, Keys.max) must beSome(0.009)
      entry(entries, Keys.mean) must beSome(0.0045)
      entry(entries, Keys.min) must beSome(0.0)
      entry(entries, Keys.stddev) must beSome(0.002872)
      entry(entries, Keys.p50) must beSome(0.005)
      entry(entries, Keys.p75) must beSome(0.007)
      entry(entries, Keys.p95) must beSome(0.009)
      entry(entries, Keys.p98) must beSome(0.009)
      entry(entries, Keys.p99) must beSome(0.009)
      entry(entries, Keys.p999) must beSome(0.009)
      entry(entries, Keys.mean_rate) must beSome(111111.111111)
      entry(entries, Keys.m1_rate) must beSome(0.0)
      entry(entries, Keys.m5_rate) must beSome(0.0)
      entry(entries, Keys.m15_rate) must beSome(0.0)
      entries.find(_.getKey.getColumnFamily == Keys.rate_unit).map(_.getValue.toString) must beSome("calls/second")
      entries.find(_.getKey.getColumnFamily == Keys.duration_unit).map(_.getValue.toString) must beSome("milliseconds")
    }
  }

  step {
    reporter.stop()
  }
}
