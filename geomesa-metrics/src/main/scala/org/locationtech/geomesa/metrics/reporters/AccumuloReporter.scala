/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.reporters


import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.google.common.base.Charsets
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Reporter for dropwizard metrics that will write to accumulo
 */
@deprecated("Will be removed without replacement")
object AccumuloReporter {

  object Keys {
    val count =  new Text("count")
    val value =  new Text("value")

    val max =    new Text("max")
    val mean =   new Text("mean")
    val min =    new Text("min")
    val stddev = new Text("stddev")

    val p50 =  new Text("p50")
    val p75 =  new Text("p75")
    val p95 =  new Text("p95")
    val p98 =  new Text("p98")
    val p99 =  new Text("p99")
    val p999 = new Text("p999")

    val mean_rate =     new Text("mean_rate")
    val m1_rate =       new Text("m1_rate")
    val m5_rate =       new Text("m5_rate")
    val m15_rate =      new Text("m15_rate")
    val rate_unit =     new Text("rate_unit")
    val duration_unit = new Text("duration_unit")

    val empty = new Text(Array.empty[Byte])
  }

  def forRegistry(registry: MetricRegistry): Builder = new Builder(registry)

  class Builder private[metrics] (registry: MetricRegistry) {

    private var locale: Locale = Locale.getDefault
    private var rateUnit: TimeUnit = TimeUnit.SECONDS
    private var durationUnit: TimeUnit = TimeUnit.MILLISECONDS
    private var clock: Clock = Clock.defaultClock
    private var filter: MetricFilter = MetricFilter.ALL
    private var table: String = "metrics"
    private var visibilities: String = _
    private var mock: Boolean = false

    def formatFor(locale: Locale): Builder = { this.locale = locale; this }

    def convertRatesTo(rateUnit: TimeUnit): Builder = { this.rateUnit = rateUnit; this }

    def convertDurationsTo(durationUnit: TimeUnit): Builder = { this.durationUnit = durationUnit; this }

    def withClock(clock: Clock): Builder = { this.clock = clock; this }

    def filter(filter: MetricFilter): Builder = { this.filter = filter; this }

    def writeToTable(table: String): Builder = { this.table = table; this }

    def withVisibilities(visibilities: String): Builder = { this.visibilities = visibilities; this }

    def mock(mock: Boolean): Builder = { this.mock = mock; this }

    def build(instanceId: String, zookeepers: String, user: String, password: String): AccumuloReporter = {
      val instance = if (mock) new MockInstance(instanceId) else new ZooKeeperInstance(instanceId, zookeepers)
      val connector = instance.getConnector(user, new PasswordToken(password))
      new AccumuloReporter(registry, connector, table, Option(visibilities),
        clock, locale, filter, rateUnit, durationUnit)
    }
  }

  private def mutationFor(name: String, timestamp: Option[String] = None) =
    new Mutation(timestamp.map(t => s"$name-$t").getOrElse(name))
}

/**
 * Reporter for dropwizard metrics that will write to accumulo
 */
class AccumuloReporter private (registry: MetricRegistry,
                                connector: Connector,
                                table: String,
                                visibilities: Option[String],
                                clock: Clock,
                                locale: Locale,
                                filter: MetricFilter,
                                rateUnit: TimeUnit,
                                durationUnit: TimeUnit)
    extends ScheduledReporter(registry, "AccumuloReporter", filter, rateUnit, durationUnit) {

  import AccumuloReporter.{Keys, mutationFor}

  if (!connector.tableOperations().exists(table)) {
    try { connector.tableOperations().create(table, true, TimeType.LOGICAL) } catch {
      case e: TableExistsException => // no-op
    }
  }
  private val writer = connector.createBatchWriter(table, new BatchWriterConfig)
  private val timeEncoder = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.US).withZone(ZoneOffset.UTC)
  private val visibility = visibilities.map(new ColumnVisibility(_))

  override def stop(): Unit = {
    writer.close()
    super.stop()
  }

  override def report(gauges: java.util.SortedMap[String, Gauge[_]],
                      counters: java.util.SortedMap[String, Counter],
                      histograms: java.util.SortedMap[String, Histogram],
                      meters: java.util.SortedMap[String, Meter],
                      timers: java.util.SortedMap[String, Timer]): Unit = {
    lazy val timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(clock.getTime), ZoneOffset.UTC).format(timeEncoder)
    addMutations(gauges.toSeq.map { case (name, metric) => gaugeToMutation(name, timestamp, metric) })
    addMutations(counters.toSeq.map { case (name, metric) => counterToMutation(name, timestamp, metric) })
    addMutations(histograms.toSeq.map { case (name, metric) => histogramToMutation(name, metric) })
    addMutations(meters.toSeq.map { case (name, metric) => meterToMutation(name, metric) })
    addMutations(timers.toSeq.map { case (name, metric) => timerToMutation(name, metric) })
  }

  def flush(): Unit = writer.flush()

  private def addMutations(mutations: Seq[Mutation]): Unit =
    if (mutations.nonEmpty) { writer.addMutations(mutations) }

  // gauges are stored with the timestamp in the row key, so history is kept
  private def gaugeToMutation(name: String, timestamp: String, gauge: Gauge[_]): Mutation = {
    val m = mutationFor(name, Some(timestamp))
    put(m, Keys.value, "%s", gauge.getValue)
    m
  }

  // counters are stored with the timestamp in the row key, so history is kept
  private def counterToMutation(name: String, timestamp: String, counter: Counter): Mutation = {
    val m = mutationFor(name, Some(timestamp))
    put(m, Keys.count, "%d", counter.getCount)
    m
  }

  // histograms will overwrite existing data
  private def histogramToMutation(name: String, histogram: Histogram): Mutation = {
    val m = mutationFor(name)
    val snapshot = histogram.getSnapshot

    put(m, Keys.count,  "%d", histogram.getCount)
    put(m, Keys.max,    "%d", snapshot.getMax)
    put(m, Keys.mean,   "%f", snapshot.getMean)
    put(m, Keys.min,    "%d", snapshot.getMin)
    put(m, Keys.stddev, "%f", snapshot.getStdDev)
    put(m, Keys.p50,    "%f", snapshot.getMedian)
    put(m, Keys.p75,    "%f", snapshot.get75thPercentile)
    put(m, Keys.p95,    "%f", snapshot.get95thPercentile)
    put(m, Keys.p98,    "%f", snapshot.get98thPercentile)
    put(m, Keys.p99,    "%f", snapshot.get99thPercentile)
    put(m, Keys.p999,   "%f", snapshot.get999thPercentile)

    m
  }

  // meters will overwrite existing data
  private def meterToMutation(name: String, meter: Meter): Mutation = {
    val m = mutationFor(name)

    put(m, Keys.count,     "%d", meter.getCount)
    put(m, Keys.mean_rate, "%f", convertRate(meter.getMeanRate))
    put(m, Keys.m1_rate,   "%f", convertRate(meter.getOneMinuteRate))
    put(m, Keys.m5_rate,   "%f", convertRate(meter.getFiveMinuteRate))
    put(m, Keys.m15_rate,  "%f", convertRate(meter.getFifteenMinuteRate))
    put(m, Keys.rate_unit, "events/%s", getRateUnit)

    m
  }

  // timers will overwrite existing data
  private def timerToMutation(name: String, timer: Timer): Mutation = {
    val m = mutationFor(name)
    val snapshot = timer.getSnapshot

    put(m, Keys.count,         "%d", timer.getCount)
    put(m, Keys.max,           "%f", convertDuration(snapshot.getMax))
    put(m, Keys.mean,          "%f", convertDuration(snapshot.getMean))
    put(m, Keys.min,           "%f", convertDuration(snapshot.getMin))
    put(m, Keys.stddev,        "%f", convertDuration(snapshot.getStdDev))
    put(m, Keys.p50,           "%f", convertDuration(snapshot.getMedian))
    put(m, Keys.p75,           "%f", convertDuration(snapshot.get75thPercentile))
    put(m, Keys.p95,           "%f", convertDuration(snapshot.get95thPercentile))
    put(m, Keys.p98,           "%f", convertDuration(snapshot.get98thPercentile))
    put(m, Keys.p99,           "%f", convertDuration(snapshot.get99thPercentile))
    put(m, Keys.p999,          "%f", convertDuration(snapshot.get999thPercentile))
    put(m, Keys.mean_rate,     "%f", convertRate(timer.getMeanRate))
    put(m, Keys.m1_rate,       "%f", convertRate(timer.getOneMinuteRate))
    put(m, Keys.m5_rate,       "%f", convertRate(timer.getFiveMinuteRate))
    put(m, Keys.m15_rate,      "%f", convertRate(timer.getFifteenMinuteRate))
    put(m, Keys.duration_unit, "%s", getDurationUnit)
    put(m, Keys.rate_unit,     "calls/%s", getRateUnit)

    m
  }

  private def put(m: Mutation, cf: Text, format: String, value: Any): Unit = {
    val v = new Value(format.formatLocal(locale, value).getBytes(Charsets.UTF_8))
    visibility match {
      case None      => m.put(cf, Keys.empty, v)
      case Some(vis) => m.put(cf, Keys.empty, vis, v)
    }
  }
}
