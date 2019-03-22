/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.reporters

import java.io._
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.language.implicitConversions

/**
 * Reporter for dropwizard metrics that will write to delimited files. In contrast to the
 * dropwizard CSVReporter, allows for either CSV or TSV files and for aggregating metrics by type.
 */
@deprecated("Will be removed without replacement")
object DelimitedFileReporter {

  val gaugeCols = Array(("value", "%s"))

  val counterCols = Array(("count", "%d"))

  val histogramCols = Array(
    ("count", "%d"),
    ("min", "%d"),
    ("max", "%d"),
    ("mean", "%2.2f"),
    ("stdDev", "%2.2f"),
    ("median", "%2.2f"),
    ("75thPercentile", "%2.2f"),
    ("95thPercentile", "%2.2f"),
    ("98thPercentile", "%2.2f"),
    ("99thPercentile", "%2.2f"),
    ("999thPercentile", "%2.2f")
  )

  val meterCols = Array(
    ("count", "%d"),
    ("meanRate", "%2.2f"),
    ("oneMinuteRate", "%2.2f"),
    ("fiveMinuteRate", "%2.2f"),
    ("fifteenMinuteRate", "%2.2f"),
    ("rateUnit", "events/%s")
  )

  val timerCols = Array(
    ("count", "%d"),
    ("min", "%2.2f"),
    ("max", "%2.2f"),
    ("mean", "%2.2f"),
    ("stdDev", "%2.2f"),
    ("median", "%2.2f"),
    ("75thPercentile", "%2.2f"),
    ("95thPercentile", "%2.2f"),
    ("98thPercentile", "%2.2f"),
    ("99thPercentile", "%2.2f"),
    ("999thPercentile", "%2.2f"),
    ("meanRate", "%2.2f"),
    ("oneMinuteRate", "%2.2f"),
    ("fiveMinuteRate", "%2.2f"),
    ("fifteenMinuteRate", "%2.2f"),
    ("rateUnit", "calls/%s"),
    ("durationUnit", "%s")
  )

  def forRegistry(registry: MetricRegistry): Builder = new Builder(registry)

  class Builder private[metrics] (registry: MetricRegistry) {

    private var locale: Locale = Locale.getDefault
    private var rateUnit: TimeUnit = TimeUnit.SECONDS
    private var durationUnit: TimeUnit = TimeUnit.MILLISECONDS
    private var clock: Clock = Clock.defaultClock
    private var filter: MetricFilter = MetricFilter.ALL
    private var aggregate: Boolean = true
    private var format: CSVFormat = CSVFormat.TDF

    def formatFor(locale: Locale): Builder = { this.locale = locale; this }

    def convertRatesTo(rateUnit: TimeUnit): Builder = { this.rateUnit = rateUnit; this }

    def convertDurationsTo(durationUnit: TimeUnit): Builder = { this.durationUnit = durationUnit; this }

    def withClock(clock: Clock): Builder = { this.clock = clock; this }

    def filter(filter: MetricFilter): Builder = { this.filter = filter; this }

    def aggregate(aggregate: Boolean): Builder = { this.aggregate = aggregate; this }

    def withCommas(): Builder = { this.format = CSVFormat.DEFAULT; this }

    def withTabs(): Builder = { this.format = CSVFormat.TDF; this }

    def build(path: String): DelimitedFileReporter = {
      new DelimitedFileReporter(registry, path, aggregate, format, clock, locale, filter, rateUnit, durationUnit)
    }
  }
}

/**
 * Reporter for dropwizard metrics that will write to accumulo
 */
class DelimitedFileReporter private (registry: MetricRegistry,
                                     path: String,
                                     aggregate: Boolean,
                                     format: CSVFormat,
                                     clock: Clock,
                                     locale: Locale,
                                     filter: MetricFilter,
                                     rateUnit: TimeUnit,
                                     durationUnit: TimeUnit)
    extends ScheduledReporter(registry, "DelimitedFileReporter", filter, rateUnit, durationUnit) {

  import DelimitedFileReporter._

  private val writers = scala.collection.mutable.Map.empty[String, CSVPrinter]
  private val folder = new File(path)
  if (folder.exists()) {
    require(folder.isDirectory, s"Path refers to a file - must be a folder: $path")
  } else {
    require(folder.mkdirs(), s"Can't create folder at $path")
  }

  private val timeEncoder =
    new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral('T')
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
        .appendOffsetId()
        .toFormatter(Locale.US)
        .withZone(ZoneOffset.UTC)

  def flush(): Unit = writers.synchronized(writers.values.foreach(_.flush()))

  override def stop(): Unit = {
    writers.synchronized(writers.values.foreach { w => w.flush(); w.close() })
    super.stop()
  }

  override def report(gauges: java.util.SortedMap[String, Gauge[_]],
                      counters: java.util.SortedMap[String, Counter],
                      histograms: java.util.SortedMap[String, Histogram],
                      meters: java.util.SortedMap[String, Meter],
                      timers: java.util.SortedMap[String, Timer]): Unit = {
    import scala.collection.JavaConversions._
    lazy val timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(clock.getTime), ZoneOffset.UTC).format(timeEncoder)
    gauges.foreach { case (name, metric) => writeGauge(name, timestamp, metric) }
    counters.foreach { case (name, metric) => writeCounter(name, timestamp, metric) }
    histograms.foreach { case (name, metric) => writeHistogram(name, timestamp, metric) }
    meters.foreach { case (name, metric) => writeMeter(name, timestamp, metric) }
    timers.foreach { case (name, metric) => writeTimer(name, timestamp, metric) }
  }

  private def writeGauge(name: String, timestamp: String, gauge: Gauge[_]): Unit =
    write("gauges", gaugeCols, name, timestamp, Array[Any](gauge.getValue))

  private def writeCounter(name: String, timestamp: String, counter: Counter): Unit =
    write("counters", counterCols, name, timestamp, Array[Any](counter.getCount))

  private def writeHistogram(name: String, timestamp: String, histogram: Histogram): Unit = {
    val snapshot = histogram.getSnapshot
    val values = Array[Any](
      histogram.getCount,
      snapshot.getMin,
      snapshot.getMax,
      snapshot.getMean,
      snapshot.getStdDev,
      snapshot.getMedian,
      snapshot.get75thPercentile,
      snapshot.get95thPercentile,
      snapshot.get98thPercentile,
      snapshot.get99thPercentile,
      snapshot.get999thPercentile
    )
    write("histograms", histogramCols, name, timestamp, values)
  }

  private def writeMeter(name: String, timestamp: String, meter: Meter): Unit = {
    val values = Array[Any](
      meter.getCount,
      convertRate(meter.getMeanRate),
      convertRate(meter.getOneMinuteRate),
      convertRate(meter.getFiveMinuteRate),
      convertRate(meter.getFifteenMinuteRate),
      getRateUnit)
    write("meters", meterCols, name, timestamp, values)
  }

  private def writeTimer(name: String, timestamp: String, timer: Timer): Unit = {
    val snapshot = timer.getSnapshot
    val values = Array[Any](
      timer.getCount,
      convertDuration(snapshot.getMin),
      convertDuration(snapshot.getMax),
      convertDuration(snapshot.getMean),
      convertDuration(snapshot.getStdDev),
      convertDuration(snapshot.getMedian),
      convertDuration(snapshot.get75thPercentile),
      convertDuration(snapshot.get95thPercentile),
      convertDuration(snapshot.get98thPercentile),
      convertDuration(snapshot.get99thPercentile),
      convertDuration(snapshot.get999thPercentile),
      convertRate(timer.getMeanRate),
      convertRate(timer.getOneMinuteRate),
      convertRate(timer.getFiveMinuteRate),
      convertRate(timer.getFifteenMinuteRate),
      getRateUnit,
      getDurationUnit
    )
    write("timers", timerCols, name, timestamp, values)
  }

  private def write(metric: String,
                    cols: Array[(String, String)],
                    name: String,
                    timestamp: String,
                    values: Array[Any]): Unit = writers.synchronized {
    val key = if (aggregate) metric else name
    val writer = writers.getOrElseUpdate(key, {
      val fileName = s"$key.${if (format == CSVFormat.TDF) "t" else "c"}sv"
      val file = new File(folder, fileName)
      val writer = format.print(new FileWriter(file, true))
      // write the header row if the file is empty
      if (!file.exists() || file.length() == 0) {
        writer.print("time")
        if (aggregate) {
          writer.print("name")
        }
        cols.foreach { case (header, _) => writer.print(header) }
        writer.println()
      }
      writer
    })
    writer.print(timestamp)
    if (aggregate) {
      writer.print(name)
    }
    var i = 0
    while (i < cols.length) {
      writer.print(cols(i)._2.formatLocal(locale, values(i)))
      i += 1
    }
    writer.println()
  }
}
