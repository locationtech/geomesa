/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.metrics.cloudwatch

import java.util.concurrent.TimeUnit

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter
import org.locationtech.geomesa.convert2.metrics.ReporterFactory
import pureconfig.ConfigReader

class CloudWatchReporterFactory extends ReporterFactory {

  import CloudWatchReporterFactory.{CloudWatchConfig, CloudWatchDefaults, Type}

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase(Type)) { None } else {
      val cloudwatch = pureconfig.loadConfigOrThrow[CloudWatchConfig](conf.withFallback(CloudWatchDefaults))
      val client = AmazonCloudWatchAsyncClientBuilder.defaultClient
      val reporter =
        CloudWatchReporter.forRegistry(registry, client, cloudwatch.namespace)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)

      if (cloudwatch.rawCounts) {
        reporter.withReportRawCountValue()
      }
      if (cloudwatch.zeroValues) {
        reporter.withZeroValuesSubmission()
      }

      Some(reporter.build)
    }
  }
}

object CloudWatchReporterFactory {

  import pureconfig.generic.semiauto._

  val Type = "cloudwatch"

  val CloudWatchDefaults: Config =
    ConfigFactory.empty
        .withValue("namespace", ConfigValueFactory.fromAnyRef("geomesa"))
        .withValue("raw-counts", ConfigValueFactory.fromAnyRef(false))
        .withValue("zero-values", ConfigValueFactory.fromAnyRef(false))

  implicit val CloudWatchConfigReader: ConfigReader[CloudWatchConfig] = deriveReader[CloudWatchConfig]

  case class CloudWatchConfig(namespace: String, rawCounts: Boolean, zeroValues: Boolean)
}
