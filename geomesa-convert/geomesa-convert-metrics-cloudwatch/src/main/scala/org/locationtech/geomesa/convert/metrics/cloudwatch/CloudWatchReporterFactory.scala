/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.metrics.cloudwatch

import java.util.concurrent.TimeUnit
import org.locationtech.geomesa.convert2.metrics.ReporterFactory
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.Config
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter

class CloudWatchReporterFactory extends ReporterFactory {

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("cloudwatch")) { None } else {
      val namespace = if(!conf.hasPath("namespace")) { "geomesa" } else { conf.getString("namespace") }
      val reporter =
        CloudWatchReporter.forRegistry(registry, AmazonCloudWatchAsyncClientBuilder.defaultClient, namespace)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
      if(conf.hasPath("raw-counts") && conf.getBoolean("raw-counts")) { reporter.withReportRawCountValue }
      if(conf.hasPath("zero-values") && conf.getBoolean("zero-values")) { reporter.withZeroValuesSubmission }
      Some(reporter.build)
    }
  }
}
