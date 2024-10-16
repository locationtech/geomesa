/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package cloudwatch
import com.typesafe.config.Config
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry
import io.micrometer.core.instrument.{Clock, MeterRegistry}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

object CloudwatchFactory extends RegistryFactory {

  override def apply(conf: Config): MeterRegistry = {
    implicit val reader: ConfigReader[CloudwatchConfig] = deriveReader[CloudwatchConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[CloudwatchConfig]
    new CloudWatchMeterRegistry(k => config.properties.getOrElse(k, null), Clock.SYSTEM, CloudWatchAsyncClient.create())
  }

  private case class CloudwatchConfig(
      namespace: String = "geomesa",
      properties: Map[String, String] = Map.empty
    )
}
