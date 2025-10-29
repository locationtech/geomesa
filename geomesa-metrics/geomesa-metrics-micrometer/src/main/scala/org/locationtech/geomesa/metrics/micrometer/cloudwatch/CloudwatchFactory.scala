/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package cloudwatch

import com.typesafe.config.Config
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry
import io.micrometer.core.instrument.{Clock, MeterRegistry}
import org.locationtech.geomesa.metrics.micrometer.RegistryFactory.BaseRegistryFactory
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

object CloudwatchFactory extends BaseRegistryFactory(RegistryFactory.Cloudwatch) {

  override protected def createRegistry(conf: Config): MeterRegistry = {
    implicit val reader: ConfigReader[CloudwatchConfig] = deriveReader[CloudwatchConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[CloudwatchConfig]
    val props = Map("namespace" -> config.namespace) ++ config.properties
    new CloudWatchMeterRegistry(k => props.getOrElse(k, null), Clock.SYSTEM, CloudWatchAsyncClient.create())
  }

  private case class CloudwatchConfig(
      namespace: String = "geomesa",
      properties: Map[String, String] = Map.empty
    )
}
