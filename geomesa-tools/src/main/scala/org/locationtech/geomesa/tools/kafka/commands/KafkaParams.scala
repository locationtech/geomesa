/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.Parameter

class KafkaParams {
  @Parameter(names = Array("-b", "--brokers"), description = "Brokers (host[:port], comma separated)", required = true)
  var brokers: String = null

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null

  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where SFT info is saved)")
  var zkPath: String = null

  val isProducer: Boolean = false
}

class FeatureParams extends KafkaParams {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

class OptionalFeatureParams extends KafkaParams {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate")
  var featureName: String = null
}

class RequiredCqlFilterParameters extends FeatureParams {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

class OptionalCqlFilterParameters extends FeatureParams {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

class CreateFeatureParams extends FeatureParams {
  @Parameter(names = Array("-s", "--spec"), description =
    "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either", required = true)
  var spec: String = null

  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null

  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  var replication: String = null

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  var partitions: String = null

  override val isProducer: Boolean = true
}
