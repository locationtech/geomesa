/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreParams

/**
  * Shared lambda-specific command line parameters
  */

trait LambdaDataStoreParams extends AccumuloDataStoreParams with LambdaParams

trait LambdaParams {
  @Parameter(names = Array("-b", "--brokers"), description = "Kafka brokers")
  var brokers: String = null

  @Parameter(names = Array("--kafka-zookeepers"), description = "Kafka zookeepers (will use Accumulo zookeepers if not specified)")
  var kafkaZookeepers: String = null

  @Parameter(names = Array("--partitions"), description = "Number of partitions to use for Kafka topics")
  var partitions: Integer = null
}
