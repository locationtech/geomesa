/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.status.KafkaDescribeSchemaCommand.DescribeParameters
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, StatusDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand
import org.opengis.feature.simple.SimpleFeatureType
import org.slf4j.Logger

class KafkaDescribeSchemaCommand extends DescribeSchemaCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new DescribeParameters

    // TODO metadata?

//    Command.user.info("\nFetching Kafka topic metadata...")
//
//    val zkUtils = KafkaUtils10.createZkUtils(params.zookeepers, Int.MaxValue, Int.MaxValue)
//    try {
//      val topicName = zkUtils.zkClient.readData[String](ds.getTopicPath(params.featureName))
//      val topicMetadata = zkUtils.fetchTopicMetadataFromZk(topicName)
//      Command.user.info(s"Topic: ${topicMetadata.topicName} Number of partitions: ${topicMetadata.numberOfPartitions}")
//    } finally {
//      zkUtils.close()
//    }
}

object KafkaDescribeSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
  class DescribeParameters extends StatusDataStoreParams with RequiredTypeNameParam
}
