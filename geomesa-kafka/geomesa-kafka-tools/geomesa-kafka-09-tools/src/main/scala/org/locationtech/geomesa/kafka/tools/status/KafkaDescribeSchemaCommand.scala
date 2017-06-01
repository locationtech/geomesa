/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.tools.{ConsumerKDSConnectionParams, KafkaDataStoreCommand}
import org.locationtech.geomesa.kafka09.KafkaUtils09
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class KafkaDescribeSchemaCommand extends KafkaDataStoreCommand {

  override val name = "get-schema"
  override val params = new KafkaDescribeSchemaParams

  def execute(): Unit = withDataStore { (ds) =>
    Command.user.info(s"Describing attributes of feature type '${params.featureName}' at zkPath '${params.zkPath}'...")
    try {
      val sft = ds.getSchema(params.featureName)

      val sb = new StringBuilder()
      sft.getAttributeDescriptors.foreach { attr =>
        sb.clear()
        val name = attr.getLocalName

        // TypeName
        sb.append(name)
        sb.append(": ")
        sb.append(attr.getType.getBinding.getSimpleName)

        if (sft.getGeometryDescriptor == attr) sb.append(" (Default geometry)")
        if (attr.getDefaultValue != null)      sb.append("- Default Value: ", attr.getDefaultValue)

        Command.output.info(sb.toString())
      }

      val userData = sft.getUserData
      if (!userData.isEmpty) {
        Command.user.info("\nUser data:")
        userData.foreach { case (key, value) => Command.user.info(s"  $key: $value") }
      }

      Command.user.info("\nFetching Kafka topic metadata...")

      val zkUtils = KafkaUtils09.createZkUtils(params.zookeepers, Int.MaxValue, Int.MaxValue)
      try {
        val topicName = zkUtils.zkClient.readData[String](ds.getTopicPath(params.featureName))
        val topicMetadata = zkUtils.fetchTopicMetadataFromZk(topicName)
        Command.user.info(s"Topic: ${topicMetadata.topicName} Number of partitions: ${topicMetadata.numberOfPartitions}")
      } finally {
        zkUtils.close()
      }
    } catch {
      case npe: NullPointerException =>
        Command.user.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        Command.user.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        Command.user.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    }
  }
}

@Parameters(commandDescription = "Describe the attributes of a given feature in GeoMesa")
class KafkaDescribeSchemaParams extends ConsumerKDSConnectionParams with RequiredTypeNameParam
