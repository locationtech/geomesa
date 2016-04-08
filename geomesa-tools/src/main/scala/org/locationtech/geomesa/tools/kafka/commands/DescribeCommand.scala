/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.locationtech.geomesa.kafka.{KafkaDataStore, KafkaDataStoreSchemaManager}
import org.locationtech.geomesa.tools.common.commands.FeatureTypeNameParam
import org.locationtech.geomesa.tools.kafka.commands.DescribeCommand._

import scala.collection.JavaConversions._

class DescribeCommand(parent: JCommander) extends CommandWithKDS(parent) with LazyLogging {
  override val command = "describe"
  override val params = new DescribeParameters

  def execute() = {
    logger.info(s"Describing attributes of feature type '${params.featureName}' at zkPath '$zkPath'...")
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

        if (sft.getGeometryDescriptor == attr) sb.append(" (ST-Geo-index)")
        if (attr.getDefaultValue != null)      sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }

      val zkClient = new ZkClient(params.zookeepers, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
      val topicName = zkClient.readData[String](ds.asInstanceOf[KafkaDataStore].getTopicPath(params.featureName))
      val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient)

      println("\nFetching Kafka topic metadata...")
      println(s"Number of partitions: ${topicMetadata.partitionsMetadata.size}")
      println(topicMetadata)
    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
    }
  }
}

object DescribeCommand {
  @Parameters(commandDescription = "Describe the attributes of a given feature in GeoMesa")
  class DescribeParameters extends ConsumerKDSConnectionParams
    with FeatureTypeNameParam {}
}

