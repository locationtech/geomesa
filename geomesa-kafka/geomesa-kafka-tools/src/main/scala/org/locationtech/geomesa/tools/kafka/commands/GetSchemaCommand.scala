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
import org.locationtech.geomesa.kafka08.KafkaDataStore
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.tools.kafka.ConsumerKDSConnectionParams
import org.locationtech.geomesa.tools.kafka.commands.GetSchemaCommand._

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class GetSchemaCommand(parent: JCommander) extends CommandWithKDS(parent) with LazyLogging {
  override val command = "get-schema"
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

        if (sft.getGeometryDescriptor == attr) sb.append(" (Default geometry)")
        if (attr.getDefaultValue != null)      sb.append("- Default Value: ", attr.getDefaultValue)

        println(sb.toString())
      }

      val userData = sft.getUserData
      if (!userData.isEmpty) {
        println("\nUser data:")
        userData.foreach { case (key, value) => println(s"  $key: $value") }
      }

      val topicName = zkClient.readData[String](ds.asInstanceOf[KafkaDataStore].getTopicPath(params.featureName))
      val topicMetadata = zkUtils.fetchTopicMetadataFromZk(topicName)

      println("\nFetching Kafka topic metadata...")
      println(s"Topic: ${topicMetadata.topicName} Number of partitions: ${topicMetadata.numberOfPartitions}")
    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        logger.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    }
  }
}

object GetSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given feature in GeoMesa")
  class DescribeParameters extends ConsumerKDSConnectionParams
    with FeatureTypeNameParam {}
}

