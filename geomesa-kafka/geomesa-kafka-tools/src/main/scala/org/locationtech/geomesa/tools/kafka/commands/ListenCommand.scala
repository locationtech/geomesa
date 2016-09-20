/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import kafka.tools.ConsoleConsumer
import kafka.tools.MessageFormatter
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.kafka08.KafkaDataStoreLogViewer._
import org.locationtech.geomesa.kafka08._
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.tools.kafka.ConsumerKDSConnectionParams
import org.locationtech.geomesa.tools.kafka.commands.ListenCommand.ListenParameters

class ListenCommand(parent: JCommander) extends CommandWithKDS(parent) with LazyLogging {

  import KafkaGeoMessageFormatter._

  override val command = "listen"
  override val params = new ListenParameters()

  override def execute(): Unit = {
    println(s"Listening to ${params.featureName}...")

    val featureConfig = new KafkaDataStore(params.zookeepers, zkPath, 1, 1, null).getFeatureConfig(params.featureName)
    val formatter = classOf[ListenMessageFormatter].getName
    val sftSpec = encodeSFT(featureConfig.sft)

    println(s"SimpleFeature type for type ${params.featureName}:")
    println(s"${DataUtilities.encodeType(featureConfig.sft)}")

    var ccArgs = Seq("--topic", featureConfig.topic,
      "--zookeeper", params.zookeepers,
      "--formatter", formatter,
      "--property", s"$sftNameKey=$params.featureName",
      "--property", s"$sftSpecKey=$sftSpec")

    if (params.fromBeginning) {
      ccArgs :+= "--from-beginning"
    }

    logger.debug(s"Running 'kafka-console-consumer ${ccArgs.mkString(" ")}'")
    ConsoleConsumer.main(ccArgs.toArray)
  }
}

object ListenCommand {
  @Parameters(commandDescription = "Listen to a GeoMesa Kafka topic")
  class ListenParameters extends ConsumerKDSConnectionParams
    with FeatureTypeNameParam {

    @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
    var fromBeginning: Boolean = false
  }
}

/**
  * A [[MessageFormatter]] used to format messages for the geomesa-kafka listen command
  */
class ListenMessageFormatter extends MessageFormatter {

  import KafkaGeoMessageFormatter._

  private var decoder: KafkaGeoMessageDecoder = null

  override def init(props: Properties): Unit = {
    if(!props.containsKey(sftNameKey)) {
      throw new IllegalArgumentException(s"Property '$sftNameKey' is required.")
    }

    if(!props.containsKey(sftSpecKey)) {
      throw new IllegalArgumentException(s"Property '$sftSpecKey' is required.")
    }

    val name = props.getProperty(sftNameKey)
    val spec = props.getProperty(sftSpecKey)

    val sft = KafkaDataStoreLogViewer.decodeSFT(name, spec)
    decoder = new KafkaGeoMessageDecoder(sft)
  }

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream): Unit = {
    val msg = decoder.decode(key, value)

    val msgToWrite = msg match {
      case createOrUpdate: CreateOrUpdate =>
        s"CreateOrUpdate at ${createOrUpdate.timestamp}: ${DataUtilities.encodeFeature(createOrUpdate.feature)}"
      case delete: Delete =>
        s"Delete at ${delete.timestamp}: ${delete.id}"
      case clear: Clear =>
        s"Clear at ${clear.timestamp}"
    }

    output.write(msgToWrite.getBytes(StandardCharsets.UTF_8))
    output.write(lineSeparator)
  }

  override def close(): Unit = {
    decoder = null
  }

}

object KafkaGeoMessageFormatter {
  val sftNameKey = "sft.name"
  val sftSpecKey = "sft.spec"

  val lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)
}