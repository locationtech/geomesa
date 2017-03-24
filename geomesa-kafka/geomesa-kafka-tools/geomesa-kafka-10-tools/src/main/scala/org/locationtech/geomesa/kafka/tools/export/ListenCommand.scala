/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.export

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.beust.jcommander.{Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import kafka.common.MessageFormatter
import kafka.tools.ConsoleConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.kafka.tools.ConsumerKDSConnectionParams
import org.locationtech.geomesa.kafka.{Clear, CreateOrUpdate, Delete}
import org.locationtech.geomesa.kafka10.KafkaDataStoreLogViewer._
import org.locationtech.geomesa.kafka10.{KafkaDataStore, KafkaDataStoreLogViewer, KafkaGeoMessageDecoder}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

class ListenCommand extends Command with LazyLogging {

  import KafkaGeoMessageFormatter._

  override val name = "listen"
  override val params = new ListenParameters()

  override def execute(): Unit = {
    Command.user.info(s"Listening to ${params.featureName}...")

    val featureConfig = new KafkaDataStore(params.zookeepers, params.zkPath, 1, 1, null).getFeatureConfig(params.featureName)
    val formatter = classOf[ListenMessageFormatter].getName
    val sftSpec = encodeSFT(featureConfig.sft)

    Command.user.info(s"SimpleFeature type for type ${params.featureName}:")
    Command.user.info(s"${DataUtilities.encodeType(featureConfig.sft)}")

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


@Parameters(commandDescription = "Listen to a GeoMesa Kafka topic")
class ListenParameters extends ConsumerKDSConnectionParams with RequiredTypeNameParam {
  @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
  var fromBeginning: Boolean = false
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


  override def writeTo(record: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    val msg = decoder.decode(record.key, record.value)

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
