/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.beust.jcommander.{IParameterValidator, JCommander, Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import kafka.common.MessageFormatter
import kafka.tools.ConsoleConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/** An alternative to the kafka-console-consumer providing options specific to viewing the log messages
  * internal to the [[KafkaDataStore]].
  *
  * To run, first copy the geomesa-kafka-gs-plugin.jar to $KAFKA_HOME/libs.  Then create a copy of
  * $KAFKA_HOME/bin/kafka-console-consumer.sh called "kafka-ds-log-viewer" and in the copy replace the
  * classname in the exec command at the end of the script with
  * org.locationtech.geomesa.kafka.KafkaDataStoreLogViewer.
  */
object KafkaDataStoreLogViewer extends LazyLogging {

  import KafkaGeoMessageFormatter._

  def main(args: Array[String]): Unit = {

    val jc = new JCommander()
    jc.setProgramName("kafka-ds-log-viewer")

    val params = new KafkaDataStoreLogViewerParameters
    jc.addObject(params)

    try {
      jc.parse(args.toArray: _*)
    } catch {
      case pe: ParameterException =>
        printUsageAndExit(jc, "Error parsing arguments: " + pe.getMessage)
    }

    val zookeepers = params.zookeeper
    val zkPath = params.zkPath
    val sftName = params.sftName

    // TODO support from oldest+n, oldest+t, newest-n, newest-t, time=t, offset=o
    val fromBeginning = params.isFromBeginning

    run(zookeepers, zkPath, sftName, fromBeginning)
  }

  def run(zookeeper: String, zkPath: String, sftName: String, fromBeginning: Boolean): Unit = {
    val featureConfig = new KafkaDataStore(zookeeper, zkPath, 1, 1, null).getFeatureConfig(sftName)

    val formatter = KafkaUtils10.messageFormatClassName
    val sftSpec = encodeSFT(featureConfig.sft)

    var ccArgs = Seq("--topic", featureConfig.topic,
      "--zookeeper", zookeeper,
      "--formatter", formatter,
      "--property", s"$sftNameKey=$sftName",
      "--property", s"$sftSpecKey=$sftSpec")

    if (fromBeginning) {
      ccArgs :+= "--from-beginning"
    }

    logger.debug(s"Running 'kafka-console-consumer ${ccArgs.mkString(" ")}'")
    ConsoleConsumer.main(ccArgs.toArray)
  }

  // double encode so that spec can be passed via command line
  def encodeSFT(sft: SimpleFeatureType): String =
    SimpleFeatureTypes.encodeType(sft).replaceAll("%", "%37").replaceAll("=", "%61")

  def decodeSFT(name: String, spec: String): SimpleFeatureType =
    SimpleFeatureTypes.createType(name, spec.replaceAll("%61", "=").replaceAll("%37", "%"))

  def printUsageAndExit(jc: JCommander, message: String) {
    System.err.println(message)
    jc.usage()
    sys.exit(-1)
  }
}

class KafkaDataStoreLogViewerParameters {

  import KafkaDataStoreLogViewerParameters._

  @Parameter(names = Array("-z", "--zookeeper"), required = true,
    description = "REQUIRED: The connection string for zookeepe.")
  var zookeeper: String = null

  @Parameter(names = Array("-p", "--zkPath"), required = true,
    description = "REQUIRED: The base zookeeper path.  Must match the zkPath used to configure "
      + "the Kafka Data Store.")
  var zkPath: String = null

  @Parameter(names = Array("-n", "--sftName"), required = true,
    description = "REQUIRED: The name of Simple Feature Type.")
  var sftName: String = null

  @Parameter(names = Array("--from"), required = false, validateWith = classOf[FromValidator],
    description = "OPTIONAL: Where to start reading from: {oldest, newest}.")
  var from: String = fromOldest

  def isFromBeginning = fromOldest.equals(from)
}

object KafkaDataStoreLogViewerParameters {

  val fromOldest = "oldest"
  val fromNewest = "newest"

  val validFromOptions = Seq(fromOldest, fromNewest)

  class FromValidator extends IParameterValidator {

    override def validate(name: String, value: String): Unit = {
      if (!validFromOptions.contains(value)) {
        throw new ParameterException(
          s"Parameter '$name' must be one of ${validFromOptions.mkString("{", ", ", "}")} (found '$value')")
      }
    }
  }
}

/** A [[MessageFormatter]] that can be used with the kafka-console-consumer to format the internal log
  * messages of the [[KafkaDataStore]]
  *
  * To use add arguments:
  * --formatter org.locationtech.geomesa.kafka.KafkaGeoMessageFormatter
  * --property sft.name={sftName}
  * --property sft.spec={sftSpec}
  *
  * In order to pass the spec via a command argument all "%" characters must be replaced by "%37" and all
  * "=" characters must be replaced by "%61".
  *
  * @see KafkaDataStoreLogViewer for an alternative to kafka-console-consumer
  */
class KafkaGeoMessageFormatter extends MessageFormatter {

  import KafkaGeoMessageFormatter._

  private var decoder: KafkaGeoMessageDecoder = null

  override def init(props: Properties): Unit = {
    if (!props.containsKey(sftNameKey)) {
      throw new IllegalArgumentException(s"Property '$sftNameKey' is required.")
    }

    if (!props.containsKey(sftSpecKey)) {
      throw new IllegalArgumentException(s"Property '$sftSpecKey' is required.")
    }

    val name = props.getProperty(sftNameKey)
    val spec = props.getProperty(sftSpecKey)

    val sft = KafkaDataStoreLogViewer.decodeSFT(name, spec)
    decoder = new KafkaGeoMessageDecoder(sft)
  }

  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    val msg = decoder.decode(consumerRecord.key, consumerRecord.value)

    output.write(msg.toString.getBytes(StandardCharsets.UTF_8))
    output.write(lineSeparator)
  }

  override def close(): Unit = {
    decoder = null
  }
}

object KafkaGeoMessageFormatter {
  private[kafka10] val sftNameKey = "sft.name"
  private[kafka10] val sftSpecKey = "sft.spec"

  val lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)
}
