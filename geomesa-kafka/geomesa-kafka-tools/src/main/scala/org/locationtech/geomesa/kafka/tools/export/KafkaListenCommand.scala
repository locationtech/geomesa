/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.export

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataUtilities, FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.tools.export.KafkaListenCommand.{ListenParameters, OutFeatureListener}
import org.locationtech.geomesa.kafka.tools.{ConsumerDataStoreParams, KafkaDataStoreCommand}
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.{KafkaFeatureChanged, KafkaFeatureCleared, KafkaFeatureRemoved}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class KafkaListenCommand extends KafkaDataStoreCommand with LazyLogging {

  override val name = "listen"
  override val params = new ListenParameters()

  override def execute(): Unit = withDataStore { ds =>
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Type ${params.featureName} does not exist at path ${params.zkPath}")
    }
    Command.user.info(s"Listening to '${sft.getTypeName}' ${SimpleFeatureTypes.encodeType(sft)} ...")

    ds.getFeatureSource(sft.getTypeName).addFeatureListener(new OutFeatureListener)

    try {
      while (true) {
        Thread.sleep(1000L)
      }
    } catch {
      case _: InterruptedException => // exit
    }
  }
}

object KafkaListenCommand {

  @Parameters(commandDescription = "Listen to a GeoMesa Kafka topic")
  class ListenParameters extends ConsumerDataStoreParams with RequiredTypeNameParam

  class OutFeatureListener extends FeatureListener {
    private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)
    private def format(ts: Long): String =
      ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC).format(formatter)

    override def changed(event: FeatureEvent): Unit = {
      val msg = event match {
        case e: KafkaFeatureChanged => s"${format(e.time)} [add/update] ${DataUtilities.encodeFeature(e.feature)}"
        case e: KafkaFeatureRemoved => s"${format(e.time)} [delete]     ${Option(e.feature).map(DataUtilities.encodeFeature).getOrElse(e.id)}"
        case e: KafkaFeatureCleared => s"${format(e.time)} [clear]"
        case e                      => s"Unknown event $e"
      }
      Command.output.info(msg)
    }
  }
}
