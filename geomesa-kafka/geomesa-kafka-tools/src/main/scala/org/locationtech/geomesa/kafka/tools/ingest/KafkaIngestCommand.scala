/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.ingest.KafkaIngestCommand.KafkaIngestParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, ProducerDataStoreParams}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.concurrent.duration.Duration

class KafkaIngestCommand extends IngestCommand[KafkaDataStore] with KafkaDataStoreCommand {

  override val params = new KafkaIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/kafka/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_KAFKA_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("KAFKA_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[KafkaDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Producer[_, _]])
  )

  // override to add delay in writing messages
  override protected def createIngest(
      mode: RunMode,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Seq[String]): Runnable = {
    val delay = params.delay.toMillis
    if (delay <= 0) { super.createIngest(mode, sft, converter, inputs) } else {
      if (mode != RunModes.Local) {
        throw new ParameterException("Delay is only supported for local ingest")
      }
      Command.user.info(s"Inserting delay of ${params.delay}")
      new LocalConverterIngest(connection, sft, converter, inputs, params.threads) {
        override protected def features(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
          iter.map { f => Thread.sleep(delay); f }
      }
    }
  }
}

object KafkaIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class KafkaIngestParams extends IngestParams with ProducerDataStoreParams {
    @Parameter(names = Array("--delay"), description = "Artificial delay inserted between messages, as a duration (e.g. '100ms')", converter = classOf[DurationConverter])
    var delay: Duration = Duration.Zero
  }
}
