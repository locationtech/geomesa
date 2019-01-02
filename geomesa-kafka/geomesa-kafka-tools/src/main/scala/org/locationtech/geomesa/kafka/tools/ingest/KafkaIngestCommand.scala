/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.ingest

import java.io.{File, InputStream}
import java.util.concurrent.atomic.AtomicLong

import com.beust.jcommander.{Parameter, Parameters}
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.ingest.KafkaIngestCommand.KafkaIngestParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, ProducerDataStoreParams}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.LocalIngestConverter
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
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
  override protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config, ingestFiles: Seq[String]): Runnable = {
    val delay = params.delay.toMillis
    if (delay <= 0) { super.createConverterIngest(sft, converterConfig, ingestFiles) } else {
      Command.user.info(s"Inserting delay of ${params.delay}")
      new ConverterIngest(sft, connection, converterConfig, ingestFiles, Option(params.mode), libjarsFile, libjarsPaths, params.threads, None, params.waitForCompletion) {
        override def createLocalConverter(path: String, failures: AtomicLong): LocalIngestConverter = {
          new LocalIngestConverterImpl(sft, path, converters, failures) {
            override def convert(is: InputStream): Iterator[SimpleFeature] = {
              val converted = converter.process(is, ec)
              new Iterator[SimpleFeature] {
                override def hasNext: Boolean = converted.hasNext
                override def next(): SimpleFeature = {
                  Thread.sleep(delay)
                  converted.next
                }
              }
            }
          }
        }
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
