/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.jobs.Awaitable
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.KafkaDataStoreCommand.KafkaDistributedCommand
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, ProducerDataStoreParams}
import org.locationtech.geomesa.kafka.tools.ingest.KafkaIngestCommand.KafkaIngestParams
import org.locationtech.geomesa.tools.{Command, ConverterConfigParam, OptionalFeatureSpecParam, OptionalForceParam, OptionalInputFormatParam, OptionalTypeNameParam}
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestParams, Inputs}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.utils.iterators.SimplePlaybackIterator
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}

import scala.concurrent.duration.Duration

class KafkaPlaybackCommand extends IngestCommand[KafkaDataStore] with KafkaDataStoreCommand {

  import scala.collection.JavaConverters._

  override val name = "playback"
  override val params = new KafkaPlaybackCommand.KafkaPlaybackParams()

  // override to add delay in writing messages
  override protected def startIngest(
                                      mode: RunMode,
                                      ds: KafkaDataStore,
                                      sft: SimpleFeatureType,
                                      converter: Config,
                                      inputs: Inputs): Awaitable = {
    if (mode != RunModes.Local) {
      throw new ParameterException("Distributed ingest is not supported for playback")
    }

    val dtg = Option(params.dtg)
    val rate: Float = Option(params.rate).map(_.floatValue()).getOrElse(1f)
    val live = Option(params.live).exists(_.booleanValue())

    Command.user.info(s"Starting playback...")
    new LocalConverterIngest(ds, connection.asJava, sft, converter, inputs, 1) {
      override protected def features(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
        new SimplePlaybackIterator(iter, sft, dtg, null, rate, live)
    }
  }
}

object KafkaPlaybackCommand {
  @Parameters(commandDescription = "Playback features onto Kafka from time-ordered file(s), based on the feature date")
  class KafkaPlaybackParams extends IngestParams with ProducerDataStoreParams {

    @Parameter(names = Array("--dtg"), description = "Date attribute to base playback on")
    var dtg: String = _

    @Parameter(names = Array("--rate"), description = "Rate multiplier to speed-up (or slow down) features being returned")
    var rate: java.lang.Float = _

    @Parameter(names = Array("--live"), description = "Simulate live data by projecting the dates to current time")
    var live: java.lang.Boolean = _
  }
}
