/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.export

import com.beust.jcommander.{Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.FeatureEvent.Type
import org.geotools.data.{DataUtilities, FeatureEvent, FeatureListener}
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.kafka.KafkaFeatureEvent
import org.locationtech.geomesa.kafka.tools.{ConsumerKDSConnectionParams, KafkaDataStoreCommand}
import org.locationtech.geomesa.kafka21.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.opengis.filter.Filter

class ListenCommand extends KafkaDataStoreCommand with LazyLogging {

  override val name = "listen"
  override val params = new ListenParameters()

  override def connection: Map[String, String] = {
    val reset = if (params.fromBeginning) "earliest" else "latest"
    super.connection + (KafkaDataStoreFactoryParams.AUTO_OFFSET_RESET.getName -> reset)
  }

  override def execute(): Unit = withDataStore { ds =>
    Command.user.info(s"SimpleFeature type for type ${params.featureName}:")
    Command.user.info(s"${DataUtilities.encodeType(ds.getSchema(params.featureName))}")
    Command.user.info(s"Listening to ${params.featureName}...")

    val listener = new FeatureListener() {
      override def changed(event: FeatureEvent): Unit = {
        val msg = event match {
          case e: KafkaFeatureEvent => s"Feature added: ${DataUtilities.encodeFeature(e.feature)}"
          case e if e.getType == Type.REMOVED && e.getFilter == Filter.INCLUDE => "Features cleared"
          case e if e.getType == Type.REMOVED => s"Features removed: ${IdFilterStrategy.intersectIdFilters(e.getFilter).mkString("'", "', '", "'")}"
          case e => s"Received event: $e"
        }
        Command.user.info(msg)
      }
    }

    val source = ds.getFeatureSource(params.featureName)
    source.addFeatureListener(listener)

    Thread.sleep(Long.MaxValue)
  }
}


@Parameters(commandDescription = "Listen to a GeoMesa Kafka topic")
class ListenParameters extends ConsumerKDSConnectionParams with RequiredTypeNameParam {
  @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
  var fromBeginning: Boolean = false
}
