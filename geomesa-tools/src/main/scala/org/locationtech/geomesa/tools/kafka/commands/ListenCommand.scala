/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.KafkaDataStoreLogViewer
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.tools.kafka.ConsumerKDSConnectionParams
import org.locationtech.geomesa.tools.kafka.commands.ListenCommand.ListenParameters

class ListenCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "listen"
  override val params = new ListenParameters()

  override def execute(): Unit = {
    println(s"Listening to ${params.featureName}...")
    KafkaDataStoreLogViewer.run(params.zookeepers, params.zkPath, params.featureName, params.fromBeginning)
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