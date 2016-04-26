/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.JCommander
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.tools.kafka.{DataStoreHelper, KafkaConnectionParams}

/**
  * Abstract class for commands that require a KafkaDataStore
  */
abstract class CommandWithKDS(parent: JCommander) extends Command(parent) {
  override val params: KafkaConnectionParams
  lazy val ds = new DataStoreHelper(params).getDataStore
  lazy val zkPath = params.zkPath

  // ZkClient which some commands use
  lazy val zkClient = new ZkClient(params.zookeepers, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
}