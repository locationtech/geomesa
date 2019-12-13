/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.status.KafkaGetTypeNamesCommand.GetTypeNamesParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, StatusDataStoreParams}
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand

class KafkaGetTypeNamesCommand extends GetTypeNamesCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new GetTypeNamesParams()
}

object KafkaGetTypeNamesCommand {
  @Parameters(commandDescription = "List the feature types for a given zookeeper path")
  class GetTypeNamesParams extends StatusDataStoreParams
}
