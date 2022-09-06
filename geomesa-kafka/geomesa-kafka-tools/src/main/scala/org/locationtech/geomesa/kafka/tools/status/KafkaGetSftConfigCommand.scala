/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.status.KafkaGetSftConfigCommand.KafkaGetSftConfigParameters
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, StatusDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}

class KafkaGetSftConfigCommand extends GetSftConfigCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new KafkaGetSftConfigParameters
}

object KafkaGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class KafkaGetSftConfigParameters extends GetSftConfigParams with StatusDataStoreParams with RequiredTypeNameParam
}

