/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.data.KafkaCreateSchemaCommand.KafkaCreateSchemaParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, StatusDataStoreParams}
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams

class KafkaCreateSchemaCommand extends CreateSchemaCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new KafkaCreateSchemaParams()
}

object KafkaCreateSchemaCommand {
  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class KafkaCreateSchemaParams extends CreateSchemaParams with StatusDataStoreParams
}
