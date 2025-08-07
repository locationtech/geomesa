/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.data.KafkaUpdateSchemaCommand.KafkaUpdateSchemaParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, KafkaDataStoreParams, StatusDataStoreParams}
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams

class KafkaUpdateSchemaCommand extends UpdateSchemaCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new KafkaUpdateSchemaParams()
}

object KafkaUpdateSchemaCommand {
  @Parameters(commandDescription = "Update a GeoMesa feature type")
  class KafkaUpdateSchemaParams extends UpdateSchemaParams with KafkaDataStoreParams with StatusDataStoreParams
}
