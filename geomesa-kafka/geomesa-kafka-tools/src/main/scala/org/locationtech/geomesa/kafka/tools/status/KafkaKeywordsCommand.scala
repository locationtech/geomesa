/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander._
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.status.KafkaKeywordsCommand.KafkaKeywordsParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, StatusDataStoreParams}
import org.locationtech.geomesa.tools.status.{KeywordsCommand, KeywordsParams}

class KafkaKeywordsCommand extends KeywordsCommand[KafkaDataStore] with KafkaDataStoreCommand {
  override val params = new KafkaKeywordsParams
}

object KafkaKeywordsCommand {
  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class KafkaKeywordsParams extends StatusDataStoreParams with KeywordsParams
}
