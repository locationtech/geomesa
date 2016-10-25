/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka

import org.locationtech.geomesa.tools.common.Runner
import org.locationtech.geomesa.tools.common.commands.{Command, VersionCommand}
import org.locationtech.geomesa.tools.kafka.commands._

object KafkaRunner extends Runner {
  override val scriptName: String = "geomesa-kafka"
  override val commands: List[Command] = List(
    new CreateSchemaCommand(jc),
    new HelpCommand(jc),
    new VersionCommand(jc),
    new RemoveSchemaCommand(jc),
    new GetSchemaCommand(jc),
    new GetNamesCommand(jc),
    new ListenCommand(jc),
    new KeywordCommand(jc)
  )
}
