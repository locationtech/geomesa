/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.kafka.tools.data.{KafkaCreateSchemaCommand, KafkaRemoveSchemaCommand}
import org.locationtech.geomesa.kafka.tools.export.ListenCommand
import org.locationtech.geomesa.kafka.tools.status._
import org.locationtech.geomesa.tools.status.{HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.{Command, ConvertCommand, Runner}

object KafkaRunner extends Runner {

  override val name: String = "geomesa-kafka"

  override protected def createCommands(jc: JCommander): Seq[Command] = Seq(
    new KafkaCreateSchemaCommand,
    new HelpCommand(this, jc),
    new VersionCommand,
    new KafkaRemoveSchemaCommand,
    new KafkaDescribeSchemaCommand,
    new KafkaGetTypeNamesCommand,
    new ListenCommand,
    new KafkaKeywordsCommand,
    new ConvertCommand
  )

}
