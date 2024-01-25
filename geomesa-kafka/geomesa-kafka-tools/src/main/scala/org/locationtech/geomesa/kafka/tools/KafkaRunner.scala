/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import org.locationtech.geomesa.tools.{Command, Runner}

object KafkaRunner extends Runner {

  override val name: String = "geomesa-kafka"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new data.KafkaCreateSchemaCommand,
      new data.KafkaMigrateZkCommand,
      new data.KafkaRemoveSchemaCommand,
      new data.KafkaUpdateSchemaCommand,
      new export.KafkaListenCommand,
      new export.KafkaExportCommand,
      new ingest.KafkaIngestCommand,
      new status.KafkaDescribeSchemaCommand,
      new status.KafkaGetSftConfigCommand,
      new status.KafkaGetTypeNamesCommand
    )
  }

  override protected def classpathEnvironments: Seq[String] = Seq("KAFKA_HOME")
}
