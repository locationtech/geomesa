/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
      new data.KafkaRemoveSchemaCommand,
      new data.KafkaUpdateSchemaCommand,
      new export.KafkaListenCommand,
      new export.KafkaExportCommand,
      new ingest.KafkaIngestCommand,
      new status.KafkaDescribeSchemaCommand,
      new status.KafkaGetTypeNamesCommand
    )
  }

  override def environmentErrorInfo(): Option[String] = {
    if (!sys.env.contains("KAFKA_HOME")) {
      Option("\nWarning: KAFKA_HOME is not set as an environment variable." +
          "\nGeoMesa tools will not run without the appropriate Kafka and Zookeeper jars in the tools classpath." +
          "\nPlease ensure that those jars are present in the classpath by running 'geomesa-kafka classpath'." +
          "\nTo take corrective action, please place the necessary jar files in the lib directory of geomesa-tools.")
    } else { None }
  }
}
