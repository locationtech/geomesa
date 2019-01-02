/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.kafka.tools.data.{KafkaCreateSchemaCommand, KafkaRemoveSchemaCommand}
import org.locationtech.geomesa.kafka.tools.export.{KafkaExportCommand, KafkaListenCommand}
import org.locationtech.geomesa.kafka.tools.ingest.KafkaIngestCommand
import org.locationtech.geomesa.kafka.tools.status._
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object KafkaRunner extends Runner {

  override val name: String = "geomesa-kafka"

  override protected def createCommands(jc: JCommander): Seq[Command] = Seq(
    new KafkaCreateSchemaCommand,
    new KafkaRemoveSchemaCommand,
    new KafkaListenCommand,
    new KafkaExportCommand,
    new KafkaIngestCommand,
    new KafkaDescribeSchemaCommand,
    new KafkaGetTypeNamesCommand,
    new KafkaKeywordsCommand,
    new HelpCommand(this, jc),
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand,
    new VersionCommand
  )

  override def environmentErrorInfo(): Option[String] = {
    if (sys.env.get("KAFKA_HOME").isEmpty) {
      Option("\nWarning: KAFKA_HOME is not set as an environment variable." +
          "\nGeoMesa tools will not run without the appropriate Kafka and Zookeeper jars in the tools classpath." +
          "\nPlease ensure that those jars are present in the classpath by running 'geomesa-kafka classpath'." +
          "\nTo take corrective action, please place the necessary jar files in the lib directory of geomesa-tools.")
    } else { None }
  }
}
