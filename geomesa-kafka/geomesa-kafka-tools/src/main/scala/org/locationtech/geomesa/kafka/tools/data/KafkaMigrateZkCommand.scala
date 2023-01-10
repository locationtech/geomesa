/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.data

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreParams}
import org.locationtech.geomesa.kafka.tools.data.KafkaMigrateZkCommand.KafkaMigrateZkParams
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, KafkaDataStoreParams}
import org.locationtech.geomesa.tools.Command

import java.io.File
import scala.concurrent.duration.Duration

class KafkaMigrateZkCommand extends KafkaDataStoreCommand {

  override val name: String = "migrate-zookeeper-metadata"
  override val params = new KafkaMigrateZkParams

  private var useZk = false

  override def connection: Map[String, String] =
    if (useZk) { super.connection } else { super.connection - KafkaDataStoreParams.Zookeepers.key }

  override def execute(): Unit = {
    useZk = true
    withDataStore { withZk =>
      useZk = false
      withDataStore { noZk =>
        val existing = noZk.getTypeNames
        withZk.getTypeNames.foreach { typeName =>
          var delete = params.delete
          if (existing.contains(typeName)) {
            // skip deletion of migrating feature type if it doesn't refer to the same data topic
            if (KafkaDataStore.topic(withZk.getSchema(typeName)) == KafkaDataStore.topic(noZk.getSchema(typeName))) {
              Command.user.info(s"Schema '$typeName' already exists in the destination store")
            } else {
              delete = false
              Command.user.warn(
                s"Schema '$typeName' already exists in the destination store and uses a different topic - skipping migration")
            }
          } else {
            Command.user.info(s"Migrating schema '$typeName'")
            noZk.createSchema(withZk.getSchema(typeName))
          }
          if (delete) {
            Command.user.info("Deleting schema from Zookeeper")
            withZk.metadata.delete(typeName)
          }
        }
        Command.user.info(s"Complete")
      }
    }
  }
}

object KafkaMigrateZkCommand {

  @Parameters(commandDescription = "Migrate schemas from Zookeeper to Kafka")
  class KafkaMigrateZkParams extends KafkaDataStoreParams {

    @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
    var zookeepers: String = _

    @Parameter(names = Array("--replication"), description = "Replication factor for Kafka metadata topic")
    var replication: Int = 1 // note: can't use override modifier since it's a var

    @Parameter(names = Array("--delete"), description = "Delete the metadata out of Zookeeper after migration")
    var delete: Boolean = false

    @Parameter(names = Array("--config"), description = "Properties file used to configure the Kafka admin client")
    var producerProperties: File = _

    override val serialization: String = null
    override val consumerProperties: File = null
    override val partitions: Int = 1 // note: ignored for the metadata topic
    override val numConsumers: Int = 0
    override val readBack: Duration = null
    override val fromBeginning: Boolean = false
  }
}
