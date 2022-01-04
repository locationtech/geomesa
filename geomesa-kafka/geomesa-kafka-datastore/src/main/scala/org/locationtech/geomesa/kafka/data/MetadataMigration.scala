/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.zk.CuratorHelper

import java.nio.charset.StandardCharsets

/**
  * Migrates kafka data store metadata from geomesa 1.3.x to 1.4.x
  *
  * @param ds datastore
  * @param zkPath zk path
  * @param zookeepers zookeepers
  */
class MetadataMigration(ds: KafkaDataStore, zkPath: String, zookeepers: String) extends Runnable {

  override def run(): Unit = {
    import scala.collection.JavaConverters._

    val client = CuratorHelper.client(zookeepers).namespace(zkPath).build()

    try {
      client.start()
      client.blockUntilConnected()

      if (client.checkExists().forPath("/") != null) {
        client.getChildren.forPath("/").asScala.foreach { name =>
          if (name != KafkaDataStore.MetadataPath && client.checkExists().forPath(s"/$name/Topic") != null) {
            if (name.indexOf("-REPLAY-") == -1) {
              val schema = new String(client.getData.forPath(s"/$name"), StandardCharsets.UTF_8)
              val sft = SimpleFeatureTypes.createType(name, schema)
              KafkaDataStore.setTopic(sft, new String(client.getData.forPath(s"/$name/Topic"), StandardCharsets.UTF_8))
              sft.getUserData.put(SimpleFeatureTypes.Configs.OverrideReservedWords, "true")
              ds.createSchema(sft)
            }
            client.delete().deletingChildrenIfNeeded().forPath(s"/$name")
          }
        }
      }
    } finally {
      client.close()
    }
  }
}
