/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.kafka10.{KafkaDataStore, KafkaDataStoreFactoryParams}
import org.locationtech.geomesa.tools.Command

/**
  * Abstract class for commands that require a KafkaDataStore
  */
trait KafkaDataStoreCommand extends Command {

  override def params: KafkaConnectionParams

  def connection: Map[String, String] = Map[String, String](
    KafkaDataStoreFactoryParams.KAFKA_BROKER_PARAM.getName -> params.brokers,
    KafkaDataStoreFactoryParams.ZOOKEEPERS_PARAM.getName   -> params.zookeepers,
    KafkaDataStoreFactoryParams.ZK_PATH.getName            -> params.zkPath,
    KafkaDataStoreFactoryParams.IS_PRODUCER_PARAM.getName  -> params.isProducer.toString,
    KafkaDataStoreFactoryParams.TOPIC_PARTITIONS.getName   -> params.partitions,
    KafkaDataStoreFactoryParams.TOPIC_REPLICATION.getName  -> params.replication
  ).filter(_._2 != null)

  def withDataStore[T](method: (KafkaDataStore) => T): T = {
    import scala.collection.JavaConversions._
    val ds = DataStoreFinder.getDataStore(connection).asInstanceOf[KafkaDataStore]
    try { method(ds) } finally {
      ds.dispose()
    }
  }
}
