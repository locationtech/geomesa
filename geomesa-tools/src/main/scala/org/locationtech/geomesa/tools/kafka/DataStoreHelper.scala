/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka

import org.locationtech.geomesa.kafka.{KafkaDataStoreFactory, KafkaDataStoreFactoryParams => dsParams}
import org.locationtech.geomesa.tools.kafka.commands.KafkaConnectionParams

import scala.collection.JavaConversions._

class DataStoreHelper(params: KafkaConnectionParams) {
  lazy val paramMap = Map[String, String](
    dsParams.KAFKA_BROKER_PARAM.getName -> params.brokers,
    dsParams.ZOOKEEPERS_PARAM.getName   -> params.zookeepers,
    dsParams.ZK_PATH.getName            -> params.zkPath,
    dsParams.IS_PRODUCER_PARAM.getName  -> params.isProducer.toString,
    dsParams.TOPIC_PARTITIONS.getName   -> Option(params.partitions).orNull,
    dsParams.TOPIC_REPLICATION.getName  -> Option(params.replication).orNull).filter(_._2 != null)

  /**
    * Gets a KDS
    *
    * @throws Exception if the KDS cannot be created with the given parameters
    */
  def getDataStore() = {
    val kdsFactory = new KafkaDataStoreFactory()
    Option(kdsFactory.createDataStore(paramMap)).getOrElse {
      throw new Exception("Could not load a data store with the provided parameters: " +
        s"${paramMap.map { case (k,v) => s"$k=$v" }.mkString(",")}")
    }
  }
}
