/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.awt.RenderingHints
import java.io.Serializable
import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.DataStoreFactorySpi
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.kafka.confluent.ConfluentGeoMessageSerializer.ConfluentGeoMessageSerializerFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreFactory}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class ConfluentKafkaDataStoreFactory extends DataStoreFactorySpi {

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore = {
    val config = KafkaDataStoreFactory.buildConfig(params)
    val url = ConfluentKafkaDataStoreFactory.SchemaRegistryUrl.lookup(params)
    val metadata = new ConfluentMetadata(new CachedSchemaRegistryClient(url.toExternalForm, 100))
    val serialization = new ConfluentGeoMessageSerializerFactory(url)
    new KafkaDataStore(config, metadata, serialization)
  }

  override def getDisplayName: String = ConfluentKafkaDataStoreFactory.DisplayName

  override def getDescription: String = ConfluentKafkaDataStoreFactory.Description

  // note: we don't return producer configs, as they would not be used in geoserver
  override def getParametersInfo: Array[Param] =
    ConfluentKafkaDataStoreFactory.ParameterInfo :+ KafkaDataStoreFactoryParams.NamespaceParam

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    ConfluentKafkaDataStoreFactory.canProcess(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object ConfluentKafkaDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  override val DisplayName = "Confluent Kafka (GeoMesa)"
  override val Description = "Confluent Apache Kafka\u2122 distributed log"

  val SchemaRegistryUrl = new GeoMesaParam[URL]("kafka.schema.registry.url", "URL to a confluent schema registry server, used to read Confluent schemas (experimental)")

  override val ParameterInfo: Array[GeoMesaParam[_]] = KafkaDataStoreFactory.ParameterInfo.+:(SchemaRegistryUrl)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean = {
    KafkaDataStoreFactoryParams.Brokers.exists(params) &&
        KafkaDataStoreFactoryParams.Zookeepers.exists(params) &&
        SchemaRegistryUrl.exists(params)
  }
}
