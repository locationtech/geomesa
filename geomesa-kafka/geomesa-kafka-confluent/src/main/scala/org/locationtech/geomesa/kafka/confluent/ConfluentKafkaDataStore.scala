/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.locationtech.geomesa.kafka.confluent.ConfluentGeoMessageSerializer.ConfluentGeoMessageSerializerFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.opengis.feature.simple.SimpleFeatureType

import java.net.URL

object ConfluentKafkaDataStore {

  def apply(
      config: KafkaDataStoreConfig,
      schemaRegistryUrl: URL,
      schemaOverrides: Map[String, (SimpleFeatureType, Schema)]): KafkaDataStore = {
    val sftNameToSchema = schemaOverrides.values.map { case (sft, schema) => sft.getTypeName -> schema }.toMap
    val topicToSft = schemaOverrides.map { case (topic, (sft, _)) => topic -> sft }

    val client = new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100)
    val metadata = new ConfluentMetadata(client, topicToSft)
    val serialization = new ConfluentGeoMessageSerializerFactory(schemaRegistryUrl, sftNameToSchema)

    new KafkaDataStore(config, metadata, serialization) {
      override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit =
        throw new NotImplementedError("Confluent Kafka stores do not support updateSchema")
    }
  }
}
