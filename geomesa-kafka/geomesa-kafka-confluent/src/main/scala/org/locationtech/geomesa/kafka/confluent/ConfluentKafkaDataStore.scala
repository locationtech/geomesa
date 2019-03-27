/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.net.URL

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.locationtech.geomesa.kafka.confluent.ConfluentGeoMessageSerializer.ConfluentGeoMessageSerializerFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig

object ConfluentKafkaDataStore {
  def apply(config: KafkaDataStoreConfig, schemaRegistryUrl: URL): KafkaDataStore = {
    val metadata = new ConfluentMetadata(new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100))
    val serialization = new ConfluentGeoMessageSerializerFactory(schemaRegistryUrl)
    new KafkaDataStore(config, metadata, serialization)
  }
}
