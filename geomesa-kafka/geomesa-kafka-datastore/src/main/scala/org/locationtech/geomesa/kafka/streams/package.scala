/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.apache.kafka.streams.processor.StreamPartitioner
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer

import java.nio.charset.StandardCharsets

package object streams {

  /**
   * Trait for provided metadata about a feature type topic
   */
  trait HasTopicMetadata {

    /**
     * Gets the topic associated with a feature type
     *
     * @param typeName feature type name
     * @return
     */
    def topic(typeName: String): String

    /**
     * Gets the partitioning associated with a feature type
     *
     * @param typeName feature type name
     * @return true if Kafka default partitioning is used, false if custom partitioning is used
     */
    def usesDefaultPartitioning(typeName: String): Boolean
  }

  /**
   * Kafka partitioner for GeoMesa messages, to make sure all updates for a given
   * feature go to the same partition
   */
  class GeoMessageStreamPartitioner extends StreamPartitioner[String, GeoMesaMessage] {
    override def partition(
        topic: String,
        key: String,
        value: GeoMesaMessage,
        numPartitions: Int): Integer = {
      GeoMessageSerializer.partition(numPartitions,
        if (key == null) { null } else { key.getBytes(StandardCharsets.UTF_8) })
    }
  }
}
