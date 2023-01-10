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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.jstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

import java.util.Map;

/**
 * Wrapper for a kafka streams builder that will configure serialization based on a GeoMesa Kafka feature store
 */
public class GeoMesaStreamsBuilder {

    private final org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder sBuilder;
    private final StreamsBuilder wrapped;

    private GeoMesaStreamsBuilder(org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder sBuilder, StreamsBuilder wrapped) {
        this.sBuilder = sBuilder;
        this.wrapped = wrapped;
    }

    /**
     * Create a streams builder
     *
     * @param params data store parameters
     */
    public static GeoMesaStreamsBuilder create(Map<String, String> params) {
        return create(params, null, null, null);
    }

    /**
     * Create a streams builder
     *
     * @param params         data store parameters
     * @param streamsBuilder underlying streams builder to use
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          StreamsBuilder streamsBuilder) {
        return create(params, null, null, streamsBuilder);
    }

    /**
     * Create a streams builder
     *
     * @param params             data store parameters
     * @param timestampExtractor timestamp extractor for message stream
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          TimestampExtractor timestampExtractor) {
        return create(params, timestampExtractor, null, null);
    }

    /**
     * Create a streams builder
     *
     * @param params             data store parameters
     * @param timestampExtractor timestamp extractor for message stream
     * @param streamsBuilder     underlying streams builder to use
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          TimestampExtractor timestampExtractor,
          StreamsBuilder streamsBuilder) {
        return create(params, timestampExtractor, null, streamsBuilder);
    }

    /**
     * Create a streams builder
     *
     * @param params      data store parameters
     * @param resetPolicy auto offset reset for reading existing topics
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          AutoOffsetReset resetPolicy) {
        return create(params, null, resetPolicy, null);
    }

    /**
     * Create a streams builder
     *
     * @param params         data store parameters
     * @param resetPolicy    auto offset reset for reading existing topics
     * @param streamsBuilder underlying streams builder to use
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          AutoOffsetReset resetPolicy,
          StreamsBuilder streamsBuilder) {
        return create(params, null, resetPolicy, streamsBuilder);
    }

    /**
     * Create a streams builder
     *
     * @param params             data store parameters
     * @param timestampExtractor timestamp extractor for message stream
     * @param resetPolicy        auto offset reset for reading existing topics
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          TimestampExtractor timestampExtractor,
          AutoOffsetReset resetPolicy) {
        return create(params, timestampExtractor, resetPolicy, null);
    }

    /**
     * Create a streams builder
     *
     * @param params             data store parameters
     * @param timestampExtractor timestamp extractor for message stream
     * @param resetPolicy        auto offset reset for reading existing topics
     * @param streamsBuilder     underlying streams builder to use
     */
    public static GeoMesaStreamsBuilder create(
          Map<String, String> params,
          TimestampExtractor timestampExtractor,
          AutoOffsetReset resetPolicy,
          StreamsBuilder streamsBuilder) {
        if (streamsBuilder == null) {
            streamsBuilder = new StreamsBuilder();
        }
        org.apache.kafka.streams.scala.StreamsBuilder scalaBuilder =
              new org.apache.kafka.streams.scala.StreamsBuilder(streamsBuilder);
        org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder sbuilder =
              org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder.apply(params, timestampExtractor, resetPolicy, scalaBuilder);
        return new GeoMesaStreamsBuilder(sbuilder, streamsBuilder);
    }

    /**
     * Create a stream of updates for a given feature type
     *
     * @param typeName feature type name
     */
    public KStream<String, GeoMesaMessage> stream(String typeName) {
        return sBuilder.stream(typeName).inner();
    }

    /**
     * Create a table for a given feature type
     *
     * @param typeName feature type name
     */
    public KTable<String, GeoMesaMessage> table(String typeName) {
        return sBuilder.table(typeName).inner();
    }

    /**
     * Create a table for a given feature type
     *
     * @param typeName     feature type name
     * @param materialized materialized
     */
    public KTable<String, GeoMesaMessage> table(
          String typeName,
          Materialized<String, GeoMesaMessage, KeyValueStore<Bytes, byte[]>> materialized) {
        return sBuilder.table(typeName, materialized).inner();
    }

    /**
     * Create a global table for a given feature type
     *
     * @param typeName feature type name
     */
    public GlobalKTable<String, GeoMesaMessage> globalTable(String typeName) {
        return sBuilder.globalTable(typeName);
    }

    /**
     * Create a global table for a given feature type
     *
     * @param typeName     feature type name
     * @param materialized materialized
     */
    public GlobalKTable<String, GeoMesaMessage> globalTable(
          String typeName,
          Materialized<String, GeoMesaMessage, KeyValueStore<Bytes, byte[]>> materialized) {
        return sBuilder.globalTable(typeName, materialized);
    }

    /**
     * Write the stream to the given feature type, which must already exist. The messages
     * must conform to the feature type schema
     *
     * @param typeName feature type name
     * @param stream   stream to persist
     */
    public void to(String typeName, KStream<String, GeoMesaMessage> stream) {
        sBuilder.to(typeName, new org.apache.kafka.streams.scala.kstream.KStream<>(stream));
    }

    /**
     * Convenience method to build the underlying topology
     */
    public Topology build() { return sBuilder.build(); }

    /**
     * Get the underlying streams builder
     *
     * @return the builder
     */
    public StreamsBuilder wrapped() {
        return this.wrapped;
    }

    /**
     * Get the `GeoMesaMessage` serde for the given feature type
     *
     * @param typeName feature type name
     * @return the serde
     */
    public Serde<GeoMesaMessage> serde(String typeName) {
        return sBuilder.serde(typeName);
    }
}
