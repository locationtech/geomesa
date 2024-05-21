/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spring.binder.kafka.datastore;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaDatastoreMessageBinderTest {

    @Mock
    DataStore ds;
    @Mock
    FeatureWriter featureWriter;
    @Mock
    SimpleFeatureStore simpleFeatureStore;
    @Mock
    SubscribableChannel errorChannel;

    Supplier<DataStore> dsFactory = () -> ds;

    @Test
    public void producerMessageHandler_canWriteSft() throws IOException {
        SimpleFeatureType sft = SimpleFeatureTypeLoader.sftForName("observation").get();
        SimpleFeature writeableFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id0");
        doReturn(featureWriter).when(ds).getFeatureWriterAppend(any(), any());
        doReturn(writeableFeature).when(featureWriter).next();

        KafkaDatastoreBinderProvisioner provisioningProvider = new KafkaDatastoreBinderProvisioner();
        provisioningProvider = new KafkaDatastoreBinderProvisioner();
        KafkaDatastoreMessageBinder messageBinder = new KafkaDatastoreMessageBinder(new String[]{}, provisioningProvider, dsFactory);

        ProducerProperties producerProperties = new ProducerProperties();
        ProducerDestination destination = provisioningProvider.provisionProducerDestination("test-out", producerProperties);

        MessageHandler handler = messageBinder.createProducerMessageHandler(destination, producerProperties, errorChannel);


        SimpleFeature simpleFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id1");
        simpleFeature.setAttribute("mmsi", 123456);
        Message<?> message = MessageBuilder.withPayload(simpleFeature)
                .setHeader("featureType", "application/simple-feature")
                .build();

        handler.handleMessage(message);

        verify(featureWriter).write();
        assertThat(writeableFeature.getAttribute("mmsi")).isEqualTo(123456);
    }

    @Test
    public void producerMessageHandler_canWriteKafkaFeatureEventChanged() throws IOException {
        SimpleFeatureType sft = SimpleFeatureTypeLoader.sftForName("observation").get();
        SimpleFeature writeableFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id0");
        doReturn(featureWriter).when(ds).getFeatureWriterAppend(any(), any());
        doReturn(writeableFeature).when(featureWriter).next();

        KafkaDatastoreBinderProvisioner provisioningProvider = new KafkaDatastoreBinderProvisioner();
        provisioningProvider = new KafkaDatastoreBinderProvisioner();
        KafkaDatastoreMessageBinder messageBinder = new KafkaDatastoreMessageBinder(new String[]{}, provisioningProvider, dsFactory);

        ProducerProperties producerProperties = new ProducerProperties();
        ProducerDestination destination = provisioningProvider.provisionProducerDestination("test-out", producerProperties);

        MessageHandler handler = messageBinder.createProducerMessageHandler(destination, producerProperties, errorChannel);


        SimpleFeature simpleFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id1");
        simpleFeature.setAttribute("mmsi", 123456);
        simpleFeature.setAttribute("elevation", 100);
        KafkaFeatureEvent changed = new KafkaFeatureEvent.KafkaFeatureChanged("test", simpleFeature, Instant.now().getEpochSecond());
        Message<?> message = MessageBuilder.withPayload(changed)
                .setHeader("featureType", "application/kafka-feature-event")
                .build();

        handler.handleMessage(message);

        verify(featureWriter).write();
        assertThat(writeableFeature.getAttribute("mmsi")).isEqualTo(123456);
        assertThat(writeableFeature.getAttribute("elevation")).isEqualTo(100f);
    }

    @Test
    public void producerMessageHandler_canWriteKafkaFeatureEventRemoved() throws IOException {
        SimpleFeatureType sft = SimpleFeatureTypeLoader.sftForName("observation").get();
        SimpleFeature writeableFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id0");
        doReturn(simpleFeatureStore).when(ds).getFeatureSource("test-out");

        KafkaDatastoreBinderProvisioner provisioningProvider = new KafkaDatastoreBinderProvisioner();
        provisioningProvider = new KafkaDatastoreBinderProvisioner();
        KafkaDatastoreMessageBinder messageBinder = new KafkaDatastoreMessageBinder(new String[]{}, provisioningProvider, dsFactory);

        ProducerProperties producerProperties = new ProducerProperties();
        ProducerDestination destination = provisioningProvider.provisionProducerDestination("test-out", producerProperties);

        MessageHandler handler = messageBinder.createProducerMessageHandler(destination, producerProperties, errorChannel);


        KafkaFeatureEvent removed = new KafkaFeatureEvent.KafkaFeatureRemoved("test-out", "id1", null, Instant.now().getEpochSecond());
        Message<?> message = MessageBuilder.withPayload(removed)
                .setHeader("featureType", "application/kafka-feature-event")
                .build();

        handler.handleMessage(message);

        verify(simpleFeatureStore).removeFeatures(any(Filter.class));
    }

    @Test
    public void producerMessageHandler_canWriteKafkaFeatureEventClear() throws IOException {
        SimpleFeatureType sft = SimpleFeatureTypeLoader.sftForName("observation").get();
        SimpleFeature writeableFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id0");
        doReturn(simpleFeatureStore).when(ds).getFeatureSource("test-out");

        KafkaDatastoreBinderProvisioner provisioningProvider = new KafkaDatastoreBinderProvisioner();
        provisioningProvider = new KafkaDatastoreBinderProvisioner();
        KafkaDatastoreMessageBinder messageBinder = new KafkaDatastoreMessageBinder(new String[]{}, provisioningProvider, dsFactory);

        ProducerProperties producerProperties = new ProducerProperties();
        ProducerDestination destination = provisioningProvider.provisionProducerDestination("test-out", producerProperties);

        MessageHandler handler = messageBinder.createProducerMessageHandler(destination, producerProperties, errorChannel);


        KafkaFeatureEvent cleared = new KafkaFeatureEvent.KafkaFeatureCleared("test-out", Instant.now().getEpochSecond());
        Message<?> message = MessageBuilder.withPayload(cleared)
                .setHeader("featureType", "application/kafka-feature-event")
                .build();

        handler.handleMessage(message);

        verify(simpleFeatureStore).removeFeatures(Filter.INCLUDE);
    }

}