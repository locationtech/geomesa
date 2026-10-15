/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaDatastoreMessageBinderTest {

    DataStore ds;
    FeatureWriter featureWriter;
    SimpleFeatureStore simpleFeatureStore;
    SubscribableChannel errorChannel;
    Supplier<DataStore> dsFactory;

    @Before
    public void init() {
        ds = mock(DataStore.class);
        featureWriter = mock(FeatureWriter.class);
        simpleFeatureStore = mock(SimpleFeatureStore.class);
        errorChannel = mock(SubscribableChannel.class);
        dsFactory = () -> ds;
    }

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
        simpleFeature.setAttribute("id", "123456");
        Message<?> message = MessageBuilder.withPayload(simpleFeature)
                .setHeader("featureType", "application/simple-feature")
                .build();

        handler.handleMessage(message);

        verify(featureWriter).write();
        assertThat(writeableFeature.getAttribute("id")).isEqualTo("123456");
    }

    @Test
    public void producerMessageHandler_canWriteKafkaFeatureEventChanged() throws IOException {
        var now = Instant.now();
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
        simpleFeature.setAttribute("id", "123456");
        simpleFeature.setAttribute("dtg", now);
        KafkaFeatureEvent changed = new KafkaFeatureEvent.KafkaFeatureChanged("test", simpleFeature, Instant.now().getEpochSecond());
        Message<?> message = MessageBuilder.withPayload(changed)
                .setHeader("featureType", "application/kafka-feature-event")
                .build();

        handler.handleMessage(message);

        verify(featureWriter).write();
        assertThat(writeableFeature.getAttribute("id")).isEqualTo("123456");
        assertThat(writeableFeature.getAttribute("dtg")).isEqualTo(Date.from(now));
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

    @Test
    public void producerMessageHandler_loadSftFromClasspath() throws IOException {
        SimpleFeatureType sft = SimpleFeatureTypeLoader.sftForName("observation").get();
        SimpleFeature writeableFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id0");
        doReturn(featureWriter).when(ds).getFeatureWriterAppend(any(), any());
        doReturn(writeableFeature).when(featureWriter).next();

        KafkaDatastoreBinderProvisioner provisioningProvider = new KafkaDatastoreBinderProvisioner();
        provisioningProvider = new KafkaDatastoreBinderProvisioner();
        KafkaDatastoreMessageBinder messageBinder = new KafkaDatastoreMessageBinder(new String[]{}, provisioningProvider, dsFactory);

        ProducerProperties producerProperties = new ProducerProperties();
        ProducerDestination destination = provisioningProvider.provisionProducerDestination("observation", producerProperties);

        // Doesn't throw an error
        MessageHandler handler = messageBinder.createProducerMessageHandler(destination, producerProperties, errorChannel);

        SimpleFeature simpleFeature = SimpleFeatureBuilder.build(sft, new ArrayList<>(), "id1");
        simpleFeature.setAttribute("id", "123456");
        KafkaFeatureEvent changed = new KafkaFeatureEvent.KafkaFeatureChanged("test", simpleFeature, Instant.now().getEpochSecond());
        Message<?> message = MessageBuilder.withPayload(changed)
                .setHeader("featureType", "application/kafka-feature-event")
                .build();

        handler.handleMessage(message);

        verify(ds).createSchema(any());
    }

}