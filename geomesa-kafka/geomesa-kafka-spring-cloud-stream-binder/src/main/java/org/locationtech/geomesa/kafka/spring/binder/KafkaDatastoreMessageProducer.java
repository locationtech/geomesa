/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder;


import org.geotools.api.data.DataStore;
import org.geotools.api.data.FeatureEvent;
import org.geotools.api.data.SimpleFeatureStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.util.function.Supplier;

public class KafkaDatastoreMessageProducer extends MessageProducerSupport {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDatastoreMessageProducer.class);

    private final ConsumerDestination destination;
    private final Supplier<DataStore> dsFactory;
    private DataStore ds;

    public KafkaDatastoreMessageProducer(ConsumerDestination destination,
                                         Supplier<DataStore> dsFactory) {
        this.destination = destination;
        this.dsFactory = dsFactory;
    }

    @Override
    public void doStart() {
        SimpleFeatureStore fs = getSimpleFeatureStore();

        fs.addFeatureListener(featureEvent -> {
            Message<FeatureEvent> receivedMessage = MessageBuilder
                    .withPayload(featureEvent)
                    .setHeader("contentType", "application/kafka-feature-event")
                    .build();
            sendMessage(receivedMessage);
        });
    }

    private @NotNull SimpleFeatureStore getSimpleFeatureStore() {
        SimpleFeatureStore fs = null;
        while (fs == null) {
            try {
                this.ds = dsFactory.get();
                fs = (SimpleFeatureStore) ds.getFeatureSource(destination.getName());
            } catch (IOException e) {
                logger.warn("Could not connect to KDS input, waiting for KDS to be created. Error: {}", e.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        logger.info("Successfully connected to the input KDS!");
        return fs;
    }
}
