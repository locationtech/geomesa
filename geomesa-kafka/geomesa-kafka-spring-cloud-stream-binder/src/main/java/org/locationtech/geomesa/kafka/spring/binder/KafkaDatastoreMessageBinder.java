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
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

public class KafkaDatastoreMessageBinder extends AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, KafkaDatastoreBinderProvisioner> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDatastoreMessageBinder.class);

    private final Supplier<DataStore> dsFactory;
    private final DataStore ds;
    FeatureWriter<SimpleFeatureType, SimpleFeature> writer;
    FilterFactory ff;

    public KafkaDatastoreMessageBinder(
            String[] headersToEmbed,
            KafkaDatastoreBinderProvisioner provisioningProvider,
            Supplier<DataStore> dsFactory
    ) {
        super(headersToEmbed, provisioningProvider);
        this.dsFactory = dsFactory;
        this.ds = dsFactory.get();
        ff = CommonFactoryFinder.getFilterFactory();
    }

    // Maybe handle a Collection<SimpleFeature> and a Collection<KafkaFeatureEvent>
    @Override
    protected MessageHandler createProducerMessageHandler(
            final ProducerDestination destination,
            final ProducerProperties producerProperties,
            final MessageChannel errorChannel) {
        return message -> {
            String sfName = destination.getName();
            SimpleFeature payload;

            try {
                var sft = SimpleFeatureTypeLoader.sftForName(sfName);
                if (sft.isDefined()) {
                    ds.createSchema(sft.get());
                } else {
                    try {
                        ds.getSchema(sfName);
                        logger.debug("There is no local schema for {}, but we found it in the kds", sfName);
                    } catch (IOException e) {
                        logger.error("There is no sft schema {} in the kds {} or locally", sfName, ds.getInfo().getDescription(), e);
                    }
                }

                if (message.getPayload() instanceof SimpleFeature) {
                    payload = (SimpleFeature) message.getPayload();
                } else if (message.getPayload() instanceof KafkaFeatureEvent.KafkaFeatureChanged) {
                    payload = ((KafkaFeatureEvent.KafkaFeatureChanged) message.getPayload()).feature();
                } else if (message.getPayload() instanceof KafkaFeatureEvent.KafkaFeatureRemoved) {
                    var remove = (KafkaFeatureEvent.KafkaFeatureRemoved) message.getPayload();
                    SimpleFeatureStore featureStore = (SimpleFeatureStore) ds.getFeatureSource(sfName);
                    featureStore.removeFeatures(ff.id(Set.of(ff.featureId(remove.id()))));
                    return;
                } else if (message.getPayload() instanceof KafkaFeatureEvent.KafkaFeatureCleared) {
                    SimpleFeatureStore featureStore = (SimpleFeatureStore) ds.getFeatureSource(sfName);
                    featureStore.removeFeatures(Filter.INCLUDE);
                    return;
                } else {
                    logger.warn("Could not process message with header {} and payload {}", message.getHeaders(), message.getPayload());
                    return;
                }

                if (writer == null) {
                    writer = ds.getFeatureWriterAppend(sfName, Transaction.AUTO_COMMIT);
                }

                FeatureUtils.write(writer, payload, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    protected MessageProducer createConsumerEndpoint(
            final ConsumerDestination destination,
            final String group,
            final ConsumerProperties properties) {
        return new KafkaDatastoreMessageProducer(destination, dsFactory);
    }
}
