/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.locationtech.geomesa.kafka.spring.binder.converters.SimpleFeatureConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Configuration
@Import({ PropertyPlaceholderAutoConfiguration.class })
@EnableConfigurationProperties({KafkaDatastoreBinderConfigurationProperties.class})
public class KafkaDatastoreBinderConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDatastoreBinderConfiguration.class);

    @Autowired
    KafkaDatastoreBinderConfigurationProperties kafkaDatastoreBinderConfigurationProperties;

    @Bean
    @ConditionalOnMissingBean
    public Supplier<DataStore> dsFactory() {
        return () -> {
            Map<String, Serializable> inParameters = new HashMap<>();
            kafkaDatastoreBinderConfigurationProperties.getBinder()
                    .forEach((key, value) -> inParameters.put(key.replace('-', '.'), value));
            logger.info("Binder config: {}", kafkaDatastoreBinderConfigurationProperties.getBinder());
            logger.info("Connecting to the KDS with params: {}", inParameters);

            try {
                return DataStoreFinder.getDataStore(inParameters);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }


    @Bean
    @ConditionalOnMissingBean
    public KafkaDatastoreBinderProvisioner kafkaDatastoreBinderProvisioner() {
        return new KafkaDatastoreBinderProvisioner();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaDatastoreMessageBinder kafkaDatastoreMessageBinder(KafkaDatastoreBinderProvisioner kafkaDatastoreBinderProvisioner) {
        return new KafkaDatastoreMessageBinder(null, kafkaDatastoreBinderProvisioner, dsFactory());
    }

    @Bean
    @ConditionalOnMissingBean
    public SimpleFeatureConverter simpleFeatureConverter() {
        return new SimpleFeatureConverter();
    }
}
