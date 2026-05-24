/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;

public class KafkaDatastoreBinderProvisioner implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

    @Override
    public ProducerDestination provisionProducerDestination(
            final String name,
            final ProducerProperties producerProperties) {
        return new KafkaDatastoreDestination(name);
    }

    @Override
    public ConsumerDestination provisionConsumerDestination(
            final String name,
            final String group,
            final ConsumerProperties consumerProperties) {
        return new KafkaDatastoreDestination(name);
    }

    private class KafkaDatastoreDestination implements ProducerDestination, ConsumerDestination {

        private final String destination;

        private KafkaDatastoreDestination(final String destination) {
            this.destination = destination;
        }

        @Override
        public String getName() {
            return destination;
        }

        @Override
        public String getNameForPartition(int partition) {
            return destination;
        }
    }
}
