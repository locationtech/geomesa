/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "spring.cloud.stream.kafka-datastore")
    public class KafkaDatastoreBinderConfigurationProperties {
    public Map<String, ? extends Serializable> binder = new HashMap<>();

    public Map<String, ? extends Serializable> getBinder() {
        return binder;
    }

    public void setBinder(Map<String, ? extends Serializable> additionalProperties) {
        this.binder = additionalProperties;
    }
}
