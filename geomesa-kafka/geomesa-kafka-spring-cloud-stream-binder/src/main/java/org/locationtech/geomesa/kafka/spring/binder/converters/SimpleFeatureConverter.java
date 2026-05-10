/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.spring.binder.converters;

import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent;
import org.geotools.api.feature.simple.SimpleFeature;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.MimeType;

public class SimpleFeatureConverter extends AbstractMessageConverter {

    public SimpleFeatureConverter() {
        super(new MimeType("application", "kafka-feature-event"));
    }

    @Override
    protected boolean supports(Class<?> aClass) {
        return KafkaFeatureEvent.KafkaFeatureChanged.class.equals(aClass);
    }

    @Override
    protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
        return message.getPayload() instanceof KafkaFeatureEvent.KafkaFeatureChanged;
    }

    @Override
    protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
        KafkaFeatureEvent.KafkaFeatureChanged event = (KafkaFeatureEvent.KafkaFeatureChanged) message.getPayload();
        return event.feature();
    }

    @Override
    protected Object convertToInternal(Object payload, MessageHeaders headers,
                                       Object conversionHint) {
        return payload;
    }

    @Override
    protected MimeType getDefaultContentType(Object toBeConverted) {
        if (toBeConverted instanceof SimpleFeature) {
            return new MimeType("application", "simple-feature");
        } else {
            return super.getDefaultContentType(toBeConverted);
        }
    }
}
