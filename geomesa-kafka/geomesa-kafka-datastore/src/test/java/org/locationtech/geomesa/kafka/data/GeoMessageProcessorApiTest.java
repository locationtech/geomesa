/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data;

import org.junit.Test;
import org.locationtech.geomesa.kafka.utils.GeoMessage;
import org.locationtech.geomesa.kafka.utils.interop.GeoMessageProcessor;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class GeoMessageProcessorApiTest {

    public static class TestProcessor implements GeoMessageProcessor {
        public List<GeoMessage.Change> added = new ArrayList<>();
        public List<GeoMessage.Delete> removed = new ArrayList<>();
        public int cleared = 0;
        @Override
        public BatchResult consume(List<GeoMessage> records) {
            records.forEach((r) -> {
                if (r instanceof GeoMessage.Change) {
                    added.add((GeoMessage.Change) r);
                } else if (r instanceof GeoMessage.Delete) {
                    removed.add(((GeoMessage.Delete) r));
                } else if (r instanceof GeoMessage.Clear) {
                    cleared++;
                }
            });
            return BatchResult.COMMIT;
        }
    }

    @Test
    public void testJavaApi() {
        GeoMessageProcessor processor = new TestProcessor();
        KafkaDataStore kds = mock();
        // verify that things compile from java
        // noinspection resource
        kds.createConsumer("type-name", "group-id", processor);
    }
}
