/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils.interop;

import org.locationtech.geomesa.kafka.consumer.BatchConsumer;
import org.locationtech.geomesa.kafka.utils.GeoMessage;
import scala.Enumeration;

import java.util.List;

/**
 * Message processor class. Guarantees 'at-least-once' processing.
 */
public interface GeoMessageProcessor extends org.locationtech.geomesa.kafka.utils.GeoMessageProcessor {

    /**
     * Consume a batch of records.
     * <p>
     * The response from this method will determine the continued processing of messages. If `Commit`
     * is returned, the batch is considered complete and won't be presented again. If `Continue` is
     * returned, the batch will be presented again in the future, and more messages will be read off the topic
     * in the meantime. If `Pause` is returned, the batch will be presented again in the future, but
     * no more messages will be read off the topic in the meantime.
     * <p>
     * This method should return in a reasonable amount of time. If too much time is spent processing
     * messages, consumers may be considered inactive and be dropped from processing. See
     * <a href="https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html">https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html</a>
     * <p>
     * Note: if there is an error committing the batch or something else goes wrong, some messages may
     * be repeated in a subsequent call, regardless of the response from this method
     *
     * @param records records
     * @return indication to continue, pause, or commit
     */
    BatchResult consume(List<GeoMessage> records);

    // scala 2.12 - note, can't @Override these due to scala version differences
    default Enumeration.Value consume(scala.collection.Seq<GeoMessage> records) {
        List<GeoMessage> list = scala.collection.JavaConverters.seqAsJavaListConverter(records).asJava();
        BatchResult result = consume(list);
        switch(result) {
            case COMMIT:   return BatchConsumer.BatchResult$.MODULE$.Commit();
            case CONTINUE: return BatchConsumer.BatchResult$.MODULE$.Continue();
            case PAUSE:    return BatchConsumer.BatchResult$.MODULE$.Pause();
        }
        return null;
    }

    // scala 2.13 - note, can't @Override these due to scala version differences
    default Enumeration.Value consume(scala.collection.immutable.Seq<GeoMessage> records) {
        List<GeoMessage> list = scala.collection.JavaConverters.seqAsJavaListConverter(records).asJava();
        BatchResult result = consume(list);
        switch(result) {
            case COMMIT:   return BatchConsumer.BatchResult$.MODULE$.Commit();
            case CONTINUE: return BatchConsumer.BatchResult$.MODULE$.Continue();
            case PAUSE:    return BatchConsumer.BatchResult$.MODULE$.Pause();
        }
        return null;
    }

    enum BatchResult {
        COMMIT, CONTINUE, PAUSE
    }
}
