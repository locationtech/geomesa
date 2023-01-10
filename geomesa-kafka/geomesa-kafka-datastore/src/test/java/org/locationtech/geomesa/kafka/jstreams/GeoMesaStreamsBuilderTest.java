/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.jstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
<<<<<<< HEAD
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
=======
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
<<<<<<< HEAD
import org.apache.kafka.streams.test.TestRecord;
<<<<<<< HEAD
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureReader;
import org.geotools.api.data.SimpleFeatureWriter;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.simple.SimpleFeatureWriter;
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
<<<<<<< HEAD
=======
import org.locationtech.geomesa.kafka.EmbeddedKafka;
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.kafka.data.KafkaDataStore;
import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.geotools.converters.FastConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
<<<<<<< HEAD
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GeoMesaStreamsBuilderTest {

    private static final Logger logger = LoggerFactory.getLogger(GeoMesaStreamsBuilderTest.class);

<<<<<<< HEAD
    static KafkaContainer container = null;
=======
    static EmbeddedKafka kafka = null;
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    static final SimpleFeatureType sft =
          SimpleFeatureTypes.createImmutableType("streams", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326");

    static final List<SimpleFeature> features = new ArrayList<>();

    static final Set<String> zkPaths = Collections.newSetFromMap(new ConcurrentHashMap<>());

<<<<<<< HEAD
    static String zookeepers() {
        return String.format("%s:%s", container.getHost(), container.getMappedPort(KafkaContainer.ZOOKEEPER_PORT));
    }
    static String brokers() {
        return container.getBootstrapServers();
    }

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    public Map<String, String> getParams(String zkPath) {
        if (!zkPaths.add(zkPath)) {
            throw new IllegalArgumentException("zk path '" + zkPath + "' is reused between tests, may cause conflicts");
        }
        Map<String, String> params = new HashMap<>();
<<<<<<< HEAD
        params.put("kafka.brokers", brokers());
        params.put("kafka.zookeepers", zookeepers());
=======
        params.put("kafka.brokers", kafka.brokers());
        params.put("kafka.zookeepers", kafka.zookeepers());
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
        params.put("kafka.topic.partitions", "1");
        params.put("kafka.topic.replication", "1");
        params.put("kafka.consumer.read-back", "Inf");
        params.put("kafka.zk.path", zkPath);
        return params;
    }

    @BeforeClass
    public static void init() {
<<<<<<< HEAD
        DockerImageName image =
              DockerImageName.parse("confluentinc/cp-kafka")
                             .withTag(System.getProperty("confluent.docker.tag", "7.3.1"));
        container = new KafkaContainer(image);
        container.start();
        container.followOutput(new Slf4jLogConsumer(logger));
=======
        logger.info("Starting embedded kafka/zk");
        kafka = new EmbeddedKafka();
        logger.info("Started embedded kafka/zk");
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

        for (int i = 0; i < 10; i ++) {
            ScalaSimpleFeature sf = new ScalaSimpleFeature(sft, "id" + i, null, null);
            sf.setAttribute(0, "name" + i);
            sf.setAttribute(1, i % 2);
            sf.setAttribute(2, "2022-04-27T00:00:0" + i + ".00Z");
            sf.setAttribute(3, "POINT(1 " + i + ")");
            features.add(sf);
        }
    }

    @AfterClass
    public static void destroy() {
<<<<<<< HEAD
        if (container != null) {
            container.stop();
        }
=======
        logger.info("Stopping embedded kafka/zk");
        if (kafka != null) {
            kafka.close();
        }
        logger.info("Stopped embedded kafka/zk");
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    }

    @Test
    public void testRead() throws Exception {
        Map<String, String> params = getParams("word/count");
        String kryoTopic;

        // write features to the embedded kafka
        KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);
        try {
            ds.createSchema(sft);
            try (SimpleFeatureWriter writer = ds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                features.forEach(f -> FeatureUtils.write(writer, f, true));
            }
            kryoTopic = KafkaDataStore.topic(ds.getSchema(sft.getTypeName()));
        } finally {
            ds.dispose();
        }

        Properties consumerProps = new Properties();
<<<<<<< HEAD
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
=======
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers());
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consume-kryo-topic");

        // read off the features from kafka
        List<ConsumerRecord<byte[], byte[]>> messages = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(kryoTopic));
            long start = System.currentTimeMillis();
            while (messages.size() < 10 && System.currentTimeMillis() - start < 10000) {
                consumer.poll(Duration.ofMillis(100)).forEach(messages::add);
            }
        }

        TimestampExtractingTransformer timestampExtractor = new TimestampExtractingTransformer();

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params);

        // pull out the timestamps with an identity transform for later comparison
        KStream<String, GeoMesaMessage> streamFeatures =
              builder.stream(sft.getTypeName()).transform(() -> timestampExtractor);

        // word count example copied from https://kafka.apache.org/31/documentation/streams/developer-guide/dsl-api.html#scala-dsl
        KStream<String, String> textLines = streamFeatures.mapValues(v ->
            v.asJava().stream().map(a -> a.toString().replaceAll(" ", "_")).collect(Collectors.joining(" ")));
        KTable<String, Long> wordCounts = textLines
              .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
              .groupBy((k, word) -> word)
              .count(Materialized.as("counts-store"));
        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "java-word-count-test");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
<<<<<<< HEAD
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);

        Map<String, Long> output;
        try (TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            TestInputTopic<byte[], byte[]> inputTopic = testDriver.createInputTopic(kryoTopic,
                                                                                    new ByteArraySerializer(),
                                                                                    new ByteArraySerializer());
            messages.forEach(m -> inputTopic.pipeInput(new TestRecord<>(m)));
            TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("word-count", new StringDeserializer(), new LongDeserializer());
            output = new HashMap<>(outputTopic.readKeyValuesToMap());
=======
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Map<String, Long> output = new HashMap<>();
        try (TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            messages.forEach(testDriver::pipeInput);
            ProducerRecord<String, Long> out =
                  testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
            while (out != null) {
                output.put(out.key(), out.value());
                out = testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
            }
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
        }

        Map<String, Long> expected = new HashMap<>();
        features.stream().flatMap(f -> f.getAttributes().stream().map(a -> a.toString().replaceAll(" ", "_")))
                .forEach(w -> expected.compute(w, (word, count) -> count == null ? 1L : count + 1));

        Assert.assertEquals(expected, output);
        List<Long> expectedTimestamps =
              features.stream().map(f -> ((Date) f.getAttribute("dtg")).getTime()).collect(Collectors.toList());
        timestampExtractor.timestamps.values().forEach(c -> Assert.assertEquals(1, c.size()));
        List<Long> timestamps =
              timestampExtractor.timestamps.values()
                                           .stream()
                                           .map(c -> c.get(0))
                                           .sorted()
                                           .collect(Collectors.toList());
        Assert.assertEquals(expectedTimestamps, timestamps);
    }

    @Test
    public void testWrite() throws Exception {
        Map<String, String> params = getParams("write/test");
        String kryoTopic;

        // create the output feature type and topic
        KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);
        try {
            ds.createSchema(sft);
            kryoTopic = KafkaDataStore.topic(ds.getSchema(sft.getTypeName()));
        } finally {
            ds.dispose();
        }

        List<ConsumerRecord<byte[], byte[]>> testInput = features.stream().map(sf -> {
            long offset = Long.parseLong(sf.getID().replace("id", ""));
            byte[] key = sf.getID().getBytes(StandardCharsets.UTF_8);
            String value =
                  sf.getAttributes()
                    .stream()
                    .map(a -> FastConverter.convert(a, String.class))
                    .collect(Collectors.joining(","));
            return new ConsumerRecord<>("input-topic", 0, offset, key, value.getBytes(StandardCharsets.UTF_8));
        }).collect(Collectors.toList());

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params);

        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                 Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        KStream<String, GeoMesaMessage> output =
<<<<<<< HEAD
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList((Object[])lines.split(","))));
=======
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

        builder.to(sft.getTypeName(), output);

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "java-write-test");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
<<<<<<< HEAD
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);

        List<ProducerRecord<byte[], byte[]>> kryoMessages = new ArrayList<>();
        try (TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            TestInputTopic<byte[], byte[]> inputTopic = testDriver.createInputTopic("input-topic",
                                                                                    new ByteArraySerializer(),
                                                                                    new ByteArraySerializer());
            testInput.forEach(m -> inputTopic.pipeInput(new TestRecord<>(m)));
            TestOutputTopic<byte[], byte[]> outputTopic = testDriver.createOutputTopic(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            while (!outputTopic.isEmpty()) {
                TestRecord<byte[], byte[]> rec = outputTopic.readRecord();
                kryoMessages.add(new ProducerRecord<>(kryoTopic, 0, rec.timestamp(), rec.getKey(), rec.getValue(), rec.getHeaders()));
=======
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        List<ProducerRecord<byte[], byte[]>> kryoMessages = new ArrayList<>();
        try (TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            testInput.forEach(testDriver::pipeInput);
            ProducerRecord<byte[], byte[]> out =
                  testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            while (out != null) {
                kryoMessages.add(out);
                out = testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
            }
        }

        List<SimpleFeature> result = new ArrayList<>();
        ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);
        try {
            // initialize kafka consumers for the store
            ds.getFeatureReader(new Query(sft.getTypeName()), Transaction.AUTO_COMMIT).close();
            // send the mocked messages to the actual embedded kafka topic
            try(Producer<byte[], byte[]> producer = KafkaDataStore.producer(ds.config())) {
                kryoMessages.forEach(producer::send);
            }

            long end = System.currentTimeMillis() + 4000;
            while (System.currentTimeMillis() < end) {
                try (SimpleFeatureReader reader = ds.getFeatureReader(new Query(sft.getTypeName()), Transaction.AUTO_COMMIT)) {
                    while (reader.hasNext()) {
                        result.add(reader.next());
                    }
                }
                result.sort(Comparator.comparing(SimpleFeature::getID));
                if (result.equals(features)) {
                    break;
                } else {
                    result.clear();
                    Thread.sleep(100);
                }
            }
        } finally {
            ds.dispose();
        }
        Assert.assertEquals(features, result);
    }

    static class TimestampExtractingTransformer
          implements Transformer<String, GeoMesaMessage, KeyValue<String, GeoMesaMessage>> {

        private ProcessorContext context = null;

        Map<String, List<Long>> timestamps = new HashMap<>();

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, GeoMesaMessage> transform(String key, GeoMesaMessage value) {
            timestamps.computeIfAbsent(key, k -> new ArrayList<>()).add(context.timestamp());
            return new KeyValue<>(key, value);
        }

        @Override
        public void close() {

        }
    }
}
