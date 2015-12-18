package org.locationtech.geomesa.examples;

import org.apache.commons.cli.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper;
import org.locationtech.geomesa.kafka.ReplayConfig;
import org.locationtech.geomesa.kafka.ReplayTimeHelper;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import java.io.IOException;
import java.util.*;

/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

public class KafkaQuickStart {
    public static final String KAFKA_BROKER_PARAM = "brokers";
    public static final String ZOOKEEPERS_PARAM = "zookeepers";
    public static final String ZK_PATH = "zkPath";

    public static final String[] KAFKA_CONNECTION_PARAMS = new String[] {
            KAFKA_BROKER_PARAM,
            ZOOKEEPERS_PARAM,
            ZK_PATH
    };

    // reads and parse the command line args
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option kafkaBrokers = OptionBuilder.withArgName(KAFKA_BROKER_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                .create(KAFKA_BROKER_PARAM);
        options.addOption(kafkaBrokers);

        Option zookeepers = OptionBuilder.withArgName(ZOOKEEPERS_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka instance, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                .create(ZOOKEEPERS_PARAM);
        options.addOption(zookeepers);

        Option zkPath = OptionBuilder.withArgName(ZK_PATH)
                .hasArg()
                .withDescription("Zookeeper's discoverable path for metadata, defaults to /geomesa/ds/kafka")
                .create(ZK_PATH);
        options.addOption(zkPath);

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<>();
        for (String param : KAFKA_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        return dsConf;
    }

    // add a SimpleFeature to the producer every half second
    public static void addSimpleFeatures(SimpleFeatureType sft, FeatureStore producerFS)
            throws InterruptedException, IOException {
        final int MIN_X = -180;
        final int MAX_X = 180;
        final int MIN_Y = -90;
        final int MAX_Y = 90;
        final int DX = 2;
        final int DY = 1;
        final String[] PEOPLE_NAMES = {"James", "John", "Peter", "Hannah", "Claire", "Gabriel"};
        final long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        final Random random = new Random();
        final DateTime MIN_DATE = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        // creates and updates two SimpleFeatures.
        // the first time this for loop runs the two SimpleFeatures are created.
        // in the subsequent iterations of the for loop, the two SimpleFeatures are updated.
        int numFeatures = (MAX_X - MIN_X) / DX;
        for (int i = 1; i <= numFeatures; i++) {
            builder.add(PEOPLE_NAMES[i % PEOPLE_NAMES.length]); // name
            builder.add((int) Math.round(random.nextDouble()*110)); // age
            builder.add(MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR)).toDate()); // dtg
            builder.add(WKTUtils$.MODULE$.read("POINT(" + (MIN_X + DX * i) + " " + (MIN_Y + DY * i) + ")")); // geom
            SimpleFeature feature1 = builder.buildFeature("1");

            builder.add(PEOPLE_NAMES[(i+1) % PEOPLE_NAMES.length]); // name
            builder.add((int) Math.round(random.nextDouble()*110)); // age
            builder.add(MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR)).toDate()); // dtg
            builder.add(WKTUtils$.MODULE$.read("POINT(" + (MIN_X + DX * i) + " " + (MAX_Y - DY * i) + ")")); // geom
            SimpleFeature feature2 = builder.buildFeature("2");

            // write the SimpleFeatures to Kafka
            featureCollection.add(feature1);
            featureCollection.add(feature2);
            producerFS.addFeatures(featureCollection);
            featureCollection.clear();

            // wait 200 ms in between updating SimpleFeatures to simulate a stream of data
            Thread.sleep(200);
        }
    }

    // prints out attribute values for a SimpleFeature
    public static void printFeature(SimpleFeature f) {
        Iterator<Property> props = f.getProperties().iterator();
        int propCount = f.getAttributeCount();
        System.out.print("fid:" + f.getID());
        for (int i = 0; i < propCount; i++) {
            Name propName = props.next().getName();
            System.out.print(" | " + propName + ":" + f.getAttribute(propName));
        }
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        // read command line args for a connection to Kafka
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse(options, args);

        // create the producer and consumer KafkaDataStore objects
        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
        dsConf.put("isProducer", "true");
        DataStore producerDS = DataStoreFinder.getDataStore(dsConf);
        dsConf.put("isProducer", "false");
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore objects properly
        if (producerDS == null) {
            throw new Exception("Null producer KafkaDataStore");
        }
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        // create the schema which creates a topic in Kafka
        // (only needs to be done once)
        final String sftName = "KafkaQuickStart";
        final String sftSchema = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);
        // set zkPath to default if not specified
        String zkPath = (dsConf.get(ZK_PATH) == null) ? "/geomesa/ds/kafka" : dsConf.get(ZK_PATH);
        SimpleFeatureType preppedOutputSft = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath);
        // only create the schema if it hasn't been created already
        if (!Arrays.asList(producerDS.getTypeNames()).contains(sftName))
            producerDS.createSchema(preppedOutputSft);

        System.out.println("Register KafkaDataStore in GeoServer (Press enter to continue)");
        System.in.read();

        // the live consumer must be created before the producer writes features
        // in order to read streaming data.
        // i.e. the live consumer will only read data written after its instantiation
        SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sftName);
        SimpleFeatureStore producerFS = (SimpleFeatureStore) producerDS.getFeatureSource(sftName);

        // creates and adds SimpleFeatures to the producer every 1/5th of a second
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");
        Instant replayStart = new Instant();
        addSimpleFeatures(sft, producerFS);
        Instant replayEnd = new Instant();

        // read from Kafka after writing all the features.
        // LIVE CONSUMER - will obtain the current state of SimpleFeatures
        System.out.println("\nConsuming with the live consumer...");
        SimpleFeatureCollection featureCollection = consumerFS.getFeatures();
        System.out.println(featureCollection.size() + " features were written to Kafka");

        // the state of the two SimpleFeatures is real time here
        System.out.println("Here are the two SimpleFeatures that were obtained with the live consumer:");
        SimpleFeatureIterator featureIterator = featureCollection.features();
        SimpleFeature feature1 = featureIterator.next();
        SimpleFeature feature2 = featureIterator.next();
        featureIterator.close();
        printFeature(feature1);
        printFeature(feature2);

        // REPLAY CONSUMER - will obtain the state of SimpleFeatures at any specified time
        // Replay consumer requires a ReplayConfig which takes a time range and a
        // duration of time to process
        System.out.println("\nConsuming with the replay consumer...");
        Duration readBehind = new Duration(1000); // 1 second readBehind
        ReplayConfig rc = new ReplayConfig(replayStart, replayEnd, readBehind);
        SimpleFeatureType replaySFT = KafkaDataStoreHelper.createReplaySFT(preppedOutputSft, rc);
        producerDS.createSchema(replaySFT);
        SimpleFeatureSource replayConsumerFS = consumerDS.getFeatureSource(replaySFT.getName());

        // querying for the state of SimpleFeatures approximately 5 seconds before the replayEnd.
        // the ReplayKafkaConsumerFeatureSource will build the state of SimpleFeatures
        // by processing all of the messages that were sent in between queryTime-readBehind and queryTime.
        // only the messages in between replayStart and replayEnd are cached.
        Instant queryTime = replayEnd.minus(5000);
        featureCollection = replayConsumerFS.getFeatures(ReplayTimeHelper.toFilter(queryTime));
        System.out.println(featureCollection.size() + " features were written to Kafka");

        System.out.println("Here are the two SimpleFeatures that were obtained with the replay consumer:");
        featureIterator = featureCollection.features();
        feature1 = featureIterator.next();
        feature2 = featureIterator.next();
        featureIterator.close();
        printFeature(feature1);
        printFeature(feature2);

        System.exit(0);
    }
}
