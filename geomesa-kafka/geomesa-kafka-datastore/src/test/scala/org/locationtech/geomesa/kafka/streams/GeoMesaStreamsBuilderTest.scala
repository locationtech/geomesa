/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 133afd3681 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7c0500a81 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0452af77a1 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 2c2075dde8 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 865530eb2c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 67d93e2791 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 39fae2ee18 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2c2075dde8 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 865530eb2c (GEOMESA-3198 Kafka streams integration (#2854))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
import org.apache.kafka.streams.test.TestRecord
=======
import com.typesafe.scalalogging.StrictLogging
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
import org.apache.kafka.streams.test.TestRecord
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
import org.apache.kafka.streams.test.TestRecord
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
import org.apache.kafka.streams.test.TestRecord
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
import org.apache.kafka.streams.test.TestRecord
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.locationtech.geomesa.kafka.KafkaContainerTest
=======
import org.locationtech.geomesa.kafka.EmbeddedKafka
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
import org.locationtech.geomesa.kafka.KafkaContainerTest
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
import org.locationtech.geomesa.kafka.KafkaContainerTest
=======
import org.locationtech.geomesa.kafka.EmbeddedKafka
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
import org.locationtech.geomesa.kafka.KafkaContainerTest
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
import org.locationtech.geomesa.kafka.KafkaContainerTest
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
import org.specs2.mutable.Specification
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
import org.specs2.mutable.Specification
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
import org.specs2.mutable.Specification
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
import org.specs2.mutable.Specification
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Date, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
class GeoMesaStreamsBuilderTest extends KafkaContainerTest {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.scala.serialization.Serdes._
=======
class GeoMesaStreamsBuilderTest extends Specification with StrictLogging {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
class GeoMesaStreamsBuilderTest extends KafkaContainerTest {
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  import org.apache.kafka.streams.scala.serialization.Serdes._
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
class GeoMesaStreamsBuilderTest extends KafkaContainerTest {
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  import org.apache.kafka.streams.scala.serialization.Serdes._
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
class GeoMesaStreamsBuilderTest extends KafkaContainerTest {
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  import org.apache.kafka.streams.scala.serialization.Serdes._
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
  import org.apache.kafka.streams.scala.serialization.Serdes._
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))

  import scala.collection.JavaConverters._

  lazy val sft =
    SimpleFeatureTypes.createImmutableType("streams", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"id$i", s"name$i", i % 2, s"2022-04-27T00:00:0$i.00Z", s"POINT(1 $i)")
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
  var kafka: EmbeddedKafka = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
  private val zkPaths = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  def getParams(zkPath: String): Map[String, String] = {
    require(zkPaths.add(zkPath), s"zk path '$zkPath' is reused between tests, may cause conflicts")
    Map(
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
      "kafka.brokers"            -> brokers,
      "kafka.zookeepers"         -> zookeepers,
=======
      "kafka.brokers"            -> kafka.brokers,
      "kafka.zookeepers"         -> kafka.zookeepers,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
      "kafka.brokers"            -> brokers,
      "kafka.zookeepers"         -> zookeepers,
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
      "kafka.topic.partitions"   -> "1",
      "kafka.topic.replication"  -> "1",
      "kafka.consumer.read-back" -> "Inf",
      "kafka.zk.path"            -> zkPath
    )
  }

  "GeoMesaStreamsBuilder" should {
    "read from geomesa topics" in {
      val params = getParams("word/count")
      // write features to the embedded kafka
      val kryoTopic = WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
      }

      // read off the features from kafka
      val messages: Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
        val props = new Properties()
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consume-kryo-topic")
        val buf = ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
        WithClose(new KafkaConsumer[Array[Byte], Array[Byte]](props)) { consumer =>
          consumer.subscribe(Collections.singleton(kryoTopic))
          val start = System.currentTimeMillis()
          while (buf.lengthCompare(10) < 0 && System.currentTimeMillis() - start < 10000) {
            consumer.poll(Duration.ofMillis(100)).asScala.foreach { record =>
              buf += record
            }
          }
        }
<<<<<<< HEAD
        buf.toSeq
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
        buf
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        buf
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        buf
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
        buf
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
        buf
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
        buf
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
        buf
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
      }

      val timestampExtractor = new TimestampExtractingTransformer()

      val builder = GeoMesaStreamsBuilder(params)

      // pull out the timestamps with an identity transform for later comparison
      val streamFeatures = builder.stream(sft.getTypeName).transform[String, GeoMesaMessage](
        new TransformerSupplier[String, GeoMesaMessage, streams.KeyValue[String, GeoMesaMessage]]() {
          override def get(): Transformer[String, GeoMesaMessage, streams.KeyValue[String, GeoMesaMessage]] =
            timestampExtractor
        })

      // word count example copied from https://kafka.apache.org/31/documentation/streams/developer-guide/dsl-api.html#scala-dsl
      val textLines: KStream[String, String] =
        streamFeatures.map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))
      val wordCounts: KTable[String, Long] =
        textLines
            .flatMapValues(textLine => textLine.split(" +"))
            .groupBy((_, word) => word)
            .count()(Materialized.as("counts-store"))

      wordCounts.toStream.to("word-count")

      val props = new Properties()
<<<<<<< HEAD
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-test")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

      val output = scala.collection.mutable.Map.empty[String, java.lang.Long]
      WithClose(new TopologyTestDriver(builder.build(), props)) { testDriver =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        val inputTopic = testDriver.createInputTopic(kryoTopic, new ByteArraySerializer(), new ByteArraySerializer())
        messages.foreach(m => inputTopic.pipeInput(new TestRecord[Array[Byte],Array[Byte]](m)))
        val outputTopic = testDriver.createOutputTopic("word-count", new StringDeserializer(), new LongDeserializer())
        while (!outputTopic.isEmpty) {
          val rec = outputTopic.readRecord
          output += rec.key() -> rec.value()
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        messages.foreach(testDriver.pipeInput)
        var out = testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer())
        while (out != null) {
          output += out.key() -> out.value()
          out = testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer())
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        }
      }

      val expected =
        features.flatMap(_.getAttributes.asScala.map(_.toString.replaceAll(" ", "_")))
            .groupBy(s => s)
            .map { case (k, v) => k -> v.length.toLong }

      output mustEqual expected

      val expectedTimestamps = features.map(_.getAttribute("dtg").asInstanceOf[Date].getTime)
      foreach(timestampExtractor.timestamps)(_._2 must haveLength(1))
      timestampExtractor.timestamps.map(_._2.head) must containTheSameElementsAs(expectedTimestamps)
    }

    "write to geomesa topics" in {
      val params = getParams("write/test")
      // create the output feature type and topic
      val kryoTopic = WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds.createSchema(sft)
        KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
      }

      val testInput = features.map { sf =>
        val offset = sf.getID.replace("id", "").toLong
        val key = sf.getID.getBytes(StandardCharsets.UTF_8)
        val value = sf.getAttributes.asScala.map(FastConverter.convert(_, classOf[String])).mkString(",")
        new ConsumerRecord("input-topic", 0, offset, key, value.getBytes(StandardCharsets.UTF_8))
      }

      val builder = GeoMesaStreamsBuilder(params)

      val input: KStream[String, String] =
        builder.wrapped.stream[String, String]("input-topic")(implicitly[Consumed[String, String]].withTimestampExtractor(new WallclockTimestampExtractor()))
      val output: KStream[String, GeoMesaMessage] =
        input.mapValues { lines =>
          GeoMesaMessage.upsert(lines.split(","))
        }

      builder.to(sft.getTypeName, output)

      val props = new Properties()
<<<<<<< HEAD
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "write-test")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

      val kryoMessages = ArrayBuffer.empty[ProducerRecord[Array[Byte], Array[Byte]]]
      WithClose(new TopologyTestDriver(builder.build(), props)) { testDriver =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        val inputTopic = testDriver.createInputTopic("input-topic", new ByteArraySerializer(), new ByteArraySerializer())
        testInput.foreach(m => inputTopic.pipeInput(new TestRecord[Array[Byte],Array[Byte]](m)))
        val outputTopic = testDriver.createOutputTopic(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        while (!outputTopic.isEmpty) {
          val rec = outputTopic.readRecord
          kryoMessages += new ProducerRecord(kryoTopic, 0, rec.timestamp, rec.getKey, rec.getValue, rec.getHeaders)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        testInput.foreach(testDriver.pipeInput)
        var out = testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        while (out != null) {
          kryoMessages += out
          out = testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
        }
      }

      WithClose(DataStoreFinder.getDataStore(params.asJava)) { case ds: KafkaDataStore =>
        // initialize kafka consumers for the store
        ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT).close()
        // send the mocked messages to the actual embedded kafka topic
        WithClose(KafkaDataStore.producer(ds.config.brokers, Map.empty)) { producer =>
          kryoMessages.foreach(producer.send)
        }
        eventually(40, 100.millis) {
          val result = SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(features)
        }
      }
    }
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
=======
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
  class TimestampExtractingTransformer
      extends Transformer[String, GeoMesaMessage, org.apache.kafka.streams.KeyValue[String, GeoMesaMessage]]() {

    private var context: ProcessorContext = _

    val timestamps = scala.collection.mutable.Map.empty[String, ArrayBuffer[Long]]

    override def init(context: ProcessorContext): Unit = this.context = context

    override def transform(key: String, value: GeoMesaMessage): streams.KeyValue[String, GeoMesaMessage] = {
      timestamps.getOrElseUpdate(key, ArrayBuffer.empty) += context.timestamp()
      new streams.KeyValue(key, value)
    }

    override def close(): Unit = {}
  }

}
