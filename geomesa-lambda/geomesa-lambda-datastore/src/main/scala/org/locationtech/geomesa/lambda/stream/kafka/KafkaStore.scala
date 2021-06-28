/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.geotools.api.data.{DataStore, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.kafka.versions.KafkaConsumerVersions
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.MessageTypes
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Flushable
import java.time.Clock
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

class KafkaStore(
    ds: DataStore,
    val sft: SimpleFeatureType,
    authProvider: Option[AuthorizationsProvider],
    config: LambdaConfig)
   (implicit clock: Clock = Clock.systemUTC()
   ) extends TransientStore with Flushable with LazyLogging {

  private val offsetManager = config.offsetManager

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e665b32ac (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 0b090a0ead (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1765bd4159 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40b1336067 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01d9dc90f5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 025b2c463d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e665b32ac (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 0b090a0ead (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1765bd4159 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e665b32ac (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d44f185056 (d)
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 01d9dc90f5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 01d9dc90f5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 025b2c463d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01d9dc90f5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 025b2c463d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
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
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 025b2c463d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1765bd4159 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0b090a0ead (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 40b1336067 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d44f185056 (d)
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248c52e680 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eb32220114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dc95d3471f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bb244340b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42732efad0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 01d9dc90f5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7f51bd24aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b89f7a2cb1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2a5f00439c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 24f8f6bc91 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6ecb4a7ef8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ddc7f6b21 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f8bce2fadb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6ddebb98a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ecb143206f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa2c62d3cc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ef8f7d3350 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dcdd947591 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 484b7019a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f6b5810362 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc25db7661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 001cdaa0eb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3f711e2dab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4f53faea4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9c1d2fa971 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 025b2c463d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 38c8256d75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98f9a2abf2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9347dd770 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1ce37ee0cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 27c591d4ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f1600d17fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8de258c229 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6c49160762 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3063a3e5b5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e665b32ac (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 943e0f6c2e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f8d2d1aa34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d1a8e5fdd4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b1f97c466c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7a6a732768 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 70130d003a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1bd6b46b7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 04cdb236b3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4dcfe983fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4c428126e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72f1a8d4a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e616bedea7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9eff957b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 50d73b885b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 37973f6e8a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8027d2776 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1032bd426e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3b00058c46 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9636212b68 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 17eaba2c47 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9233b0b6ee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0b090a0ead (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 90d3a58a5d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a37107118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c06b355749 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92b4bc605a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 36f81c2a93 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad4af01820 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> becde42ca5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0985d7384c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 963850cc06 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6bdbf50f43 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 946ac59980 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 09fad75c88 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5914b050f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cdafa6a242 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 668cd10d4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a26475e089 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5488f03b1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4e4245795 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9e88fdab53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04aa3548a9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2eddc3ef1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 300c3c3f15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 488816d898 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8fcd8f4bc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65a21dfc2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> df8e9e35f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adb7b3537d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d89cfa814a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65397839ec (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eb6404e893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a528ff1ce9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fcbec75dcf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7a9bd1ebef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 614adc0dd8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 457d31635c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dde2dc9857 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 90fb71802f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dfd67099fb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3c65ae5d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be3cb304f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ed451c2cb9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> eb3a84f5e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0b6bcc45cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f7da62d9ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 17c94c4cd7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 934023387e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4203721e52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a891e903bf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f06899d2d6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ab2825de65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c8f7e687c8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 592a72860d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 38df2fe8ca (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 712c22c70c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> caad70c1f6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 965baec7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b48095fd2d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 200af79ae5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 542cda4744 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 731292b1cb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d1baa628e5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 13d76636d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fad4dd0893 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57728378e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 95142aca4e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4491ba2abb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6d9f37fc4f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9eff3cec42 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 482326b3cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad8fde3d37 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f692371537 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b9e86ce8ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1765bd4159 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f1f9dcf7d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03335db95c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ee49947395 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9dca4459a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d6ab9d089 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f0c0cef137 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07adf3186b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> af14a7d63a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 71266af70d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1c9a489451 (GEOMESA-3092 Support Lambda NiFi processor (#2777))

  private val topic = KafkaStore.topic(config.zkNamespace, sft)

  private val cache = new KafkaFeatureCache(topic)

  private val serializer = {
    // use immutable so we can return query results without copying or worrying about user modification
    // use lazy so that we don't create lots of objects that get replaced/updated before actually being read
    val options = SerializationOptions.builder.withUserData.withoutFidHints.immutable.`lazy`.build
    KryoFeatureSerializer(sft, options)
  }

  private val interceptors = QueryInterceptorFactory(ds)

  private val queryRunner = new KafkaQueryRunner(cache, stats, authProvider, interceptors)

  private val loader = {
    val consumers = KafkaStore.consumers(config.consumerConfig, topic, offsetManager, config.consumers, cache.partitionAssigned)
    val frequency = KafkaStore.LoadIntervalProperty.toDuration.get.toMillis
    new KafkaCacheLoader(consumers, topic, frequency, serializer, cache)
  }

  private val persistence = if (config.expiry == Duration.Inf) { None } else {
    Some(new DataStorePersistence(ds, sft, offsetManager, cache, topic, config.expiry.toMillis, config.persist))
  }

  // register as a listener for offset changes
  offsetManager.addOffsetListener(topic, cache)

  override def createSchema(): Unit = {
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }

    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        val replication = SystemProperty("geomesa.kafka.replication").option.map(_.toInt).getOrElse(1)
        val newTopic = new NewTopic(topic, config.partitions, replication.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
      }
    }
  }

  override def removeSchema(): Unit = {
    offsetManager.deleteOffsets(topic)
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }

    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        admin.deleteTopics(Collections.singletonList(topic)).all().get
      } else {
        logger.warn(s"Topic [$topic] does not exist, can't delete it")
      }
    }
  }

  override def read(
      filter: Option[Filter] = None,
      transforms: Option[Array[String]] = None,
      hints: Option[Hints] = None,
      explain: Explainer = new ExplainLogging): QueryResult = {
    val query = new Query()
    filter.foreach(query.setFilter)
    transforms.foreach(query.setPropertyNames(_: _*))
    hints.foreach(query.setHints)
    queryRunner.runQuery(sft, query, explain)
  }

  override def write(original: SimpleFeature): Unit = {
    val feature = GeoMesaFeatureWriter.featureWithFid(original)
    val key = KafkaStore.serializeKey(clock.millis(), MessageTypes.Write)
    producer.send(new ProducerRecord(topic, key, serializer.serialize(feature)))
    logger.trace(s"Wrote feature to [$topic]: $feature")
  }

  override def delete(original: SimpleFeature): Unit = {
    import org.locationtech.geomesa.filter.ff
    // send a message to delete from all transient stores
    val feature = GeoMesaFeatureWriter.featureWithFid(original)
    val key = KafkaStore.serializeKey(clock.millis(), MessageTypes.Delete)
    producer.send(new ProducerRecord(topic, key, serializer.serialize(feature)))
    // also delete from persistent store
    if (config.persist) {
      val filter = ff.id(ff.featureId(feature.getID))
      WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
        while (writer.hasNext) {
          writer.next()
          writer.remove()
        }
      }
    }
  }

  override def persist(): Unit = persistence match {
    case Some(p) => p.run()
    case None => throw new IllegalStateException("Persistence disabled for this store")
  }

  override def flush(): Unit = producer.flush()

  override def close(): Unit = {
    CloseWithLogging(loader)
    CloseWithLogging(interceptors)
    CloseWithLogging(persistence)
    offsetManager.removeOffsetListener(topic, cache)
  }
}

object KafkaStore {

  val SimpleFeatureSpecConfig = "geomesa.sft.spec"

  val LoadIntervalProperty: SystemProperty = SystemProperty("geomesa.lambda.load.interval", "100ms")

  object MessageTypes {
    val Write:  Byte = 0
    val Delete: Byte = 1
  }

  def topic(ns: String, sft: SimpleFeatureType): String = topic(ns, sft.getTypeName)

  def topic(ns: String, typeName: String): String = s"${ns}_$typeName".replaceAll("[^a-zA-Z0-9_\\-]", "_")

  def producer(sft: SimpleFeatureType, connect: Map[String, String]): Producer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig._
    val props = new Properties()
    // set some defaults but allow them to be overridden
    props.put(ACKS_CONFIG, "1") // mix of reliability and performance
    props.put(RETRIES_CONFIG, Int.box(3))
    props.put(LINGER_MS_CONFIG, Int.box(3)) // helps improve batching at the expense of slight delays in write
    props.put(PARTITIONER_CLASS_CONFIG, classOf[FeatureIdPartitioner].getName)
    props.put(SimpleFeatureSpecConfig, SimpleFeatureTypes.encodeType(sft, includeUserData = false))
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def consumer(connect: Map[String, String], group: String): Consumer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._
    val props = new Properties()
    props.put(GROUP_ID_CONFIG, group)
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  // creates a consumer and sets to the latest offsets
  private [kafka] def consumers(connect: Map[String, String],
                                topic: String,
                                manager: OffsetManager,
                                parallelism: Int,
                                callback: (Int, Long) => Unit): Seq[Consumer[Array[Byte], Array[Byte]]] = {
    require(parallelism > 0, "Parallelism must be greater than 0")

    val group = UUID.randomUUID().toString

    Seq.fill(parallelism) {
      val consumer = KafkaStore.consumer(connect, group)
      val listener = new OffsetRebalanceListener(consumer, manager, callback)
      KafkaConsumerVersions.subscribe(consumer, topic, listener)
      consumer
    }
  }

  private [kafka] def serializeKey(time: Long, action: Byte): Array[Byte] = {
    val result = Array.ofDim[Byte](9)

    result(0) = ((time >> 56) & 0xff).asInstanceOf[Byte]
    result(1) = ((time >> 48) & 0xff).asInstanceOf[Byte]
    result(2) = ((time >> 40) & 0xff).asInstanceOf[Byte]
    result(3) = ((time >> 32) & 0xff).asInstanceOf[Byte]
    result(4) = ((time >> 24) & 0xff).asInstanceOf[Byte]
    result(5) = ((time >> 16) & 0xff).asInstanceOf[Byte]
    result(6) = ((time >> 8)  & 0xff).asInstanceOf[Byte]
    result(7) = (time & 0xff        ).asInstanceOf[Byte]
    result(8) = action

    result
  }

  private [kafka] def deserializeKey(key: Array[Byte]): (Long, Byte) = (ByteArrays.readLong(key), key(8))

  private [kafka] class OffsetRebalanceListener(consumer: Consumer[Array[Byte], Array[Byte]],
                                                manager: OffsetManager,
                                                callback: (Int, Long) => Unit)
      extends ConsumerRebalanceListener with LazyLogging {

    override def onPartitionsRevoked(topicPartitions: java.util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(topicPartitions: java.util.Collection[TopicPartition]): Unit = {
      import scala.collection.JavaConverters._

      // ensure we have queues for each partition
      // read our last committed offsets and seek to them
      topicPartitions.asScala.foreach { tp =>

        // seek to earliest existing offset and return the offset
        def seekToBeginning(): Long = {
          KafkaConsumerVersions.seekToBeginning(consumer, tp)
          consumer.position(tp) - 1
        }

        val lastRead = manager.getOffset(tp.topic(), tp.partition())

        KafkaConsumerVersions.pause(consumer, tp)

        val offset = if (lastRead < 0) { seekToBeginning() } else {
          try { consumer.seek(tp, lastRead + 1); lastRead } catch {
            case NonFatal(e) =>
              logger.warn(s"Error seeking to initial offset: [${tp.topic}:${tp.partition}:$lastRead]" +
                  s", seeking to beginning: $e")
              seekToBeginning()
          }
        }
        callback.apply(tp.partition, offset)

        KafkaConsumerVersions.resume(consumer, tp)
      }
    }
  }

  /**
    * Ensures that updates to a given feature go to the same partition, so that they maintain order
    */
  class FeatureIdPartitioner extends Partitioner {

    private var serializer: KryoFeatureSerializer = _

    private val features = new ThreadLocal[KryoBufferSimpleFeature]() {
      override def initialValue(): KryoBufferSimpleFeature = serializer.getReusableFeature
    }

    override def partition(
        topic: String,
        key: scala.Any,
        keyBytes: Array[Byte],
        value: scala.Any,
        valueBytes: Array[Byte],
        cluster: Cluster): Int = {
      val numPartitions = cluster.partitionsForTopic(topic).size
      if (numPartitions < 2) { 0 } else {
        val feature = features.get
        feature.setBuffer(valueBytes)
        Math.abs(MurmurHash3.stringHash(feature.getID)) % numPartitions
      }
    }

    override def configure(configs: java.util.Map[String, _]): Unit = {
      val spec = configs.get(SimpleFeatureSpecConfig) match {
        case s: String => s
        case s => throw new IllegalStateException(s"Invalid spec config for $SimpleFeatureSpecConfig: $s")
      }
      val options = SerializationOptions.builder.immutable.`lazy`.build
      serializer = KryoFeatureSerializer(SimpleFeatureTypes.createType("", spec), options)
    }

    override def close(): Unit = {}
  }
}
