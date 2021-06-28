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
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
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
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
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
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
<<<<<<< HEAD
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
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
<<<<<<< HEAD
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
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
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
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
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
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
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
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
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
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
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
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
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
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
=======
  private val producer = KafkaStore.producer(config.producerConfig)
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  private val producer = KafkaStore.producer(sft, config.producerConfig)
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))

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
