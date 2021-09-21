/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.data.KafkaDataStore.ExpiryTimeConfig
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer
<<<<<<< HEAD
import org.locationtech.geomesa.kafka.versions.{KafkaConsumerVersions, RecordVersions}
<<<<<<< HEAD
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
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {
  def cache: KafkaFeatureCache
}

object KafkaCacheLoader extends LazyLogging {

  object LoaderStatus {
    private val count = new AtomicInteger(0)
    private val firstLoadStartTime = new AtomicLong(0L)

    def startLoad(): Boolean = synchronized {
      count.incrementAndGet()
      firstLoadStartTime.compareAndSet(0L, System.currentTimeMillis())
    }
    def completedLoad(): Unit = synchronized {
      if (count.decrementAndGet() == 0) {
        logger.info(s"Last active initial load completed.  " +
          s"Initial loads took ${System.currentTimeMillis()-firstLoadStartTime.get} milliseconds.")
        firstLoadStartTime.set(0L)
      }
    }

    def allLoaded(): Boolean = count.get() == 0
  }

  object NoOpLoader extends KafkaCacheLoader {
    override val cache: KafkaFeatureCache = KafkaFeatureCache.empty()
    override def close(): Unit = {}
  }

  class KafkaCacheLoaderImpl(
      sft: SimpleFeatureType,
      override val cache: KafkaFeatureCache,
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      topic: String,
      frequency: Long,
      serializer: GeoMessageSerializer,
      doInitialLoad: Boolean,
      initialLoadConfig: ExpiryTimeConfig
    ) extends ThreadedConsumer(consumers, Duration.ofMillis(frequency)) with KafkaCacheLoader {

    try { classOf[ConsumerRecord[Any, Any]].getMethod("timestamp") } catch {
      case _: NoSuchMethodException => logger.warn("This version of Kafka doesn't support timestamps, using system time")
    }

    private val initialLoader = if (doInitialLoad) {
      // for the initial load, don't bother spatially indexing until we have the final state
      val loader = new InitialLoader(sft, consumers, topic, frequency, serializer, initialLoadConfig, this)
      CachedThreadPool.execute(loader)
      Some(loader)
    } else {
      startConsumers()
      None
    }

    override def close(): Unit = {
      try {
        super.close()
      } finally {
        CloseWithLogging(initialLoader)
        cache.close()
      }
    }

    override protected [KafkaCacheLoader] def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      val headers = RecordVersions.getHeaders(record)
      val timestamp = RecordVersions.getTimestamp(record)
      val message = serializer.deserialize(record.key(), record.value(), headers, timestamp)
      logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
      message match {
        case m: Change => cache.fireChange(timestamp, m.feature); cache.put(m.feature)
        case m: Delete => cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull); cache.remove(m.id)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
=======
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
        case m => throw new IllegalArgumentException(s"Unknown message: $m")
      }
    }
  }

  /**
    * Handles initial loaded 'from-beginning' without indexing features in the spatial index. Will still
    * trigger message events.
    *
    * @param consumers consumers, won't be closed even on call to 'close()'
    * @param topic kafka topic
    * @param frequency polling frequency in milliseconds
    * @param serializer message serializer
    * @param toLoad main cache loader, used for callback when bulk loading is done
    */
  private class InitialLoader(
      sft: SimpleFeatureType,
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      topic: String,
      frequency: Long,
      serializer: GeoMessageSerializer,
      ordering: ExpiryTimeConfig,
      toLoad: KafkaCacheLoaderImpl
    ) extends ThreadedConsumer(consumers, Duration.ofMillis(frequency), false) with Runnable {

    private val cache = KafkaFeatureCache.nonIndexing(sft, ordering)

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private var latch: CountDownLatch = _
    private val done = new AtomicBoolean(false)

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done.get) { toLoad.consume(record) } else {
        val headers = RecordVersions.getHeaders(record)
        val timestamp = RecordVersions.getTimestamp(record)
        val message = serializer.deserialize(record.key, record.value, headers, timestamp)
        logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
        message match {
          case m: Change => toLoad.cache.fireChange(timestamp, m.feature); cache.put(m.feature)
          case m: Delete => toLoad.cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull); cache.remove(m.id)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
=======
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c6426a3a7a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 757d430ce6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a2836a23ad (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9007cdfc87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 357b5be81d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7765277e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 473d51f7e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7d35db0aaf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6e5007fe4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 3fcf3dfd76 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> eac2c13333 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 186135b4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0d789c064b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5b52f49baf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 335182b81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> da4045f79e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6d1de8ce90 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ff40ff096 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5242a49df1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 63a375da7c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6426a3a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d403547ad5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e89a5797 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 757d430ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2443aee96b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> aadbd60bd5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> a2836a23a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bf93ea67c4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6ecba877e1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 9007cdfc8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ef8e16d0b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 38fbbc89ed (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 357b5be81 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7b82c7bf20 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7fa6b34b36 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 8e2afa1db6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 95f71b643b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ab394fb0f7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 596a21bc37 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7f11c23700 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cba3550cf0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> f6b758c8f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 0c1272d3f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b62f2f9ee6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7ea0bdef54 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 074e01c30e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0701daa5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 2b1f4aa737 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> df0f1d92f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7e51f860b1 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }
        // once we've hit the max offset for the partition, remove from the offset map to indicate we're done
        val maxOffset = offsets.getOrDefault(record.partition, Long.MaxValue)
        if (maxOffset <= record.offset) {
          offsets.remove(record.partition)
          latch.countDown()
          logger.info(s"Initial load: consumed [$topic:${record.partition}:${record.offset}] of $maxOffset, " +
              s"${latch.getCount} partitions remaining")
        } else if (record.offset > 0 && record.offset % 1048576 == 0) { // magic number 2^20
          logger.info(s"Initial load: consumed [$topic:${record.partition}:${record.offset}] of $maxOffset")
        }
      }
    }

    override def run(): Unit = {
      LoaderStatus.startLoad()

      import scala.collection.JavaConverters._

      val partitions = consumers.head.partitionsFor(topic).asScala.map(_.partition)
      try {
        // note: these methods are not available in kafka 0.9, which will cause it to fall back to normal loading
        val beginningOffsets = KafkaConsumerVersions.beginningOffsets(consumers.head, topic, partitions.toSeq)
        val endOffsets = KafkaConsumerVersions.endOffsets(consumers.head, topic, partitions.toSeq)
        partitions.foreach { p =>
          // end offsets are the *next* offset that will be returned, so subtract one to track the last offset
          // we will actually consume
          val endOffset = endOffsets.getOrElse(p, 0L) - 1L
          // note: not sure if start offsets are also off by one, but at the worst we would skip bulk loading
          // for the last message per topic
          val beginningOffset = beginningOffsets.getOrElse(p, 0L)
          if (beginningOffset < endOffset) {
            offsets.put(p, endOffset)
          }
        }
      } catch {
        case e: NoSuchMethodException => logger.warn(s"Can't support initial bulk loading for current Kafka version: $e")
      }
      // don't bother spinning up the consumer threads if we don't need to actually bulk load anything
      if (!offsets.isEmpty) {
        logger.info(s"Starting initial load for [$topic] with ${offsets.size} partitions")
        latch = new CountDownLatch(offsets.size)
        startConsumers() // kick off the asynchronous consumer threads
        try { latch.await() } finally {
          // stop the consumer threads, but won't close the consumers due to `closeConsumers`
          close()
        }
        // set a flag just in case the consumer threads haven't finished spinning down, so that we will
        // pass any additional messages back to the main loader
        done.set(true)
        logger.info(s"Finished initial load, transferring to indexed cache for [$topic]")
        cache.query(Filter.INCLUDE).foreach(toLoad.cache.put)
        logger.info(s"Finished transfer for [$topic]")
      }
      logger.info(s"Starting normal load for [$topic]")
      // start the normal loading
      toLoad.startConsumers()
      LoaderStatus.completedLoad()
    }
  }
}

