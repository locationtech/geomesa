/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.apache.kafka.clients.admin._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.kafka.ExpirationMocking.{MockTicker, ScheduledExpiry, WrappedRunnable}
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult.BatchResult
import org.locationtech.geomesa.kafka.data.KafkaDataStoreTest.TestAuthsProvider
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.{KafkaFeatureChanged, KafkaFeatureCleared, KafkaFeatureRemoved}
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageProcessor}
import org.locationtech.geomesa.memory.index.impl.SizeSeparatedBucketIndex
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.mockito.ArgumentMatchers
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import java.net.{ServerSocket, URL}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CopyOnWriteArrayList, ScheduledExecutorService, SynchronousQueue, TimeUnit}
import java.util.{Collections, Date, Properties}
import scala.io.{Codec, Source}

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends KafkaContainerTest with Mockito {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  lazy val baseParams = Map(
    // "kafka.serialization.type" -> "avro",
    "kafka.brokers"            -> brokers,
    "kafka.topic.partitions"   -> 1,
    "kafka.topic.replication"  -> 1,
    "kafka.consumer.read-back" -> "Inf"
  )

  private val catalogs = new AtomicInteger(0)

  private val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  private val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
  private val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
  private val f2 = ScalaSimpleFeature.create(sft, "do", "doe", 40, "2017-01-03T00:00:00.000Z", "POINT (10 10)")

  def newCatalog: String = s"geomesa-${catalogs.getAndIncrement()}-test"

  def getStore(
      catalog: String = newCatalog,
      consumers: Int = 1,
      extras: Map[String, AnyRef] = Map.empty,
      metadataExpiry: String = null): KafkaDataStore = {
    val params = baseParams ++ Map("kafka.catalog.topic" -> catalog, "kafka.consumer.count" -> consumers) ++ extras
    Option(metadataExpiry).foreach(e => TableBasedMetadata.Expiry.threadLocalValue.set(e))
    try {
      DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KafkaDataStore]
    } finally {
      TableBasedMetadata.Expiry.threadLocalValue.remove()
    }
  }

  def createStorePair(
      params: Map[String, AnyRef] = Map.empty,
      metadataExpiry: String = null,
      sft: SimpleFeatureType = this.sft): (KafkaDataStore, KafkaDataStore) = {
    val catalog = newCatalog
    val producer = getStore(catalog, 0, params, metadataExpiry)
    producer.createSchema(sft)
    (producer, getStore(catalog, 1, params, metadataExpiry))
  }

  "KafkaDataStore" should {

    "return correctly from canProcess" >> {
      val factory = new KafkaDataStoreFactory()
      factory.canProcess(Collections.emptyMap[String, String]) must beFalse
      factory.canProcess(java.util.Map.of("kafka.brokers", "test")) must beTrue
    }

    "handle old read-back params" >> {
      KafkaDataStoreParams.ConsumerReadBack.lookup(java.util.Map.of("autoOffsetReset", "earliest")) mustEqual Duration.Inf
      KafkaDataStoreParams.ConsumerReadBack.lookup(java.util.Map.of("autoOffsetReset", "latest")) must beNull
      KafkaDataStoreParams.ConsumerReadBack.lookup(java.util.Map.of("kafka.consumer.from-beginning", "true")) mustEqual Duration.Inf
      KafkaDataStoreParams.ConsumerReadBack.lookup(java.util.Map.of("kafka.consumer.from-beginning", "false")) must beNull
    }

    "create unique topics based on catalog" >> {
      val path = s"geomesa-catalog-${catalogs.getAndIncrement()}-unique"
      WithClose(getStore(path, 0)) { ds =>
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName).getUserData.get(KafkaDataStore.TopicKey) mustEqual s"$path-${sft.getTypeName}"

      }
    }

    "use default kafka partitioning" >> {
      WithClose(getStore(consumers = 0)) { ds =>
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName).getUserData.get(KafkaDataStore.PartitioningKey) mustEqual KafkaDataStore.PartitioningDefault
        KafkaDataStore.usesDefaultPartitioning(ds.getSchema(sft.getTypeName)) must beTrue
      }
    }

    "use namespaces" >> {
      val catalog = newCatalog
      WithClose(getStore(catalog, 0, Map("namespace" -> "ns0"))) { ds =>
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName).getName.getNamespaceURI mustEqual "ns0"
        ds.getSchema(sft.getTypeName).getName.getLocalPart mustEqual "kafka"
      }
      WithClose(getStore(catalog, 0, Map("namespace" -> "ns1"))) { ds =>
        ds.getSchema(sft.getTypeName).getName.getNamespaceURI mustEqual "ns1"
        ds.getSchema(sft.getTypeName).getName.getLocalPart mustEqual "kafka"
      }
    }

    "allow schemas to be created and deleted" >> {
      val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
      val (producer, consumer) = createStorePair(metadataExpiry = "10ms", sft = sft)
      try {
        val topic = s"${producer.config.catalog}-${sft.getTypeName}"
        foreach(Seq(producer, consumer)) { ds =>
          eventually(40, 100.millis)(ds.getTypeNames.toSeq mustEqual Seq(sft.getTypeName))
          val schema = ds.getSchema(sft.getTypeName)
          schema must not(beNull)
          SimpleFeatureTypes.encodeType(schema) mustEqual SimpleFeatureTypes.encodeType(sft)
          schema.getUserData.get("geomesa.foo") mustEqual "bar"
          schema.getUserData.get(KafkaDataStore.TopicKey) mustEqual topic
        }

        val props = Collections.singletonMap[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        WithClose(AdminClient.create(props)) { admin =>
          admin.listTopics().names().get.asScala must contain(topic)
        }
        consumer.removeSchema(sft.getTypeName)
        foreach(Seq(consumer, producer)) { ds =>
          eventually(40, 100.millis)(ds.getTypeNames.toSeq must beEmpty)
          ds.getSchema(sft.getTypeName) must beNull
        }
        WithClose(AdminClient.create(props)) { admin =>
          eventually(40, 100.millis)(admin.listTopics().names().get.asScala must not(contain(topic)))
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "allow schemas to be truncated on delete" >> {
      WithClose(getStore(extras = Map("kafka.topic.truncate-on-delete" -> "true"), metadataExpiry = "10ms")) { ds=>
        ds.createSchema(sft)
        val topic = ds.getSchema(sft.getTypeName).getUserData.get(KafkaDataStore.TopicKey).asInstanceOf[String]
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        WithClose(AdminClient.create(Collections.singletonMap[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) { admin =>
          admin.listTopics().names().get.asScala must contain(topic)
          val partition = new TopicPartition(topic, 0)
          val startOffset = admin.listOffsets(java.util.Map.of(partition, OffsetSpec.earliest())).partitionResult(partition).get.offset()

          ds.removeSchema(sft.getTypeName)

          // topic should still exist
          admin.listTopics().names().get.asScala must contain(topic)

          // ensure that the latest offset is past the features we deleted
          val earliest = admin.listOffsets(java.util.Map.of(partition, OffsetSpec.earliest())).partitionResult(partition).get.offset()
          earliest must beGreaterThanOrEqualTo(startOffset + 2)
          val latest = admin.listOffsets(java.util.Map.of(partition, OffsetSpec.latest())).partitionResult(partition).get.offset()
          earliest mustEqual latest
        }
      }
    }

    "support multiple stores creating schemas on the same catalog topic" >> {
      val (producer, consumer) = createStorePair()
      val sft2 = SimpleFeatureTypes.renameSft(sft, "consumer")
      try {
        consumer.createSchema(sft2)
        foreach(Seq(producer, consumer)) { ds =>
          ds.metadata.resetCache()
          ds.getTypeNames.toSeq must containAllOf(Seq(sft.getTypeName, sft2.getTypeName))
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/update/read/delete features" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val params = if (cqEngine) { Map("kafka.index.cqengine" -> "geom:default,name:unique") } else { Map.empty[String, String] }
        val (producer, consumer) = createStorePair(params)
        try {
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f0, f1)))

          // update
          val f2 = ScalaSimpleFeature.create(sft, "sm", "smith2", 32, "2017-01-01T00:00:02.000Z", "POINT (2 2)")
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f2, useProvidedFid = true)
          }
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f1, f2)))

          // query
          val queries = Seq(
            "strToUpperCase(name) = 'JONES'",
            "name = 'jones' OR name = 'smith'",
            "name = 'foo' OR name = 'bar' OR name = 'baz' OR name = 'blarg' OR name = 'jones' OR name = 'smith'",
            "name = 'jones'",
            "age < 25",
            "bbox(geom, -15, -15, -5, -5) AND age < 25",
            "bbox(geom, -15, -15, 5, 5) AND dtg DURING 2017-01-01T12:00:00.000Z/2017-01-02T12:00:00.000Z",
            "INTERSECTS(geom, POLYGON((-11 -11, -9 -11, -9 -9, -11 -9, -11 -11))) AND bbox(geom, -15, -15, 5, 5)"
          )

          forall(queries) { ecql =>
            val query = new Query(sft.getTypeName, ECQL.toFilter(ecql))
            val features = CloseableIterator(consumer.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            features mustEqual Seq(f1)
          }

          // delete
          producer.getFeatureSource(sft.getTypeName).removeFeatures(ECQL.toFilter("IN('sm')"))
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList mustEqual Seq(f1))

          // clear
          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must beEmpty)
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "support metrics" >> {
      val port = KafkaDataStoreTest.getFreePort
      val params = Map("geomesa.metrics.registry" -> "prometheus", "geomesa.metrics.registry.config" -> s"port = $port")
      val (producer, consumer) = createStorePair(params)
      try {
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f0, f1)))
        // write a second time so that our "live" metrics get updated, vs we may have hit the initial loader in our first write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        // delete
        producer.getFeatureSource(sft.getTypeName).removeFeatures(ECQL.toFilter("IN('sm')"))
        eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList mustEqual Seq(f1))

        val metrics = WithClose(Source.fromURL(new URL(s"http://localhost:$port/metrics"))(Codec.UTF8))(_.getLines().toList)
        val indexTagsRegex = s"""\\{.*catalog="${producer.config.catalog}".*store="kafka".*type_name="${sft.getTypeName}".*\\}"""
        metrics must contain(beMatching(s"""^geomesa_kafka_index_size$indexTagsRegex 1\\.0$$"""))
        metrics must contain(beMatching(s"""^geomesa_kafka_index_expirations_total$indexTagsRegex 0\\.0$$"""))
        def msgTagsRegex(op: String) = s"""\\{.*catalog="${producer.config.catalog}".*op="$op".*store="kafka".*type_name="${sft.getTypeName}".*\\}"""
        // may have been between 2-4 reads, due to timing around initial loading
        metrics must contain(beMatching(s"""^geomesa_kafka_consumer_consumed_total${msgTagsRegex("update")} [2-4]\\.0$$"""))
        metrics must contain(beMatching(s"""^geomesa_kafka_consumer_consumed_total${msgTagsRegex("delete")} 1\\.0$$"""))
        metrics must contain(beMatching(s"""^geomesa_kafka_consumer_dtg_latest_seconds$indexTagsRegex 1\\.4833152E9$$"""))
        metrics must contain(beMatching(s"""^geomesa_kafka_producer_produced_total${msgTagsRegex("delete")} 1\\.0$$"""))
        metrics must contain(beMatching(s"""^geomesa_kafka_producer_produced_total${msgTagsRegex("update")} 4\\.0$$"""))
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support topic read-back" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val params = if (cqEngine) { Map("kafka.index.cqengine" -> "geom:default,name:unique") } else { Map.empty[String, String] }
        val (producer, consumer) = createStorePair(params ++ Map("kafka.consumer.read-back" -> "Inf"))
        try {
          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f0, f1)))
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "support topic read-back with multiple partitions, some empty" >> {
      val params = Map("kafka.consumer.read-back" -> "2 minutes", "kafka.topic.partitions" -> "2")
      val (producer, consumer) = createStorePair(params)
      try {
        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling
        eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList mustEqual Seq(f0))
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/read with visibilities" >> {
      import org.locationtech.geomesa.security.AuthProviderParam

      foreach(Seq(true, false)) { cqEngine =>
        val provider = new TestAuthsProvider()
        val params = if (cqEngine) { Map("kafka.index.cqengine" -> "geom:default,name:unique") } else { Map.empty[String, String] }
        val (producer, consumer) = createStorePair(params + (AuthProviderParam.key -> provider))
        try {
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          f0.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
          f1.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER&ADMIN")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          val q = new Query(sft.getTypeName)
          q.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)

          // admin user
          provider.auths.addAll(java.util.List.of("USER", "ADMIN"))
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f0, f1)))
          store.getCount(q) mustEqual 2

          // regular user
          provider.auths.remove("ADMIN")
          CloseableIterator(store.getFeatures.features).toList mustEqual Seq(f0)
          store.getCount(q) mustEqual 1

          // unauthorized
          provider.auths.clear()
          CloseableIterator(store.getFeatures.features).toList must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "require visibilities on write" >> {
      val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.vis.required=true")
      val (producer, consumer) = createStorePair(sft = sft)
      try {
        val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true)) must throwAn[IllegalArgumentException]
          f0.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER")
          f1.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER&ADMIN")
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true)) must not(throwAn[Exception]) // ok
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/read json array attributes" >> {
      val sft = SimpleFeatureTypes.createType("kafka", "name:String:json=true,age:Int,dtg:Date,*geom:Point:srid=4326")
      val (producer, consumer) = createStorePair(sft = sft)
      try {
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val f0 = ScalaSimpleFeature.create(sft, "sm", "[\"smith1\",\"smith2\"]", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "[\"jones\"]", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
            containTheSameElementsAs(Seq(f0, f1)))
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/read avro collection attributes" >> {
      val catalog = newCatalog
      val sft =
        SimpleFeatureTypes.createType("kafka", "names:List[String],props:Map[String,String],uuid:UUID,dtg:Date,*geom:Point:srid=4326")
      val features = Seq(
        ScalaSimpleFeature.create(sft, "sm", List("smith1", "smith2"), Map("s" -> "smith"),
          "8e619e92-e894-4553-b65d-ce65681a75f4", "2017-01-01T00:00:00.000Z", "POINT (0 0)"),
        ScalaSimpleFeature.create(sft, "jo", List("jones"), Map("j1" -> "jones1", "j2" -> "jones2"),
          "d6505c88-c5ea-4bb3-99d7-26af5b531eda", "2017-01-02T00:00:00.000Z", "POINT (-10 -10)"),
        ScalaSimpleFeature.create(sft, "wi", List("wilson"), Map("w1" -> "wilson1"),
          "d6505c88-c5ea-4bb3-99d7-26af5b531edb", "2017-01-03T00:00:00.000Z", "POINT (10 10)")
      )
      KafkaDataStoreParams.SerializationTypes.Types must haveLength(features.length)
      KafkaDataStoreParams.SerializationTypes.Types.zip(features).foreach { case (serde, f) =>
        val params = Map(KafkaDataStoreParams.SerializationType.key -> serde)
        WithClose(getStore(catalog, 0, params)) { producer =>
          if (!producer.getTypeNames.contains(sft.getTypeName)) {
            producer.createSchema(sft)
          }
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f, useProvidedFid = true)
          }
        }
      }

      foreach(KafkaDataStoreParams.SerializationTypes.Types) { serde =>
        val params = Map(KafkaDataStoreParams.SerializationType.key -> serde)
        WithClose(getStore(catalog, 1, params)) { consumer =>
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
            containTheSameElementsAs(features))
        }
      }
    }

    "expire entries" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val executor = mock[ScheduledExecutorService]
        val ticker = new MockTicker()
        val params = {
          val base = Map(
            "kafka.cache.expiry" -> "100ms",
            "kafka.cache.executor" -> (executor, ticker),
          )
          if (cqEngine) { base + ("kafka.index.cqengine" -> "geom:default,name:unique") } else { base }
        }
        val (producer, consumer) = createStorePair(params)
        try {
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          val expirations = Collections.synchronizedList(new java.util.ArrayList[WrappedRunnable](2))
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.anyLong(), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          // check the cache directly
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
              containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures(bbox).features).toList must
              containTheSameElementsAs(Seq(f0, f1)))

          // expire the cache
          expirations.asScala.foreach(_.runnable.run())

          // verify feature has expired - hit the cache directly
          CloseableIterator(store.getFeatures.features).toList must beEmpty
          // verify feature has expired - hit the spatial index
          CloseableIterator(store.getFeatures(bbox).features).toList must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "expire entries based on cql filters" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val executor = mock[ScheduledExecutorService]
        val ticker = new MockTicker()
        val params = {
          val expiry =
            """{
               |"name = 'smith'": "100ms",
               |"name = 'jones'": "200ms"
               |}""".stripMargin
          val base = Map(
            "kafka.cache.expiry.dynamic" -> expiry,
            "kafka.cache.expiry"         -> "300ms",
            "kafka.cache.executor"       -> (executor, ticker)
          )
          if (cqEngine) { base + ("kafka.index.cqengine" -> "geom:default,name:unique") } else { base }
        }
        val (producer, consumer) = createStorePair(params)
        try {
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
          val f2 = ScalaSimpleFeature.create(sft, "wi", "wilson", 10, "2017-01-03T00:00:00.000Z", "POINT (10 10)")

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          val expirations = Collections.synchronizedList(new java.util.ArrayList[WrappedRunnable](2))

          // test the first filter expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          // check the cache directly
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
              containTheSameElementsAs(Seq(f0)))
          // check the spatial index
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures(bbox).features).toList must
              containTheSameElementsAs(Seq(f0)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(0).runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // test the second filter expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          // check the cache directly
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
              containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures(bbox).features).toList must
              containTheSameElementsAs(Seq(f0, f1)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(1).runnable), ArgumentMatchers.eq(200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // test the fallback expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(300L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f2).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          // check the cache directly
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must
              containTheSameElementsAs(Seq(f0, f1, f2)))
          // check the spatial index
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures(bbox).features).toList must
              containTheSameElementsAs(Seq(f0, f1, f2)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(2).runnable), ArgumentMatchers.eq(300L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // expire the cache
          expirations.asScala.foreach(_.runnable.run())

          // verify feature has expired - hit the cache directly
          CloseableIterator(store.getFeatures.features).toList must beEmpty
          // verify feature has expired - hit the spatial index
          CloseableIterator(store.getFeatures(bbox).features).toList must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "clear on startup" >> {
      val params = Map("kafka.producer.clear" -> "true")
      val (producer, consumer) = createStorePair(params)
      try {
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList must containTheSameElementsAs(Seq(f0, f1)))

        // new producer - clears on startup
        val producer2 = getStore(producer.config.catalog, 0, params)
        try {
          // write the third feature
          WithClose(producer2.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f2, useProvidedFid = true)
          }
          eventually(40, 100.millis)(CloseableIterator(store.getFeatures.features).toList mustEqual Seq(f2))
        } finally {
          producer2.dispose()
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support listeners" >> {
      val (producer, consumer) = createStorePair()
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support transactions" >> {
      val (producer, consumer) = createStorePair()
      try {
        val features = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }

        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val ids = new CopyOnWriteArrayList[String]()

        val listener = new FeatureListener() {
          override def changed(event: FeatureEvent): Unit = {
            ids.add(event.asInstanceOf[KafkaFeatureChanged].feature.getID)
          }
        }

        store.addFeatureListener(listener)

        try {
          WithClose(new DefaultTransaction()) { transaction =>
            WithClose(producer.getFeatureWriterAppend(sft.getTypeName, transaction)) { writer =>
              features.take(2).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.rollback()
              features.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.commit()
            }
            eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(3)(_.toString))

            WithClose(producer.getFeatureWriterAppend(sft.getTypeName, transaction)) { writer =>
              features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.commit()
            }

            eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(3)(_.toString) ++ Seq.tabulate(10)(_.toString))
          }
        } finally {
          store.removeFeatureListener(listener)
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support layer views" >> {
      val views =
        """{
          |  test = [
          |    { type-name = test2, filter = "dtg > '2018-01-01T05:00:00.000Z'", transform = [ "name", "dtg", "geom" ] }
          |    { type-name = test3, transform = [ "derived=strConcat(name,'-d')", "dtg", "geom" ] }
          |    { type-name = test4, filter = "dtg > '2018-01-01T05:00:00.000Z'" }
          |  ]
          |}
          |
          |""".stripMargin
      val sft = SimpleFeatureTypes.renameSft(this.sft, "test")
      val (producer, consumer) =
        createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views), sft = sft)
      try {
        val sft2 = SimpleFeatureTypes.createType("test2", "name:String,dtg:Date,*geom:Point:srid=4326")
        val sft3 = SimpleFeatureTypes.createType("test3", "derived:String,dtg:Date,*geom:Point:srid=4326")
        val sft4 = SimpleFeatureTypes.createType("test4", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

        val features = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }
        val derived = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft3, s"$i", s"name$i-d", f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }

        consumer.getTypeNames.toSeq must containTheSameElementsAs(Seq("test", "test2", "test3", "test4"))
        SimpleFeatureTypes.encodeType(consumer.getSchema("test2")) mustEqual SimpleFeatureTypes.encodeType(sft2)
        SimpleFeatureTypes.encodeType(consumer.getSchema("test3")) mustEqual SimpleFeatureTypes.encodeType(sft3)
        SimpleFeatureTypes.encodeType(consumer.getSchema("test4")) mustEqual SimpleFeatureTypes.encodeType(sft4)

        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val ids = new CopyOnWriteArrayList[String]()

        val listener = new FeatureListener() {
          override def changed(event: FeatureEvent): Unit = {
            event match {
              case e: KafkaFeatureChanged => ids.add(e.feature.getID)
              case e: KafkaFeatureRemoved => ids.remove(e.id)
              case _: KafkaFeatureCleared => ids.clear()
              case _ => failure(s"Unexpected event: $event")
            }
          }
        }

        store.addFeatureListener(listener)

        try {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(10)(_.toString))
          eventually(40, 100.millis)(CloseableIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features))
          CloseableIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.drop(6).map(ScalaSimpleFeature.retype(sft2, _)))
          CloseableIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(derived)
          CloseableIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.drop(6).map(ScalaSimpleFeature.retype(sft4, _)))

          val toRemove = ECQL.toFilter("IN('0','9')")
          WithClose(producer.getFeatureWriter(sft.getTypeName, toRemove, Transaction.AUTO_COMMIT)) { writer =>
            while(writer.hasNext) {
              writer.next()
              writer.remove()
            }
          }

          eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(10)(_.toString).slice(1, 9))
          eventually(40, 100.millis)(CloseableIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.slice(1, 9)))
          CloseableIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.drop(6).dropRight(1).map(ScalaSimpleFeature.retype(sft2, _)))
          CloseableIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(derived.slice(1, 9))
          CloseableIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.drop(6).dropRight(1).map(ScalaSimpleFeature.retype(sft4, _)))

          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          eventually(40, 100.millis)(ids.asScala must beEmpty)
          eventually(40, 100.millis)(CloseableIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toList must beEmpty)
          CloseableIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toList must beEmpty
          CloseableIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toList must beEmpty
          CloseableIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT)).toList must beEmpty
        } finally {
          store.removeFeatureListener(listener)
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support at-least-once consumers" >> {
      val params = Map(
        KafkaDataStoreParams.ConsumerConfig.key -> "auto.offset.reset=earliest",
        KafkaDataStoreParams.ConsumerCount.key -> "2",
        KafkaDataStoreParams.TopicPartitions.key -> "2"
      )
      val (producer, consumer) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 3
        val maxLon = 80.0

        val seen = new AtomicBoolean(false)
        val results = new CopyOnWriteArrayList[SimpleFeature]().asScala

        val processor = new GeoMessageProcessor() {
          override def consume(records: Seq[GeoMessage]): BatchResult = {
            if (!seen.get) {
              seen.set(true)
              BatchResult.Continue // this should cause the messages to be replayed
            } else {
              results ++= records.collect { case GeoMessage.Change(f) => f }
              BatchResult.Commit
            }
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()

        def writeUpdates(): Unit = {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            (numUpdates to 1 by -1).foreach { i =>
              val ll = maxLon - maxLon / i
              val sf = writer.next()
              sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
              sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"$id-$ll")
              sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              writer.write()
            }
          }
        }

        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          writeUpdates()
          eventually(seen.get must beTrue)
          eventually(results must haveLength(numUpdates))
        }

        // verify that we can read a second batch
        writeUpdates()
        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          eventually(results must haveLength(numUpdates * 2))
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support pausing at-least-once consumers" >> {
      skipped("inconsistent")
      val params = Map(
        KafkaDataStoreParams.ConsumerConfig.key -> "auto.offset.reset=earliest",
        KafkaDataStoreParams.ConsumerCount.key -> "2",
        KafkaDataStoreParams.TopicPartitions.key -> "2"
      )
      val (producer, consumer) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 3
        val maxLon = 80.0

        val in = new SynchronousQueue[Seq[SimpleFeature]]()
        val out = new SynchronousQueue[BatchResult]()

        val processor = new GeoMessageProcessor() {
          override def consume(records: Seq[GeoMessage]): BatchResult = {
            in.offer(records.collect { case GeoMessage.Change(f) => f }, 10, TimeUnit.SECONDS)
            Option(out.poll(10, TimeUnit.SECONDS)).getOrElse(BatchResult.Continue)
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()

        def writeUpdates(): Unit = {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            (numUpdates to 1 by -1).foreach { i =>
              val ll = maxLon - maxLon / i
              val sf = writer.next()
              sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
              sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"$id-$ll")
              sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              writer.write()
            }
          }
        }

        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          out.put(BatchResult.Pause)
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          foreach(0 until 10) { _ =>
            out.put(BatchResult.Pause)
            in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          }
          out.put(BatchResult.Continue)
          eventually {
            val res = in.poll(10, TimeUnit.SECONDS)
            out.put(BatchResult.Continue)
            res must haveLength(numUpdates * 2)
          }
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates * 2)
          out.put(BatchResult.Commit)
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          out.put(BatchResult.Commit)
        }
        ok
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "configure topics by feature type" in {
      val ds = getStore(consumers = 0)
      try {
        val sft = SimpleFeatureTypes.renameSft(this.sft, "test")
        sft.getUserData.put("kafka.topic.config", "cleanup.policy=compact\nretention.ms=86400000")
        ds.createSchema(sft)
        val topic = KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
        val props = new Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

        WithClose(AdminClient.create(props)) { admin =>
          val configs =
            admin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
          val config = configs.values().get(new ConfigResource(ConfigResource.Type.TOPIC, topic)).get()
          config must not(beNull)
          config.entries().asScala.map(e => e.name() -> e.value()).toMap must
              containAllOf(Seq("cleanup.policy" -> "compact", "retention.ms" -> "86400000"))
        }
      } finally {
        ds.dispose()
      }
    }

    "update compaction policy for catalog topics if not set" in {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      val catalog = newCatalog
      // create the topic
      WithClose(AdminClient.create(props)) { admin =>
        val newTopic = new NewTopic(catalog, 1, 1.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
      }
      val ds = getStore(catalog, 0)
      try {
        ds.getTypeNames()
        // verify the compaction policy
        WithClose(AdminClient.create(props)) { admin =>
          val configs =
            admin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, catalog)))
          val config = configs.values().get(new ConfigResource(ConfigResource.Type.TOPIC, catalog)).get()
          config must not(beNull)
          config.entries().asScala.map(e => e.name() -> e.value()).toMap must
            containAllOf(Seq("cleanup.policy" -> "compact"))
        }
      } finally {
        ds.dispose()
      }
    }

    "Parse SSI tiers" >> {
      val key = KafkaDataStoreParams.IndexTiers.getName
      KafkaDataStoreFactory.parseSsiTiers(Collections.emptyMap()) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "foo")) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2")) mustEqual Seq((1d, 2d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2,3:4")) mustEqual Seq((1d, 2d), (3d, 4d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "3:4,1:2")) mustEqual Seq((1d, 2d), (3d, 4d))
    }

    "handle Query instances with null TypeName (GeoServer querylayer extension implementation nuance)" >> {
      val (producer, consumer) = createStorePair()
      try {
        val fs = consumer.getFeatureSource(sft.getTypeName)
        val q = new Query(null, Filter.INCLUDE)
        fs.getFeatures(q).features().close() must not(throwA[NullPointerException])
      } finally {
        producer.dispose()
        consumer.dispose()
      }
    }
  }
}

object KafkaDataStoreTest {

  class TestAuthsProvider extends AuthorizationsProvider {
    val auths: java.util.List[String] = new java.util.ArrayList[String]()
    override def getAuthorizations: java.util.List[String] = auths
    override def configure(params: java.util.Map[String, _]): Unit = {}
  }

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }
}
