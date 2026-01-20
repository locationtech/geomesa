/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.admin._
import org.geotools.api.data._
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.kafka.KafkaWithZookeeperTest
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreZookeeperTest extends KafkaWithZookeeperTest with Mockito {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  lazy val baseParams = Map(
    "kafka.brokers"            -> brokers,
    "kafka.zookeepers"         -> zookeepers,
    "kafka.topic.partitions"   -> 1,
    "kafka.topic.replication"  -> 1,
    "kafka.consumer.read-back" -> "Inf"
  )

  private val paths = new AtomicInteger(0)

  private val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  def getUniquePath: String = s"geomesa/${paths.getAndIncrement()}/test/"

  def getStore(zkPath: String, consumers: Int, extras: Map[String, AnyRef] = Map.empty): KafkaDataStore = {
    val params = baseParams ++ Map(KafkaDataStoreParams.ZkPath.key -> zkPath, "kafka.consumer.count" -> consumers) ++ extras
    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KafkaDataStore]
  }

  def createStorePair(params: Map[String, AnyRef] = Map.empty): (KafkaDataStore, KafkaDataStore) = {
    val path = getUniquePath
    (getStore(path, 0, params), getStore(path, 1, params))
  }

  "KafkaDataStore" should {

    "return correctly from canProcess" >> {
      val factory = new KafkaDataStoreFactory()
      factory.canProcess(java.util.Map.of("kafka.brokers", "test", "kafka.zookeepers", "test")) must beTrue
    }

    "create unique topics based on zkPath" >> {
      val path = s"geomesa/topics/test/${paths.getAndIncrement()}"
      val ds = getStore(path, 0)
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        ds.getSchema("kafka").getUserData.get(KafkaDataStore.TopicKey) mustEqual s"$path-kafka".replaceAll("/", "-")
      } finally {
        ds.dispose()
      }
    }

    "allow schemas to be created and deleted" >> {
      TableBasedMetadata.Expiry.threadLocalValue.set("10ms")
      val (producer, consumer) = try {
        createStorePair(Map("kafka.zookeepers" -> zookeepers))
      } finally {
        TableBasedMetadata.Expiry.threadLocalValue.remove()
      }
      consumer must not(beNull)
      producer must not(beNull)
      try {
        val sft = SimpleFeatureTypes.createImmutableType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
        val topic = s"${producer.config.catalog}-${sft.getTypeName}".replaceAll("/", "-")
        producer.createSchema(sft)
        consumer.metadata.resetCache()
        foreach(Seq(producer, consumer)) { ds =>
          ds.getTypeNames.toSeq mustEqual Seq(sft.getTypeName)
          val schema = ds.getSchema(sft.getTypeName)
          schema must not(beNull)
          schema mustEqual sft
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

    "support multiple stores creating schemas on the same catalog topic" >> {
      val (producer, consumer) = createStorePair()
      val sft2 = SimpleFeatureTypes.renameSft(sft, "consumer")

      consumer must not(beNull)
      producer must not(beNull)
      try {
        producer.createSchema(sft)
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

    "migrate old kafka data store schemas" >> {
      val spec = "test:String,dtg:Date,*location:Point:srid=4326"

      val path = s"geomesa/migrate/test/${paths.getAndIncrement()}"
      val client = CuratorFrameworkFactory.builder()
          .namespace(path)
          .connectString(zookeepers)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          .build()
      client.start()

      try {
        client.create.forPath("/test", s"$spec;geomesa.index.dtg=dtg".getBytes(StandardCharsets.UTF_8))
        client.create.forPath("/test/Topic", "test-topic".getBytes(StandardCharsets.UTF_8))

        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
        try {
          ds.getTypeNames.toSeq mustEqual Seq("test")
          val sft = ds.getSchema("test")
          sft must not(beNull)
          KafkaDataStore.topic(sft) mustEqual "test-topic"
          SimpleFeatureTypes.encodeType(sft) mustEqual spec

          client.checkExists().forPath("/test") must beNull
        } finally {
          ds.dispose()
        }
      } finally {
        client.close()
      }
    }

    "clean zkPath" >> {
      def getNamespace(path: java.io.Serializable): String =
        KafkaDataStoreFactory.createZkNamespace(Map(KafkaDataStoreParams.ZkPath.getName -> path).asJava)

      // a well-formed path starts does not start or end with a /
      getNamespace("foo/bar/baz") mustEqual "foo/bar/baz"
      getNamespace("foo/bar/baz/") mustEqual "foo/bar/baz" // trailing slash
      getNamespace("/foo/bar/baz") mustEqual "foo/bar/baz" // leading slash
      getNamespace("/foo/bar/baz/") mustEqual "foo/bar/baz" // both leading and trailing slash
      forall(Seq("/", "//", "", null))(n => getNamespace(n) mustEqual KafkaDataStoreFactory.DefaultZkPath) // empty
    }
  }
}
