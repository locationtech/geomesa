/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka21

import java.io.{Closeable, StringReader}
import java.nio.charset.StandardCharsets
import java.util.{Properties, UUID}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.commons.codec.binary.Hex
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, Producer}
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.geotools.data.Query
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.kafka21.KafkaDataStore.{ConsumerConfig, ProducerConfig}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

class KafkaDataStore(
    brokers: String,
    zookeepers: String,
    zkPath: String,
    partitions: Int,
    replication: Int,
    config: Either[ConsumerConfig, ProducerConfig]
  ) extends ContentDataStore with LazyLogging {

  import scala.collection.JavaConverters._

  private val client = CuratorFrameworkFactory.builder()
      .namespace(if (zkPath.startsWith("/")) zkPath.substring(1) else zkPath)
      .connectString(zookeepers)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
  client.start()

  private val schemaCache: LoadingCache[String, SimpleFeatureType] =
    Caffeine.newBuilder().build(
      new CacheLoader[String, SimpleFeatureType] {
        override def load(typeName: String): SimpleFeatureType = {
          val schemaPath = KafkaDataStore.getSchemaPath(typeName)
          if (client.checkExists().forPath(schemaPath) == null) { null } else {
            val schema = client.getData.forPath(schemaPath)
            val sft = SimpleFeatureTypes.createType(typeName, new String(schema, StandardCharsets.UTF_8))
            val topic = client.getData.forPath(KafkaDataStore.getTopicPath(typeName))
            KafkaDataStoreHelper.insertTopic(sft, new String(topic, StandardCharsets.UTF_8))
            sft
          }
        }
      }
    )

  private val featureSourceCache: LoadingCache[ContentEntry, ContentFeatureSource with Closeable] =
    Caffeine.newBuilder().build[ContentEntry, ContentFeatureSource with Closeable](
      new CacheLoader[ContentEntry, ContentFeatureSource with Closeable] {
        override def load(key: ContentEntry): ContentFeatureSource with Closeable = {
          val sft = schemaCache.get(key.getTypeName)
          val topic = KafkaDataStoreHelper.extractTopic(sft).getOrElse {
            throw new IllegalStateException(s"No topic set in the user data for type '${sft.getTypeName}'")
          }
          config match {
            case Left(c) =>
              val consumers = KafkaDataStore.consumers(brokers, topic, 1, c.properties)
              new KafkaConsumerFeatureSource(key, sft, topic, consumers, c.expirationPeriod,
                c.consistencyCheck, c.cleanUpCache, c.useCQCache, c.cacheCleanUpPeriod, c.monitor)

            case Right(c) =>
              val producer = KafkaDataStore.producer(brokers, c.properties)
              new KafkaProducerFeatureStore(key, sft, topic, producer)
          }
        }
      })

  // ensure that we stop consuming on shutdown
  sys.addShutdownHook(dispose())

  override def createTypeNames(): java.util.List[Name] = {
    val result = new java.util.ArrayList[Name]()
    if (client.checkExists().forPath("/") != null) {
      val all = client.getChildren.forPath("/").iterator
      while (all.hasNext) {
        val path = all.next()
        if (client.checkExists().forPath(KafkaDataStore.getTopicPath(path)) != null) {
          result.add(new NameImpl(getNamespaceURI, path))
        }
      }
    }
    result
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    // inspect and update the simple feature type for various components
    // do this before anything else so that any modifications will be in place
    GeoMesaSchemaValidator.validate(sft)

    val topic = KafkaDataStoreHelper.extractTopic(sft).getOrElse {
      throw new IllegalArgumentException(s"No topic set in the user data for type '${sft.getTypeName}'")
    }
    val typeName = sft.getTypeName

    // build the schema node
    val schemaPath = KafkaDataStore.getSchemaPath(typeName)
    if (client.checkExists().forPath(schemaPath) != null) {
      throw new IllegalArgumentException(s"Type '$typeName' already exists at zookeeper path '$zkPath'")
    }
    val data = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    client.create().creatingParentsIfNeeded().forPath(schemaPath)
    client.setData().forPath(schemaPath, data.getBytes(StandardCharsets.UTF_8))
    val topicPath = KafkaDataStore.getTopicPath(typeName)
    client.create().creatingParentsIfNeeded().forPath(topicPath)
    client.setData().forPath(topicPath, topic.getBytes(StandardCharsets.UTF_8))

    // create the Kafka topic
    KafkaDataStore.withZk(zookeepers) { zk =>
      if (AdminUtils.topicExists(zk, topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        AdminUtilsVersions.createTopic(zk, topic, partitions, replication)
      }
    }

    // put it in the cache
    schemaCache.put(typeName, sft)
  }

  // for some reason the local name method is final and not implemented, but we can override this one?
  override def updateSchema(name: Name, sft: SimpleFeatureType): Unit = {
    // get previous schema and user data
    val typeName = name.getLocalPart
    val previousSft = getSchema(typeName)

    if (previousSft == null) {
      throw new IllegalArgumentException(s"No schema found for given type name $typeName")
    }

    // check that unmodifiable user data has not changed
    val unmodifiableUserdataKeys = Set(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY)

    unmodifiableUserdataKeys.foreach { key =>
      if (sft.getUserData.keySet().contains(key) && sft.getUserData.get(key) != previousSft.getUserData.get(key)) {
        throw new UnsupportedOperationException(s"Updating $key is not allowed")
      }
    }

    // check that the rest of the schema has not changed (columns, types, etc)
    val previousColumns = previousSft.getAttributeDescriptors
    val currentColumns = sft.getAttributeDescriptors
    if (previousColumns != currentColumns) {
      throw new UnsupportedOperationException("Updating schema columns is not allowed")
    }

    // if all is well, write to zookeeper metadata
    val data = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    client.setData().forPath(KafkaDataStore.getSchemaPath(typeName), data.getBytes(StandardCharsets.UTF_8))
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    // grab topic name before deleting the metadata
    val topic = KafkaDataStoreHelper.extractTopic(schemaCache.get(typeName)).getOrElse {
      throw new IllegalStateException(s"No topic set in the user data for type '$typeName'")
    }

    // delete the schema in zookeeper
    val path = KafkaDataStore.getSchemaPath(typeName)
    if (client.checkExists().forPath(path) != null) {
      client.delete().deletingChildrenIfNeeded().forPath(path)
    }

    // delete the topic in kafka
    KafkaDataStore.withZk(zookeepers) { zk =>
      if (AdminUtils.topicExists(zk, topic)) {
        AdminUtils.deleteTopic(zk, topic)
      } else {
        logger.warn(s"Topic [$topic] does not exist, can't delete it")
      }
    }

    // clean up cache
    schemaCache.invalidate(typeName)
  }

  override protected def createFeatureSource(entry: ContentEntry): ContentFeatureSource =
    featureSourceCache.get(entry)

  override def dispose(): Unit = {
    super.dispose()
    featureSourceCache.asMap().values.asScala.foreach(_.close())
    featureSourceCache.asMap().clear()
    client.close()
  }
}

object KafkaDataStore extends LazyLogging {

  def getSchemaPath(typeName: String): String = s"/$typeName"
  def getTopicPath(typeName: String): String = s"/$typeName/Topic"

  def parseConfig(param: Option[String]): Properties = {
    val props = new Properties
    param.foreach(p => props.load(new StringReader(p)))
    props
  }

  def producer(brokers: String, properties: Properties): Producer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig._

    val props = new Properties()
    // set some defaults but allow them to be overridden
    props.put(ACKS_CONFIG, "1") // mix of reliability and performance
    props.put(RETRIES_CONFIG, Int.box(3))
    props.put(LINGER_MS_CONFIG, Int.box(3)) // helps improve batching at the expense of slight delays in write
    props.put(PARTITIONER_CLASS_CONFIG, classOf[GeoMessagePartitioner].getName)
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.putAll(properties)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def consumer(brokers: String, group: String, properties: Properties): Consumer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._

    val props = new Properties()
    props.put(GROUP_ID_CONFIG, group)
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.putAll(properties)
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  // creates a consumer and sets to the latest offsets
  private def consumers(
      brokers: String,
      topic: String,
      count: Int,
      properties: Properties): Seq[Consumer[Array[Byte], Array[Byte]]] = {
    require(count > 0, "Number of consumers must be greater than 0")

    val group = UUID.randomUUID().toString

    logger.debug(s"Creating $count consumers for topic [$topic] with group-id [$group]")

    Seq.fill(count) {
      val consumer = KafkaDataStore.consumer(brokers, group, properties)
      val listener = new NoOpConsumerRebalanceListener()
      KafkaConsumerVersions.subscribe(consumer, topic, listener)
      consumer
    }
  }

  def withZk[T](zookeepers: String)(fn: ZkUtils => T): T = {
    val security = SystemProperty("geomesa.zookeeper.security.enabled").option.exists(_.toBoolean)
    val zkUtils = ZkUtils(zookeepers, 3000, 3000, security)
    try { fn(zkUtils) } finally {
      zkUtils.close()
    }
  }

  case class ProducerConfig(properties: Properties)
  case class ConsumerConfig(properties: Properties, expirationPeriod: Option[Long], consistencyCheck: Option[Long], cleanUpCache: Boolean, cacheCleanUpPeriod: Long, useCQCache: Boolean, monitor: Boolean)

  case class FeatureSourceCacheKey(entry: ContentEntry, query: Query)

  /**
    * Ensures that updates to a given feature go to the same partition, so that they maintain order
    */
  class GeoMessagePartitioner extends Partitioner {

    override def partition(topic: String,
        key: Any,
        keyBytes: Array[Byte],
        value: Any,
        valueBytes: Array[Byte],
        cluster: Cluster): Int = {
      val count = cluster.partitionsForTopic(topic).size

      try {
        // use the feature id if available, otherwise (for clear) use random shard
        if (keyBytes.length > 0) {
          Math.abs(MurmurHash3.bytesHash(keyBytes)) % count
        } else {
          Random.nextInt(count)
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Unexpected message format: ${Option(keyBytes).map(Hex.encodeHexString).getOrElse("")} " +
                s"${Option(valueBytes).map(Hex.encodeHexString).getOrElse("")}", e)
      }

    }

    override def configure(configs: java.util.Map[String, _]): Unit = {}

    override def close(): Unit = {}
  }
}

