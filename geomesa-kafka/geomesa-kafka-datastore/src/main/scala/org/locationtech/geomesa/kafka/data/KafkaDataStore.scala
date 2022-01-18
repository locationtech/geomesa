/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.{Closeable, IOException}
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService}
import java.util.{Collections, Properties, UUID}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, Ticker}
import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureStore}
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceConfig
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureReader, MetadataBackedDataStore}
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, RunnableStats}
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.ConsumerErrorHandler
import org.locationtech.geomesa.kafka.data.KafkaCacheLoader.KafkaCacheLoaderImpl
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.locationtech.geomesa.kafka.data.KafkaFeatureWriter.{AppendKafkaFeatureWriter, ModifyKafkaFeatureWriter}
import org.locationtech.geomesa.kafka.index._
import org.locationtech.geomesa.kafka.utils.GeoMessageProcessor
import org.locationtech.geomesa.kafka.utils.GeoMessageProcessor.GeoMessageConsumer
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.{GeoMessagePartitioner, GeoMessageSerializerFactory}
import org.locationtech.geomesa.kafka.{AdminUtilsVersions, KafkaConsumerVersions}
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.TableSharing
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.TableSharingPrefix
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.zk.ZookeeperLocking
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class KafkaDataStore(
    val config: KafkaDataStoreConfig,
    val metadata: GeoMesaMetadata[String],
    serialization: GeoMessageSerializerFactory
  ) extends MetadataBackedDataStore(config) with HasGeoMesaStats with ZookeeperLocking {

  import KafkaDataStore.TopicKey

  import scala.collection.JavaConverters._

  override val stats: GeoMesaStats = new RunnableStats(this)

  // note: sharing a single producer is generally faster
  // http://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html

  private var producerInitialized = false

  // only instantiate the producer if needed
  private lazy val producer = {
    producerInitialized = true
    KafkaDataStore.producer(config)
  }

  private val cleared = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  private val caches = Caffeine.newBuilder().build(new CacheLoader[String, KafkaCacheLoader] {
    override def load(key: String): KafkaCacheLoader = {
      if (config.consumers.count < 1) {
        logger.info("Kafka consumers disabled for this data store instance")
        KafkaCacheLoader.NoOpLoader
      } else {
        val sft = getSchema(key)
        // if the expiry is zero, this will return a NoOpFeatureCache
        val cache = KafkaFeatureCache(sft, config.indices)
        val topic = KafkaDataStore.topic(sft)
        val consumers = KafkaDataStore.consumers(config.brokers, topic, config.consumers)
        val frequency = KafkaDataStore.LoadIntervalProperty.toDuration.get.toMillis
        val serializer = serialization.apply(sft, config.serialization, config.indices.lazyDeserialization)
        val initialLoad = config.consumers.readBack.isDefined
        val expiry = config.indices.expiry
        new KafkaCacheLoaderImpl(sft, cache, consumers, topic, frequency, serializer, initialLoad, expiry)
      }
    }
  })

  private val runner = new KafkaQueryRunner(this, caches)

  // migrate old schemas, if any
  if (!metadata.read("migration", "check").exists(_.toBoolean)) {
    new MetadataMigration(this, config.catalog, config.zookeepers).run()
    metadata.insert("migration", "check", "true")
  }

  /**
    * Start consuming from all topics. Consumers are normally only started for a simple feature type
    * when it is first queried - this will start them immediately.
    */
  def startAllConsumers(): Unit = getTypeNames.foreach(caches.get)

  /**
   * Create a message consumer for the given feature type. This can be used for guaranteed at-least-once
   * message processing
   *
   * @param typeName type name
   * @param groupId consumer group id
   * @param processor message processor
   * @return
   */
  def createConsumer(
      typeName: String,
      groupId: String,
      processor: GeoMessageProcessor,
      errorHandler: Option[ConsumerErrorHandler] = None): Closeable = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IllegalArgumentException(s"Schema '$typeName' does not exist; call `createSchema` first")
    }
    val topic = KafkaDataStore.topic(sft)
    val consumers = {
      // add group id and
      // disable read-back so we don't trigger a re-balance listener that messes with group offset tracking
      val props = config.consumers.properties + (GROUP_ID_CONFIG -> groupId)
      val conf = config.consumers.copy(properties = props, readBack = None)
      KafkaDataStore.consumers(config.brokers, topic, conf)
    }
    val frequency = java.time.Duration.ofMillis(KafkaDataStore.LoadIntervalProperty.toDuration.get.toMillis)
    val serializer = serialization.apply(sft, config.serialization, config.indices.lazyDeserialization)
    val consumer = new GeoMessageConsumer(consumers, frequency, serializer, processor)
    consumer.startConsumers(errorHandler)
    consumer
  }

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = {
    // note: kafka doesn't allow slashes in topic names
    KafkaDataStore.topic(sft) match {
      case null  => KafkaDataStore.setTopic(sft, s"${config.catalog}-${sft.getTypeName}".replaceAll("/", "-"))
      case topic if topic.contains("/") => throw new IllegalArgumentException(s"Topic cannot contain '/': $topic")
      case topic => logger.debug(s"Using user-defined topic [$topic]")
    }
    // remove table sharing as it's not relevant
    sft.getUserData.remove(TableSharing)
    sft.getUserData.remove(TableSharingPrefix)
  }

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    val topic = KafkaDataStore.topic(sft)
    if (topic == null) {
      throw new IllegalArgumentException(s"Topic must be defined in user data under '$TopicKey'")
    } else if (topic != KafkaDataStore.topic(previous)) {
      if (topic.contains("/")) {
        throw new IllegalArgumentException(s"Topic cannot contain '/': $topic")
      }
      onSchemaDeleted(previous)
      onSchemaCreated(sft)
    }
  }

  // create kafka topic
  override protected def onSchemaCreated(sft: SimpleFeatureType): Unit = {
    val topic = KafkaDataStore.topic(sft)
    KafkaDataStore.withZk(config.zookeepers) { zk =>
      if (AdminUtils.topicExists(zk, topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        AdminUtilsVersions.createTopic(zk, topic, config.topics.partitions, config.topics.replication)
      }
    }
  }

  // invalidate any cached consumers in order to reload the new schema
  override protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    Option(caches.getIfPresent(sft.getTypeName)).foreach { cache =>
      cache.close()
      caches.invalidate(sft.getTypeName)
    }
  }

  // stop consumers and delete kafka topic
  override protected def onSchemaDeleted(sft: SimpleFeatureType): Unit = {
    Option(caches.getIfPresent(sft.getTypeName)).foreach { cache =>
      cache.close()
      caches.invalidate(sft.getTypeName)
    }
    val topic = KafkaDataStore.topic(sft)
    KafkaDataStore.withZk(config.zookeepers) { zk =>
      if (AdminUtils.topicExists(zk, topic)) {
        AdminUtils.deleteTopic(zk, topic)
      } else {
        logger.warn(s"Topic [$topic] does not exist, can't delete it")
      }
    }
  }

  /**
    * @see org.geotools.data.DataStore#getFeatureSource(org.opengis.feature.type.Name)
    * @param typeName simple feature type name
    * @return featureStore, suitable for reading and writing
    */
  override def getFeatureSource(typeName: String): SimpleFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    new KafkaFeatureStore(this, sft, runner, caches.get(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    caches.get(query.getTypeName) // kick off the kafka consumers for this sft, if not already started
    GeoMesaFeatureReader(sft, query, runner, None, config.audit)
  }

  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): KafkaFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    val writer = new ModifyKafkaFeatureWriter(sft, producer, config.serialization, filter)
    if (config.clearOnStart && cleared.add(typeName)) {
      writer.clear()
    }
    writer
  }

  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): KafkaFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    val writer = new AppendKafkaFeatureWriter(sft, producer, config.serialization)
    if (config.clearOnStart && cleared.add(typeName)) {
      writer.clear()
    }
    writer
  }

  override def dispose(): Unit = {
    if (producerInitialized) {
      CloseWithLogging(producer)
    }
    CloseWithLogging(caches.asMap.asScala.values)
    caches.invalidateAll()
    super.dispose()
  }

  // zookeeper locking methods
  override protected def zookeepers: String = config.zookeepers
}

object KafkaDataStore extends LazyLogging {

  val TopicKey = "geomesa.kafka.topic"

  val MetadataPath = "metadata"

  val LoadIntervalProperty: SystemProperty = SystemProperty("geomesa.kafka.load.interval", "1s")

  // marker to trigger the cq engine index when using the deprecated enable flag
  private [kafka] val CqIndexFlag: (String, CQIndexType) = null

  def topic(sft: SimpleFeatureType): String = sft.getUserData.get(TopicKey).asInstanceOf[String]

  def setTopic(sft: SimpleFeatureType, topic: String): Unit = sft.getUserData.put(TopicKey, topic)

  def producer(config: KafkaDataStoreConfig): Producer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig._

    val props = new Properties()
    // set some defaults but allow them to be overridden
    props.put(ACKS_CONFIG, "1") // mix of reliability and performance
    props.put(RETRIES_CONFIG, Int.box(3))
    props.put(LINGER_MS_CONFIG, Int.box(3)) // helps improve batching at the expense of slight delays in write
    props.put(PARTITIONER_CLASS_CONFIG, classOf[GeoMessagePartitioner].getName)
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    config.producers.properties.foreach { case (k, v) => props.put(k, v) }
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def consumer(config: KafkaDataStoreConfig, group: String): Consumer[Array[Byte], Array[Byte]] =
    consumer(config.brokers, Map(GROUP_ID_CONFIG -> group) ++ config.consumers.properties)

  def consumer(brokers: String, properties: Map[String, String]): Consumer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._

    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.foreach { case (k, v) => props.put(k, v) }

    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  // creates a consumer and sets to the latest offsets
  private [kafka] def consumers(
      brokers: String,
      topic: String,
      config: ConsumerConfig): Seq[Consumer[Array[Byte], Array[Byte]]] = {
    require(config.count > 0, "Number of consumers must be greater than 0")

    val props = Map(GROUP_ID_CONFIG -> UUID.randomUUID().toString) ++ config.properties
    lazy val partitions = Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean])

    logger.debug(s"Creating ${config.count} consumers for topic [$topic] with group-id [${props(GROUP_ID_CONFIG)}]")

    Seq.fill(config.count) {
      val consumer = KafkaDataStore.consumer(brokers, props)
      val listener = config.readBack match {
        case None    => new NoOpConsumerRebalanceListener()
        case Some(d) => new ReadBackRebalanceListener(consumer, partitions, d)
      }
      KafkaConsumerVersions.subscribe(consumer, topic, listener)
      consumer
    }
  }

  private [kafka] def withZk[T](zookeepers: String)(fn: ZkUtils => T): T = {
    val security = SystemProperty("geomesa.zookeeper.security.enabled").option.exists(_.toBoolean)
    val zkUtils = ZkUtils(zookeepers, 3000, 3000, security)
    try { fn(zkUtils) } finally {
      zkUtils.close()
    }
  }

  /**
    * Rebalance listener that seeks the consumer to the an offset based on a read-back duration
    *
    * @param consumer consumer
    * @param partitions shared partition map, to ensure we only read-back once per partition. For subsequent
    *                   rebalances, we should have committed offsets that will be used
    * @param readBack duration to read back, or Duration.Inf to go to the beginning
    */
  private [kafka] class ReadBackRebalanceListener(consumer: Consumer[Array[Byte], Array[Byte]],
                                                  partitions: java.util.Set[Int],
                                                  readBack: Duration)
      extends ConsumerRebalanceListener with LazyLogging {

    import scala.collection.JavaConverters._

    override def onPartitionsRevoked(topicPartitions: java.util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(topicPartitions: java.util.Collection[TopicPartition]): Unit = {
      topicPartitions.asScala.foreach { tp =>
        if (partitions.add(tp.partition())) {
          KafkaConsumerVersions.pause(consumer, tp)
          try {
            if (readBack.isFinite()) {
              val offset = Try {
                val time = System.currentTimeMillis() - readBack.toMillis
                KafkaConsumerVersions.offsetsForTimes(consumer, tp.topic, Seq(tp.partition), time).get(tp.partition)
              }
              offset match {
                case Success(Some(o)) =>
                  logger.debug(s"Seeking to offset $o for read-back $readBack on [${tp.topic}:${tp.partition}]")
                  consumer.seek(tp, o)

                case Success(None) =>
                  logger.debug(s"No prior offset found for read-back $readBack on [${tp.topic}:${tp.partition}], " +
                      "reading from head of queue")

                case Failure(e) =>
                  logger.warn(s"Error finding initial offset: [${tp.topic}:${tp.partition}], seeking to beginning", e)
                  KafkaConsumerVersions.seekToBeginning(consumer, tp)
              }
            } else {
              KafkaConsumerVersions.seekToBeginning(consumer, tp)
            }
          } finally {
            KafkaConsumerVersions.resume(consumer, tp)
          }
        }
      }
    }
  }

  case class KafkaDataStoreConfig(
      catalog: String,
      brokers: String,
      zookeepers: String,
      consumers: ConsumerConfig,
      producers: ProducerConfig,
      clearOnStart: Boolean,
      topics: TopicConfig,
      serialization: SerializationType,
      indices: IndexConfig,
      looseBBox: Boolean,
      authProvider: AuthorizationsProvider,
      audit: Option[(AuditWriter, AuditProvider, String)],
      namespace: Option[String]) extends NamespaceConfig

  case class ConsumerConfig(count: Int, properties: Map[String, String], readBack: Option[Duration])

  case class ProducerConfig(properties: Map[String, String])

  case class TopicConfig(partitions: Int, replication: Int)

  case class IndexConfig(
      expiry: ExpiryTimeConfig,
      resolution: IndexResolution,
      ssiTiers: Seq[(Double, Double)],
      cqAttributes: Seq[(String, CQIndexType)],
      lazyDeserialization: Boolean,
      executor: Option[(ScheduledExecutorService, Ticker)]
    )

  case class IndexResolution(x: Int, y: Int)

  sealed trait ExpiryTimeConfig
  case object NeverExpireConfig extends ExpiryTimeConfig
  case object ImmediatelyExpireConfig extends ExpiryTimeConfig
  case class IngestTimeConfig(expiry: Duration) extends ExpiryTimeConfig
  case class EventTimeConfig(expiry: Duration, expression: String, ordered: Boolean) extends ExpiryTimeConfig
  case class FilteredExpiryConfig(expiry: Seq[(String, ExpiryTimeConfig)]) extends ExpiryTimeConfig
}
