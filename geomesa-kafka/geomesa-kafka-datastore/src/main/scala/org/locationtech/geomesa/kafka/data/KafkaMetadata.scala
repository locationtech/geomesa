/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.{InterruptException, WakeupException}
import org.locationtech.geomesa.index.metadata.{KeyValueStoreMetadata, MetadataSerializer}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.locationtech.geomesa.kafka.{KafkaAdminVersions, KafkaConsumerVersions}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Future, TimeUnit}
import java.util.{Collections, Properties, UUID}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Stores metadata in a Kafka topic
 *
 * @param config data store config
 * @param serializer serializer
 * @tparam T type param
 */
class KafkaMetadata[T](val config: KafkaDataStoreConfig, val serializer: MetadataSerializer[T])
    extends KeyValueStoreMetadata[T] {

  import org.apache.kafka.clients.consumer.ConsumerConfig.{AUTO_OFFSET_RESET_CONFIG, GROUP_ID_CONFIG}

  import scala.collection.JavaConverters._

  private val producer = new LazyProducer(KafkaDataStore.producer(config.brokers, config.producers.properties))
  private lazy val consumer = new TopicMap()

  override protected def checkIfTableExists: Boolean =
    adminClientOp(_.listTopics().names().get.contains(config.catalog))

  override protected def createTable(): Unit = {
    val newTopic =
      new NewTopic(config.catalog, 1, config.topics.replication.toShort)
          .configs(Collections.singletonMap("cleanup.policy", "compact"))
    adminClientOp(_.createTopics(Collections.singletonList(newTopic)).all().get)
  }

  override protected def createEmptyBackup(timestamp: String): KafkaMetadata[T] =
    new KafkaMetadata(config.copy(catalog = s"${config.catalog}_${timestamp}_bak"), serializer)

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    rows.foreach { case (row, value) =>
      producer.producer.send(new ProducerRecord(config.catalog, row, value))
    }
    producer.producer.flush()
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    rows.foreach { row =>
      producer.producer.send(new ProducerRecord(config.catalog, row, null))
    }
    producer.producer.flush()
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = consumer.get(row)

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    prefix match {
      case None => consumer.all()
      case Some(p) => consumer.prefix(p)
    }
  }

  override def close(): Unit = CloseWithLogging(Seq(producer, consumer))

  private def adminClientOp[V](fn: AdminClient => V): V = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    config.producers.properties.foreach { case (k, v) => props.put(k, v) }
    WithClose(AdminClient.create(props)) { admin => fn(admin) }
  }

  /**
   * Models the topic as a map of key-value pairs
   */
  private class TopicMap extends Runnable with Closeable {

    private val groupId = UUID.randomUUID().toString
    private val poll = Duration.of(100, ChronoUnit.MILLIS)

    private val state = new ConcurrentHashMap[KeyBytes, Array[Byte]]()
    private val complete = new CountDownLatch(1)

    private val consumer =
      KafkaDataStore.consumer(config.brokers,
        config.consumers.properties ++ Map(GROUP_ID_CONFIG -> groupId, AUTO_OFFSET_RESET_CONFIG -> "earliest"))

    private var future: Future[_] = _

    KafkaConsumerVersions.subscribe(consumer, config.catalog)
    doInitialLoad()

    override def run(): Unit = {
      try {
        var interrupted = Thread.currentThread().isInterrupted
        while (!interrupted) {
          try {
            val result = KafkaConsumerVersions.poll(consumer, poll)
            if (!result.isEmpty) {
              val records = result.iterator()
              while (records.hasNext) {
                val r = records.next()
                val v = r.value()
                if (v == null) {
                  state.remove(KeyBytes(r.key()))
                } else {
                  state.put(KeyBytes(r.key()), v)
                }
              }
              consumer.commitAsync()
            }
          } catch {
            case _: WakeupException | _: InterruptException | _: InterruptedException => interrupted = true
            case NonFatal(e) =>
              logger.warn(s"Consumer [$groupId] error receiving message from topic:", e)
              Thread.sleep(1000)
          }
        }
      } finally {
        complete.countDown()
      }
    }

    private def doInitialLoad(): Unit = {
      try {
        val offsets = scala.collection.mutable.Map.empty[Int, Long]
        // noinspection RedundantCollectionConversion
        val partitions = consumer.partitionsFor(config.catalog).asScala.map(_.partition).toSeq
        // note: end offsets are the *next* offset that will be returned, so subtract one to track the last offset
        // we will actually consume
        offsets ++=
            KafkaConsumerVersions.endOffsets(consumer, config.catalog, partitions)
                .collect { case (p, o) if o > 0 => (p, o - 1) }
        while (offsets.nonEmpty) {
          val result = KafkaConsumerVersions.poll(consumer, poll)
          if (!result.isEmpty) {
            val records = result.iterator()
            while (records.hasNext) {
              val r = records.next()
              val v = r.value()
              if (v == null) {
                state.remove(KeyBytes(r.key()))
              } else {
                state.put(KeyBytes(r.key()), v)
              }
              if (offsets.get(r.partition()).exists(o => r.offset() >= o)) {
                offsets.remove(r.partition())
              }
            }
            consumer.commitAsync()
          }
        }
        lazy val stateStrings =
          state.asScala.map { case (k, v) => new String(k.bytes, StandardCharsets.UTF_8) -> new String(v, StandardCharsets.UTF_8)}
        logger.debug(s"Completed initial load of catalog '${config.catalog}': \n  ${stateStrings.mkString("\n  ")}")

        future = CachedThreadPool.submit(this)
        sys.addShutdownHook(future.cancel(true)) // prevent consumer from hanging if ds isn't disposed properly
      } catch {
        case NonFatal(e) => complete.countDown(); throw e
      }
    }

    def get(key: Array[Byte]): Option[Array[Byte]] = Option(state.get(KeyBytes(key)))

    def all(): CloseableIterator[(Array[Byte], Array[Byte])] =
      CloseableIterator(state.asScala.iterator.map { case (k, v) => k.bytes -> v })

    def prefix(prefix: Array[Byte]): CloseableIterator[(Array[Byte], Array[Byte])] =
      all().filter { case (k, _) => k.startsWith(prefix) }

    override def close(): Unit = {
      try {
        if (future != null) {
          future.cancel(true)
        }
        complete.await(10, TimeUnit.SECONDS)
      } finally {
        cleanupConsumer()
      }
    }

    private def cleanupConsumer(): Unit = {
      try {
        val topics = consumer.assignment()
        consumer.unsubscribe()
        if (!topics.isEmpty) {
          Try(adminClientOp(KafkaAdminVersions.deleteConsumerGroupOffsets(_, groupId, topics))).failed.foreach { e =>
            logger.warn("Error deleting consumer group offsets:", e)
          }
        }
      } finally {
        consumer.close()
      }
    }
  }

  /**
   * Supports using a byte array as a map key
   *
   * @param bytes bytes
   */
  private case class KeyBytes(bytes: Array[Byte]) {
    override def hashCode(): Int = java.util.Arrays.hashCode(bytes)
    override def equals(obj: Any): Boolean = {
      obj match {
        case KeyBytes(other) => java.util.Arrays.equals(bytes, other)
        case _ => false
      }
    }
  }
}
