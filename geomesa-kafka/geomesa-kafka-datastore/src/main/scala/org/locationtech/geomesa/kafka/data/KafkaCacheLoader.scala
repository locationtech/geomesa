/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.Closeable
import java.time.{Duration, Instant}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.{Collections, Date}

import com.google.common.base.Charsets
import com.google.common.collect.{ForwardingMap, Maps, Queues}
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageSerializer, KafkaFeatureEvent}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.Expression

import scala.util.control.NonFatal

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {

  private val listeners = Collections.newSetFromMap {
    new ConcurrentHashMap[(SimpleFeatureSource, FeatureListener), java.lang.Boolean]()
  }

  def cache: KafkaFeatureCache

  def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit =
    listeners.add((source, listener))

  def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit =
    listeners.remove((source, listener))

  protected [KafkaCacheLoader] def fireEvent(message: Change): Unit = {
    if (!listeners.isEmpty) {
      fireEvent(KafkaFeatureEvent.changed(_, message.feature, message.timestamp.toEpochMilli))
    }
  }

  protected [KafkaCacheLoader] def fireEvent(message: Delete): Unit = {
    if (!listeners.isEmpty) {
      val removed = cache.query(message.id).orNull
      fireEvent(KafkaFeatureEvent.removed(_, message.id, removed, message.timestamp.toEpochMilli))
    }
  }

  protected [KafkaCacheLoader] def fireEvent(message: Clear): Unit = {
    if (!listeners.isEmpty) {
      fireEvent(KafkaFeatureEvent.cleared(_, message.timestamp.toEpochMilli))
    }
  }

  private def fireEvent(toEvent: (SimpleFeatureSource) => FeatureEvent): Unit = {
    import scala.collection.JavaConversions._
    val events = scala.collection.mutable.Map.empty[SimpleFeatureSource, FeatureEvent]
    listeners.foreach { case (source, listener) =>
      val event = events.getOrElseUpdate(source, toEvent(source))
      try { listener.changed(event) } catch {
        case NonFatal(e) => logger.error(s"Error in feature listener for $event", e)
      }
    }
  }
}

object KafkaCacheLoader {

  object NoOpLoader extends KafkaCacheLoader {
    override val cache: KafkaFeatureCache = KafkaFeatureCache.empty()
    override def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def close(): Unit = {}
  }

  class KafkaCacheLoaderImpl(sft: SimpleFeatureType,
                             override val cache: KafkaFeatureCache,
                             override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
                             override protected val topic: String,
                             override protected val frequency: Long,
                             private var doInitialLoad: Boolean,
                             eventTime: Boolean,
                             eventTimeOrdering: Boolean,
                             eventTimeAttribute: Expression) extends ThreadedConsumer with KafkaCacheLoader {

    private val serializer = new GeoMessageSerializer(sft)

    if (doInitialLoad) {
      doInitialLoad = false
      // for the initial load, don't bother indexing in the feature cache until we have the final state
      val executor = Executors.newSingleThreadExecutor()
      executor.submit(new InitialLoader(consumers, topic, frequency, eventTime, eventTimeOrdering, eventTimeAttribute, serializer, this))
      executor.shutdown()
    } else {
      startConsumers()
    }

    override def close(): Unit = {
      try {
        super.close()
      } finally {
        cache.close()
      }
    }

    override protected [KafkaCacheLoader] def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      val message = serializer.deserialize(record.key, record.value)
      logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
      message match {
        case m: Change => fireEvent(m); cache.put(m.feature)
        case m: Delete => fireEvent(m); cache.remove(m.id)
        case m: Clear  => fireEvent(m); cache.clear()
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
  private class InitialLoader(override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
                              override protected val topic: String,
                              override protected val frequency: Long,
                              eventTime: Boolean,
                              eventTimeOrdering: Boolean,
                              eventTimeAttribute: Expression,
                              serializer: GeoMessageSerializer,
                              toLoad: KafkaCacheLoaderImpl) extends ThreadedConsumer with Runnable {

    private val loadCache: java.util.Map[String, SimpleFeature] = {
      // TODO: Does this need to be synchronized?
      val map = Maps.newHashMap[String,SimpleFeature]()
      if(eventTime && eventTimeOrdering) {
        new ForwardingMap[String, SimpleFeature] {
          override def delegate(): util.Map[String, SimpleFeature] = map

          override def put(key: String, value: SimpleFeature): SimpleFeature = {
            try {
              if (!map.containsKey(key)) {
                super.put(key, value)
              } else {
                val dtg = eventTimeAttribute.evaluate(value)
                if (dtg == null) {
                  // TODO: Does this violate the Map contract?
                  logger.warn(s"Received feature with null event-time, ignoring $key")
                  null
                } else {
                  val cur = map.get(key)
                  val curEventTime = eventTimeAttribute.evaluate(cur).asInstanceOf[Date]
                  if (dtg.asInstanceOf[Date].after(curEventTime)) {
                    super.put(key, value)
                  } else {
                    // TODO: Does this violate the Map contract?
                    logger.trace(s"Received an out-of-order feature, ignoring $key")
                    null
                  }
                }
              }
            } catch {
              case t: Throwable =>
                logger.error(s"While processing feature $key", t)
                null
            }
          }
        }
      } else {
        map
      }
    }

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private val done = new AtomicBoolean(false)

    override protected def closeConsumers: Boolean = false

    private val numConsumerThreads = Option(System.getProperty("geomesa.kafka.initial-load-threads")).map(_.toInt).getOrElse(4)
    private val hashQueues = Array.fill(numConsumerThreads)(Queues.newArrayBlockingQueue[GeoMessage](1000000))
    private val messageProcessors = (0 until numConsumerThreads).map { i => new GeoMessageProcessor(i) }
    private val messageProcessorPool = Executors.newFixedThreadPool(numConsumerThreads)
    private val messageProcessorFutures = messageProcessors.map { mp => messageProcessorPool.submit(mp) }

    private class GeoMessageProcessor(mod: Int) extends Runnable {
      private val q = hashQueues(mod)
      override def run(): Unit = {
        // while the queue is not empty or we haven't toggled done
        while(!q.isEmpty || !done.get()) {
          try {
            val msg = q.poll(100, TimeUnit.MILLISECONDS)
            msg match {
              case null => // do nothing
              case m: Change => toLoad.fireEvent(m); loadCache.put(m.feature.getID, m.feature)
              case m: Delete => toLoad.fireEvent(m); loadCache.remove(m.id)
              case m: Clear => toLoad.fireEvent(m); loadCache.clear()
              case m => throw new IllegalArgumentException(s"Unknown message: $m")
            }
          } catch {
            case t: Throwable =>
              logger.warn("Failure while processing messages", t)
          }
        }
      }
    }

    // we use a hash so all records with the same id are processed in the order they are received
    private val hash = Hashing.goodFastHash(32)

    private var lastLog = Instant.now()
    // TODO: will we ever have more than 32 partitions???
    private val loggedOffsets = Array.ofDim[Long](32)
    private var count = 0
    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done.get) { toLoad.consume(record) } else {
        val message = serializer.deserialize(record.key, record.value)
        logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
        loggedOffsets(record.partition) = record.offset

        message match {
          case m: Change => hashQueues(math.abs(hash.hashString(m.feature.getID, Charsets.UTF_8).asInt()%numConsumerThreads)).put(m)
          case m: Delete => hashQueues(math.abs(hash.hashString(m.id, Charsets.UTF_8).asInt()%numConsumerThreads)).put(m)
          case m: Clear  => hashQueues.foreach { q => q.put(m) }
          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }

        count+=1
        if(count % 1000000 == 0) {
          if (Duration.between(lastLog, Instant.now()).getSeconds > 60) {
            (0 until offsets.size()).foreach { i =>
              val curOffset = loggedOffsets(i)
              val maxOffset = offsets.get(i)
              logger.info(s"$topic: Processed up to $curOffset of $maxOffset total for partition $i")
            }
            lastLog = Instant.now()
          }
        }

        // once we've hit the max offset for the partition, remove from the offset map to indicate we're done
        if (offsets.getOrDefault(record.partition, Long.MaxValue) <= record.offset) {
          offsets.remove(record.partition)
        }
      }
    }

    override def run(): Unit = {
      import scala.collection.JavaConverters._

      logger.info(s"Starting initial load for $topic")
      val partitions = consumers.head.partitionsFor(topic).asScala.map(_.partition)
      try {
        // note: these methods are not available in kafka 0.9, which will cause it to fall back to normal loading
        val beginningOffsets = KafkaConsumerVersions.beginningOffsets(consumers.head, topic, partitions)
        val endOffsets = KafkaConsumerVersions.endOffsets(consumers.head, topic, partitions)
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
        startConsumers() // kick off the asynchronous consumer threads
        try {
          // offsets will get removed from the map in `consume`, so we're done when the map is empty
          while (!offsets.isEmpty) {
            Thread.sleep(1000)
          }
        } finally {
          // stop the consumer threads, but won't close the consumers due to `closeConsumers`
          close()
        }
        // set a flag just in case the consumer threads haven't finished spinning down, so that we will
        // pass any additional messages back to the main loader
        done.set(true)
        // wait for message process threads to finish
        messageProcessorPool.shutdown()
        messageProcessorFutures.foreach { _.get() }
        logger.info(s"Processed all data for $topic, transferring to in-memory cache")
        loadCache.asScala.foreach { case (_, v) => toLoad.cache.put(v) }
        logger.info(s"Finished initial load for $topic")
        loadCache.clear()
      }
      logger.info(s"Starting normal cache loading for $topic")
      // start the normal loading
      toLoad.startConsumers()
    }
  }
}
