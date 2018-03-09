/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.{GeoMessageSerializer, KafkaFeatureEvent}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

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

  protected def fireEvent(message: Change): Unit = {
    if (!listeners.isEmpty) {
      fireEvent(KafkaFeatureEvent.changed(_, message.feature, message.timestamp.toEpochMilli))
    }
  }

  protected def fireEvent(message: Delete): Unit = {
    if (!listeners.isEmpty) {
      val removed = cache.query(message.id).orNull
      fireEvent(KafkaFeatureEvent.removed(_, message.id, removed, message.timestamp.toEpochMilli))
    }
  }

  protected def fireEvent(message: Clear): Unit = {
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
                             val cache: KafkaFeatureCache,
                             override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]])
      extends ThreadedConsumer with KafkaCacheLoader {

    override protected val topic: String = KafkaDataStore.topic(sft)
    override protected val frequency: Long =
      SystemProperty("geomesa.kafka.load.interval").toDuration.map(_.toMillis).getOrElse(100L)

    private val serializer = new GeoMessageSerializer(sft)

    override def close(): Unit = {
      try {
        super.close()
      } finally {
        cache.close()
      }
    }

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
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
}

