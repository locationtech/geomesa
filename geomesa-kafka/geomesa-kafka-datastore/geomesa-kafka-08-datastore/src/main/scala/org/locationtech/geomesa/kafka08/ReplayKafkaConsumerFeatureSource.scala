/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import java.io.Closeable

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.store.ContentEntry
import org.geotools.data.{EmptyFeatureReader, Query}
import org.joda.time.Instant
import org.locationtech.geomesa.kafka._
import org.locationtech.geomesa.kafka08.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.kafka08.consumer.KafkaStreamLike._
import org.locationtech.geomesa.kafka08.consumer.offsets.FindOffset
import org.locationtech.geomesa.utils.geotools.FR
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

class ReplayKafkaConsumerFeatureSource(entry: ContentEntry,
                                       replaySFT: SimpleFeatureType,
                                       liveSFT: SimpleFeatureType,
                                       topic: String,
                                       kf: KafkaConsumerFactory,
                                       replayConfig: ReplayConfig,
                                       query: Query)
  extends KafkaConsumerFeatureSource(entry, replaySFT, query, false) with MethodProfiling with Closeable with LazyLogging {

  import TimestampFilterSplit.split

  // messages are stored as an array where the most recent is at index 0
  private[kafka08] val messages: Array[GeoMessage] = {
    val timing = new Timing
    logger.debug(s"Begin reading messages from $topic using $replayConfig")
    val msgs = profile(readMessages())(timing)
    logger.debug(s"Read ${msgs.length} messages in ${timing.time}ms from $topic using $replayConfig")
    msgs
  }

  override def getReaderForFilter(filter: Filter): FR = {

    val reader: Option[FR] = if (messages.isEmpty) {
      // no data!
      None
    } else {
      split(filter).flatMap { tfs =>
        val time = tfs.ts
        val filter = tfs.filter.getOrElse(Filter.INCLUDE)

        snapshot(time).map(_.getReaderForFilter(filter))
      }
    }

    reader.getOrElse(new EmptyFeatureReader[SimpleFeatureType, SimpleFeature](replaySFT))
  }

  private[kafka08] def snapshot(time: Option[Long]): Option[ReplaySnapshotFeatureCache] = {

    val (startTime, startIndex) = time
      .map(ts => (ts, indexAtTime(ts)))
      .getOrElse((replayConfig.end.getMillis, Some(0)))

    startIndex.map { si =>
      val endTime = startTime - replayConfig.readBehind.getMillis
      snapshot(startTime, si, endTime)
    }.getOrElse(None)
  }

  /** @return the index of the most recent [[GeoMessage]] at or before the given ``time``
    */
  private[kafka08] def indexAtTime(time: Long): Option[Int] = {

    if (replayConfig.isInWindow(time)) {
      // it doesn't matter what the message is, only the time
      val key: GeoMessage = new Clear(new Instant(time))

      // reverse ordering for reverse ordered ``messages``
      val ordering = new Ordering[GeoMessage] {
        override def compare(x: GeoMessage, y: GeoMessage): Int = y.timestamp.compareTo(x.timestamp)
      }

      var index = java.util.Arrays.binarySearch(messages, key, ordering)

      if (index < 0) {
        // no message found at sought time; convert to index of first message before ``time``
        index = -index - 1
      }

      // there may be multiple messages at the same time so there may be messages before ``index``
      // that are at the given ``time``
      while (index > 0 && messages(index - 1).timestamp.getMillis <= time) index -= 1

      if (index < messages.length) Some(index) else None
    } else {
      // requested time is outside of user specified time window
      None
    }
  }

  /**
    * @param startIndex the index of the most recent message to process
    * @param endTime the time of the last message to process
    */
  private def snapshot(time: Long, startIndex: Int, endTime: Long): Option[ReplaySnapshotFeatureCache] = {
    val snapshot: Seq[GeoMessage] = messages.view
      .drop(startIndex)
      .takeWhile {
        // stop at the first clear or when past the endTime
        case c: Clear => false
        case e => e.timestamp.getMillis >= endTime
      }

    if (snapshot.isEmpty) {
      None
    } else {
      Some(ReplaySnapshotFeatureCache(replaySFT, time, snapshot))
    }
  }

  private def readMessages(): Array[GeoMessage] = {
    // use the liveSFT to decode becuase that's the type that was used to encode
    val msgDecoder = new KafkaGeoMessageDecoder(liveSFT)

    // start 1 ms earlier because there might be multiple messages with the same timestamp
    val startTime = replayConfig.realStartTime.minus(1L)

    val offsetRequest = FindOffset(msg => {
      val key = msgDecoder.decodeKey(msg)
      if (key.ts.isEqual(startTime)) 0 else if (key.ts.isAfter(startTime)) 1 else -1
    })

    // don't want to block waiting for more messages so specify  a timeout
    val kafkaConsumer = kf.kafkaConsumer(topic, Map("consumer.timeout.ms" -> "250"))

    try {
      // required: there is only 1 partition;  validate??
      val stream = kafkaConsumer.createMessageStreams(1, offsetRequest).head

      val msgIds = scala.collection.mutable.HashSet.empty[(Int, Long)]

      stream.iterator
        .stopOnTimeout
        .filter(m => msgIds.add((m.partition, m.offset))) // avoid replayed duplicates
        .map(msgDecoder.decode)
        .dropWhile(replayConfig.isBeforeRealStart)
        .takeWhile(replayConfig.isNotAfterEnd)
        .foldLeft(List.empty[GeoMessage])((seq, elem) => elem :: seq)
        .toArray
    } finally {
      kafkaConsumer.shutdown()
    }
  }

  override def close(): Unit = {}
}

