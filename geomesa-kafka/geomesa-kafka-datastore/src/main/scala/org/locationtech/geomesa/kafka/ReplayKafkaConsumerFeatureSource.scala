/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import com.google.common.cache.{CacheBuilder, Cache}
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.quadtree.Quadtree
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{EmptyFeatureReader, FeatureReader, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.kafka.ReplayKafkaConsumerFeatureSource.GeoMessages
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConverters._
import scala.collection.mutable

class ReplayKafkaConsumerFeatureSource(entry: ContentEntry,
                                       schema: SimpleFeatureType,
                                       query: Query,
                                       topic: String,
                                       zookeepers: String,
                                       replayConfig: ReplayConfig)(implicit val kf: KafkaFactory)
  extends ContentFeatureSource(entry, query) {

  // messages are stored as an array where the most recent is at index 0
  private val messages: Array[GeoMessage] = readMessages()

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val split = TimestampFilterSplit.split(query.getFilter)

    val reader = if (split.filter.getOrElse(Filter.EXCLUDE) == Filter.EXCLUDE) {
      None
    } else {
      // default to end time if no time specified
      split.ts.map(messages.indexAtTime).getOrElse(if (messages.isEmpty) None else Some(0)).map(startIndex => {
        val endTime = split.ts.getOrElse(replayConfig.end.getMillis) - replayConfig.readBehind.getMillis

        val q = new Query(query)
        q.setFilter(split.filter.get)

        snapshot(startIndex, endTime).getReaderInternal(q)
      })
    }

    reader.getOrElse(new EmptyFeatureReader[SimpleFeatureType, SimpleFeature](schema))
  }

  /**
    * @param startIndex the index of the most recent message to process
    * @param endTime the time of the last message to process
    */
  private def snapshot(startIndex: Int, endTime: Long): SnapshotConsumerFeatureSource = {
    val snapshot: Seq[GeoMessage] = messages.view
      .drop(startIndex)
      .takeWhile {
        // stop at the first clear or when past the endTime
        case c: Clear => false
        case e => e.timestamp.getMillis >= endTime
      }

    new SnapshotConsumerFeatureSource(snapshot, entry, schema, query)
  }

  private def readMessages(): Array[GeoMessage] = {

    val kafkaConsumer = kf.kafkaConsumer(zookeepers)
    val msgDecoder = new KafkaGeoMessageDecoder(schema)

    // start 1 ms earlier because there might be multiple messages with the same timestamp
    val startTime = replayConfig.realStartTime.minus(1L)

    val offsetRequest = FindOffset(msg => {
      val key = msgDecoder.decodeKey(msg)
      if (key.ts.isEqual(startTime)) 0 else if (key.ts.isAfter(startTime)) 1 else -1
    })

    // required: there is only 1 partition;  validate??
    val stream = kafkaConsumer.createMessageStreams(topic, 1, offsetRequest).head

    // stop at the last offset even if before the end instant
    val lastOffset = kafkaConsumer.getOffsets(topic, LatestOffset).head

    stream.iterator
      .takeWhile(_.offset <= lastOffset)
      .map(msgDecoder.decode)
      .dropWhile(replayConfig.isBeforeRealStart)
      .takeWhile(replayConfig.isNotAfterEnd)
      .foldLeft(Seq.empty[GeoMessage])((seq, elem) => elem +: seq)
      .toArray
  }
}

object ReplayKafkaConsumerFeatureSource {

  val KafkaMessageTimestampAttribute = "KafkaMessageTimestamp"

  implicit class GeoMessages(val messages: Array[GeoMessage]) extends AnyVal {

    /** @return the index of the most recent [[GeoMessage]] on or before the given ``time``
      */
    def indexAtTime(time: Long): Option[Int] = {

      // look for first event before the given ``time`` because there may be
      // multiple events at the same time
      // it doesn't matter what the message is, only the time
      val key = new Clear(new Instant(time - 1))

      // reverse ordering for reverse ordered ``messages``
      val ordering = new Ordering[GeoMessage] {
        override def compare(x: GeoMessage, y: GeoMessage): Int = y.timestamp.compareTo(x.timestamp)
      }

      var index = java.util.Arrays.binarySearch(messages, key, ordering)

      if (index < 0) {
        // no message found at sought time
        index = -index -1
      }

      // walk forward to the first message at ``time``
      while (index >= 0 && messages(index).timestamp.getMillis > time) index -= 1

      if (index < messages.length) Some(index) else None
    }
  }
}


class SnapshotConsumerFeatureSource(events: Seq[GeoMessage],
                                    entry: ContentEntry,
                                    schema: SimpleFeatureType,
                                    query: Query)
  extends KafkaConsumerFeatureSource(entry, schema, query) {

  override lazy val (qt, features) = processMessages

  private def processMessages: (Quadtree, Cache[String, FeatureHolder]) = {
    def features: Cache[String, FeatureHolder] = CacheBuilder.newBuilder().build()
    def qt = new Quadtree
    def seen = new mutable.HashSet[String]

    events.foreach {
      case CreateOrUpdate(ts, sf) =>
        val id = sf.getID

        // starting with the most recent so if haven't seen it yet, add it, otherwise keep newer version
        if (!seen(id)) {
          val env = sf.geometry.getEnvelopeInternal

          qt.insert(env, sf)
          features.put(id, FeatureHolder(sf, env))
          seen.add(id)
        }

      case Delete(ts, id) =>
        seen.add(id)

      case unknown =>
        // clear messages should not get here
        throw new IllegalStateException(s"Unexpected message: '$unknown'")
    }

    (qt, features)
  }
}


/** Configuration for replaying a Kafka DataStore.
  *
  * @param start the instant at which to start the replay
  * @param end the instant at which to end the replay; must be >= ``start``
  * @param readBehind the additional time to pre-read
 */
case class ReplayConfig(start: Instant, end: Instant, readBehind: Duration) {

  require(start.getMillis <= end.getMillis)

  /** The starting time to read from kafka, accounting for read behind. */
  val realStartTime: Instant = start.minus(readBehind)

  /**
   * @param msg the [[GeoMessage]] to check
   * @return true if the ``message`` is before the ``realStartTime`` [[Instant]]
   */
  def isBeforeRealStart(msg: GeoMessage): Boolean = msg.timestamp.isBefore(realStartTime)

  /**
    * @param msg the [[GeoMessage]] to check
    * @return true if the ``message`` is not after the ``end`` [[Instant]]
    */
  def isNotAfterEnd(msg: GeoMessage): Boolean = !msg.timestamp.isAfter(end)
}


case class TimestampFilterSplit(ts: Option[Long], filter: Option[Filter])

object TimestampFilterSplit {

  import ReplayKafkaConsumerFeatureSource.KafkaMessageTimestampAttribute

  /** Look for a Kafka message timestamp filter in ``filter`` and if found, extract the requested timestamp
    * and return that timestamp and the remaining filters.
    *
    * If multiple message timestamps filters are found joined by 'and' or 'or' then all found timestamps must
    * be exactly equal.  If not then an ``EXCLUDE`` filters will be used.  In the case of 'and' this is
    * logically correct.  In the case of 'or', the query makes no sense because each timestamp represents a
    * moment in time.
    */
  def split(filter: Filter): TimestampFilterSplit = filter match {

    case eq: PropertyIsEqualTo =>
      val ts = checkOrder(eq.getExpression1, eq.getExpression2)
        .filter(pl => pl.name == KafkaMessageTimestampAttribute && pl.literal.getValue.isInstanceOf[Long])
        .map(_.literal.getValue.asInstanceOf[Long])
      val f = ts.map(_ => None).getOrElse(Some(filter))
      TimestampFilterSplit(ts, f)

    case a: And =>
      split(a, ff.and)

    case o: Or =>
      split(o, ff.or)

    case _ => TimestampFilterSplit(None, Some(filter))
  }

  def split(op: BinaryLogicOperator, combiner: java.util.List[Filter] => Filter): TimestampFilterSplit = {
    op.getChildren.asScala.map(split)
      .foldLeft(new TimestampFilterLists()) {_ + _}
      .combine(combiner)
  }
}

/** Helper class to [[TimestampFilterSplit]] for processing [[And]] and [[Or]] filters.
  *
  * @param timestamps the sequence of message timestamps found in the [[And]] or [[Or]] filter
  * @param filters the sequence of non-message-timestamp filters found in the [[And]] or [[Or]] filter
  */
case class TimestampFilterLists(timestamps: Seq[Long], filters: Seq[Filter]) {

  def this() {
    this(Seq.empty[Long], Seq.empty[Filter])
  }

  def +(split: TimestampFilterSplit): TimestampFilterLists = {
    // could have a timestamp, a filter, both or neither
    val ts = split.ts.map(timestamps :+ _).getOrElse(timestamps)
    val f = split.filter.map(filters :+ _).getOrElse(filters)
    new TimestampFilterLists(ts, f)
  }

  def isConsistent: Boolean = timestamps.tail.forall(_ == timestamps.head)

  def combine(combiner: java.util.List[Filter] => Filter): TimestampFilterSplit = {
    if (isConsistent) {
      // all timestamps are the same or there are none
      val ts = timestamps.headOption
      val filter =
        if (filters.size > 1) {
          Some(combiner(filters.toList.asJava))
        } else {
          filters.headOption
        }
      TimestampFilterSplit(ts, filter)
    } else {
      // have multiple different timestamps
      TimestampFilterSplit(None, None)
    }
  }
}
