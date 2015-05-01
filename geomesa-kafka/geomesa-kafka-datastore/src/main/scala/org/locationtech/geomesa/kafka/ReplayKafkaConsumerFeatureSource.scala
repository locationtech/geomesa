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
import org.locationtech.geomesa.kafka.ReplayKafkaConsumerFeatureSource.History
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

  // history is stored as an array of events where the most recent is at index 0
  private val history: Array[Event] = readMessages()

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
      split.ts.map(history.indexAtTime).getOrElse(if (history.isEmpty) None else Some(0)).map(startIndex => {
        val endTime = split.ts.getOrElse(replayConfig.end.getMillis) - replayConfig.readBehind.getMillis

        val q = new Query(query)
        q.setFilter(split.filter.get)

        snapshot(startIndex, endTime).getReaderInternal(q)
      })
    }

    reader.getOrElse(new EmptyFeatureReader[SimpleFeatureType, SimpleFeature](schema))
  }

  private def snapshot(startIndex: Int, endTime: Long): SnapshotConsumerFeatureSource = {
    val snapshot: Seq[CUDEvent] = history.view
      .drop(startIndex)
      .takeWhile {
        // stop at the first clear or when past the endTime
        case c: ClearEvent => false
        case e => e.time >= endTime
      }
      .map(_.asInstanceOf[CUDEvent])

    new SnapshotConsumerFeatureSource(snapshot, entry, schema, query)
  }

  private def readMessages(): Array[Event] = {

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
      .foldLeft(EventSequence.empty)(_.add(_))
      .toArray
  }
}

object ReplayKafkaConsumerFeatureSource {

  val KafkaMessageTimestampAttribute = "KafkaMessageTimestamp"

  implicit class History(val events: Array[Event]) extends AnyVal {

    /** @return the most recent [[Event]] on or before the given ``time``
      */
    def indexAtTime(time: Long): Option[Int] = {
      val key = new Event {
        // look for first event before the given ``time`` because there may be multiple events
        // at the same time
        override val time: Long = time - 1
      }

      var index = java.util.Arrays.binarySearch(events.asInstanceOf[Array[AnyRef]], key)
      while (index >= 0 && events(index).time > time) index -= 1

      if (index >= 0) Some(index) else None
    }
  }
}

sealed trait Event extends Ordered[Event] {

  /** @return the time of the event
    */
  def time: Long

  override def compare(that: Event): Int = this.time.compare(that.time)
}


/** An [[Event]] in which all tracks were cleared.
 */
case class ClearEvent(override val time: Long) extends Event


/** An [[Event]] in which one or more tracks were created, updated, or deleted.
  *
  * @param changes a [[Map]] of the changes at the given ``time``, keyed by Track ID containing the
  *                new version of the [[SimpleFeature]]
  */
case class CUDEvent(override val time: Long, changes: Map[String, Option[SimpleFeature]])
  extends Event {

  /** Add an additional state change at the same time.
    *
    * @return a copy of ``this`` with an additional change
    */
  def add(trackId: String, sf: Option[SimpleFeature]): CUDEvent =
    CUDEvent(time, changes + (trackId -> sf))
}

object EventSequence {

  def empty: EventSequence = Seq.empty[Event]

  implicit class EventSequence(val events: Seq[Event]) extends AnyVal {

    def add(msg: GeoMessage): EventSequence = msg match {
      case update: CreateOrUpdate => createOrUpdateFeature(update)
      case delete: Delete => removeFeature(delete)
      case clear: Clear => clearFeatures(clear)
      case _ => throw new IllegalArgumentException("Unknown message: " + msg)
    }

    def createOrUpdateFeature(msg: CreateOrUpdate): EventSequence = {
      val sf = msg.feature
      addEvent(msg.timestamp.getMillis, sf.getID, Some(sf))
    }

    def removeFeature(msg: Delete): EventSequence = {
      addEvent(msg.timestamp.getMillis, msg.id, None)
    }

    def clearFeatures(msg: Clear): EventSequence = {
      val ts = msg.timestamp.getMillis

      events.headOption.map { head =>

        if (ts == head.time) {
          // clear at the same time as other changes; last wins so replaces existing head
          ClearEvent(ts) +: events.tail : EventSequence
        } else {
          ClearEvent(ts) +: events : EventSequence
        }

      }.getOrElse(Seq(ClearEvent(ts)))
    }

    def addEvent(ts: Long, trackId: String, sf: Option[SimpleFeature]): EventSequence = {
      events.headOption.map {
        case t: CUDEvent if t.time == ts =>
          // additional change at the same time - append to existing head
          t.add(trackId, sf) +: events.tail : EventSequence
        case h =>
          // new change
          CUDEvent(ts, Map(trackId -> sf)) +: events : EventSequence

      }.getOrElse(Seq(CUDEvent(ts, Map(trackId -> sf))))
    }
    
    def toArray: Array[Event] = events.toArray
  }
}

class SnapshotConsumerFeatureSource(events: Seq[CUDEvent],
                                    entry: ContentEntry,
                                    schema: SimpleFeatureType,
                                    query: Query)
  extends KafkaConsumerFeatureSource(entry, schema, query) {


  override lazy val (qt, features) = processEvents

  private def processEvents: (Quadtree, Cache[String, FeatureHolder]) = {
    def features: Cache[String, FeatureHolder] = CacheBuilder.newBuilder().build()
    def qt = new Quadtree
    def seen = new mutable.HashSet[String]

    events.foreach {e =>
      e.changes.foreach {
        case (trackId, sf) =>
          if (!seen.contains(trackId)) {
            sf.foreach { feature =>
              val env = feature.geometry.getEnvelopeInternal
              qt.insert(env, sf)
              features.put(feature.getID, FeatureHolder(feature, env))
            }
            seen.add(trackId)
          }
      }
    }

    (qt, features)
  }

  def processNewFeatures(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = update.id
    Option(features.getIfPresent(id)).foreach { old => qt.remove(old.env, old.sf) }
    val env = sf.geometry.getEnvelopeInternal
    qt.insert(env, sf)
    features.put(sf.getID, FeatureHolder(sf, env))
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
