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

import org.geotools.data.Query
import org.geotools.data.store.ContentEntry
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.core.filter._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConverters._
import scala.collection.immutable.{SortedMap, TreeMap}

object ReplayKafkaConsumerFeatureSource {

  val KafkaMessageTimestampAttribute = "KafkaMessageTimestamp"
}

class ReplayKafkaConsumerFeatureSource(entry: ContentEntry,
                                       schema: SimpleFeatureType,
                                       query: Query,
                                       topic: String,
                                       zookeepers: String,
                                       replayConfig: ReplayConfig)(implicit val kf: KafkaFactory) {

  def readMessages(): SortedMap[Long, Seq[KafkaGeoMessage]] = {

    val kafkaConsumer = kf.kafkaConsumer(zookeepers)
    val msgDecoder = new KafkaGeoMessageDecoder(schema)

    val startTime = replayConfig.realStartTime

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
      .takeWhile(replayConfig.isNotAfterEnd)
      .foldLeft(TreeMap.empty[Long, Seq[KafkaGeoMessage]]) { (map, msg) =>
        val ts = msg.timestamp.getMillis
        val msgList = map.get(ts).map(_ :+ msg).getOrElse(Seq(msg))
        map + (ts -> msgList)
      }
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
    * @param msg the [[KafkaGeoMessage]] to check
    * @return true if the ``message`` is not after the ``end`` [[Instant]]
    */
  def isNotAfterEnd(msg: KafkaGeoMessage): Boolean = end.isAfter(msg.timestamp)
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
