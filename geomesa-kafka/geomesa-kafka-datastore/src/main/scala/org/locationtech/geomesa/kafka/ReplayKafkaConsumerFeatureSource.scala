/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.kafka

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.store.ContentEntry
import org.geotools.data.{EmptyFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.kafka.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.kafka.consumer.KafkaStreamLike.KafkaStreamLikeIterator
import org.locationtech.geomesa.kafka.consumer.offsets.FindOffset
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.FR
import org.locationtech.geomesa.utils.index.{SpatialIndex, WrappedQuadtree}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConverters._
import scala.collection.mutable

class ReplayKafkaConsumerFeatureSource(entry: ContentEntry,
                                       replaySFT: SimpleFeatureType,
                                       liveSFT: SimpleFeatureType,
                                       topic: String,
                                       kf: KafkaConsumerFactory,
                                       replayConfig: ReplayConfig,
                                       query: Query = null)
  extends KafkaConsumerFeatureSource(entry, replaySFT, query)
  with Logging {

  import TimestampFilterSplit.split

  // messages are stored as an array where the most recent is at index 0
  private[kafka] val messages: Array[GeoMessage] = readMessages()

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

  private[kafka] def snapshot(time: Option[Long]): Option[ReplaySnapshotFeatureCache] = {

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
  private[kafka] def indexAtTime(time: Long): Option[Int] = {

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

    logger.debug("Begin reading messages from {} using {}", topic, replayConfig)
    val readStart = System.currentTimeMillis()

    // don't want to block waiting for more messages so specify  a timeout
    val kafkaConsumer = kf.kafkaConsumer(topic, Map("consumer.timeout.ms" -> "250"))

    // use the liveSFT to decode becuase that's the type that was used to encode
    val msgDecoder = new KafkaGeoMessageDecoder(liveSFT)

    // start 1 ms earlier because there might be multiple messages with the same timestamp
    val startTime = replayConfig.realStartTime.minus(1L)

    val offsetRequest = FindOffset(msg => {
      val key = msgDecoder.decodeKey(msg)
      if (key.ts.isEqual(startTime)) 0 else if (key.ts.isAfter(startTime)) 1 else -1
    })

    // required: there is only 1 partition;  validate??
    val stream = kafkaConsumer.createMessageStreams(1, offsetRequest).head

    val msgIds = scala.collection.mutable.HashSet.empty[(Int, Long)]

    val msgs = stream.iterator
      .stopOnTimeout
      .filter(m => msgIds.add((m.partition, m.offset))) // avoid replayed duplicates
      .map(msgDecoder.decode)
      .dropWhile(replayConfig.isBeforeRealStart)
      .takeWhile(replayConfig.isNotAfterEnd)
      .foldLeft(List.empty[GeoMessage])((seq, elem) => elem :: seq)
      .toArray

    val readTime = (System.currentTimeMillis() - readStart).asInstanceOf[AnyRef]
    logger.debug("Read {} messages in {}ms from {} using {}",
      msgs.length.asInstanceOf[AnyRef], readTime, topic, replayConfig)

    msgs
  }
}

object ReplayTimeHelper {

  private[kafka] val ff = CommonFactoryFinder.getFilterFactory2

  val AttributeName: String = "KafkaLogTime"
  val AttributeProp: PropertyName = ff.property(AttributeName)

  def addReplayTimeAttribute(builder: SimpleFeatureTypeBuilder): Unit =
    builder.add(AttributeName, classOf[Date])
  
  def toFilter(time: Instant): Filter =
    ff.equals(AttributeProp, ff.literal(time.toDate))

  def fromFilter(filter: PropertyIsEqualTo): Option[Long] = {
    checkOrder(filter.getExpression1, filter.getExpression2)
      .filter(pl => pl.name == AttributeName && pl.literal.getValue.isInstanceOf[Date])
      .map(pl => new Instant(pl.literal.getValue.asInstanceOf[Date]).getMillis)
  }
}

/** @param sft the [[SimpleFeatureType]] - must contain the replay time attribute
  * @param replayTime the current replay time
  */
class ReplayTimeHelper(sft: SimpleFeatureType, replayTime: Long) {
  import ReplayTimeHelper._

  private val replayDate = new java.util.Date(replayTime)
  private val builder = new SimpleFeatureBuilder(sft)
  private val attrIndex = sft.indexOf(AttributeName)

  require(attrIndex >= 0, s"Invalid SFT.  The $AttributeName attribute is missing.")

  /** Copy the given ``sf`` and add a value for the replay time attribute. */
  def reType(sf: SimpleFeature): SimpleFeature = {
    builder.init(sf)
    builder.set(attrIndex, replayDate)
    builder.buildFeature(sf.getID)
  }
}


/** Represents the state at a specific point in time.
  *
  * @param sft the SFT
  * @param events must be ordered, with the most recent first; must consist of only [[CreateOrUpdate]] and
  *               [[Delete]] messages
  */
private[kafka] case class ReplaySnapshotFeatureCache(override val sft: SimpleFeatureType,
                                                     replayTime: Long,
                                                     events: Seq[GeoMessage])

  extends KafkaConsumerFeatureCache {

  override lazy val (spatialIndex, features) = processMessages

  private def processMessages: (SpatialIndex[SimpleFeature], mutable.Map[String, FeatureHolder]) = {
    val features = new mutable.HashMap[String, FeatureHolder]()
    val qt = new WrappedQuadtree[SimpleFeature]
    val seen = new mutable.HashSet[String]

    val timeHelper = new ReplayTimeHelper(sft, replayTime)

    events.foreach {
      case CreateOrUpdate(ts, sf) =>
        val id = sf.getID

        // starting with the most recent so if haven't seen it yet, add it, otherwise keep newer version
        if (!seen(id)) {
          val env = sf.geometry.getEnvelopeInternal
          val modSF = timeHelper.reType(sf)

          qt.insert(env, modSF)
          features.put(id, FeatureHolder(modSF, env))
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

  require(start.getMillis <= end.getMillis, "The start time must not be after the end time.")

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

  def isInWindow(time: Long): Boolean = !(start.isAfter(time) || end.isBefore(time))
}

object ReplayConfig extends Logging {

  def apply(start: Long, end: Long, readBehind: Long): ReplayConfig =
    ReplayConfig(new Instant(start), new Instant(end), Duration.millis(readBehind))

  def encode(conf: ReplayConfig): String =
    s"${conf.start.getMillis.toHexString}-${conf.end.getMillis.toHexString}-${conf.readBehind.getMillis.toHexString}"

  def decode(rcString: String): Option[ReplayConfig] = {

    try {
      val values = rcString.split('-').map(java.lang.Long.valueOf(_, 16))

      if (values.length != 3) {
        logger.error("Unable to decode ReplayConfig. Wrong number of tokens splitting " + rcString)
        None
      } else {

        val start = new Instant(values(0))
        val end = new Instant(values(1))
        val duration = Duration.millis(values(2))
        Some(ReplayConfig(start, end, duration))
      }
    } catch {
      case e: IllegalArgumentException  =>
        logger.error("Exception thrown decoding ReplayConfig.", e)
        None
    }
  }
}

/** Splits a [[Filter]] into the requested Kafka Message Timestamp and the remaining filters
  */
case class TimestampFilterSplit(ts: Option[Long], filter: Option[Filter])

object TimestampFilterSplit {

  import ReplayTimeHelper.ff

  /** Look for a Kafka message timestamp filter in ``filter`` and if found, extract the requested timestamp
    * and return that timestamp and the remaining filters.
    *
    * Any operand (or none) of an 'and' may specify a timestamp.  If multiple operands of the 'and'
    * specify a timestamp then all timestamps must be the same.
    *
    * For an 'or' the requirement is that either all operands specify the same timestamp or none specify a
    * timestamp.
    *
    * A timestamp may not be specified within a 'not'.
    */
  def split(filter: Filter): Option[TimestampFilterSplit] = filter match {

    case eq: PropertyIsEqualTo =>
      val ts = ReplayTimeHelper.fromFilter(eq)
      val f = ts.map(_ => None).getOrElse(Some(filter))
      Some(TimestampFilterSplit(ts, f))

    case a: And =>
      // either no child specifies a timestamp, one child specifies a timestamp or multiple children specify
      // the same timestamp
      split(a, buildAnd)

    case o: Or =>
      // either all children specify the same timestamp or none specify a timestamp
      split(o, buildOr)

    case n: Not =>
      // the filter being inverted may not contain a timestamp
      val s = split(n.getFilter)
      s.flatMap(split => split.ts.map(_ => None)
        .getOrElse(Some(TimestampFilterSplit(None, split.filter.map(ff.not)))))

    case _ => Some(TimestampFilterSplit(None, Some(filter)))
  }

  type SplitCombiner = Seq[TimestampFilterSplit] => Option[TimestampFilterSplit]

  def split(op: BinaryLogicOperator, combiner: SplitCombiner): Option[TimestampFilterSplit] = {
    val children = op.getChildren.asScala
    val childSplits = children.flatMap(c => split(c))

    if (childSplits.size != children.size) {
      // one or more children are invalid
      None
    } else {
      combiner(childSplits)
    }
  }

  def buildAnd(childSplits: Seq[TimestampFilterSplit]): Option[TimestampFilterSplit] = {
    val tsList = childSplits.flatMap(_.ts)
    val ts = tsList.headOption

    if (tsList.nonEmpty && tsList.tail.exists(_ != tsList.head)) {
      // inconsistent timestamps
      None
    } else {
      val filters = childSplits.flatMap(_.filter)
      val filter = combine(filters, ff.and)

      Some(TimestampFilterSplit(ts, filter))
    }
  }

  def buildOr(childSplits: Seq[TimestampFilterSplit]): Option[TimestampFilterSplit] = {
    val ts = childSplits.headOption.flatMap(_.ts)

    if (!childSplits.forall(_.ts == ts)) {
      // inconsistent timestamps
      None
    } else {
      val filters = childSplits.flatMap(_.filter)
      val filter = combine(filters, ff.or)

      Some(TimestampFilterSplit(ts, filter))
    }
  }

  def combine(filters: Seq[Filter], combiner: java.util.List[Filter] => Filter): Option[Filter] = {

    if (filters.isEmpty) {
      None
    } else if (filters.size == 1) {
      filters.headOption
    } else {
      Some(combiner(filters.asJava))
    }
  }
}

