/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.{SpatialIndex, WrappedQuadtree}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConverters._
import scala.collection.mutable

object ReplayTimeHelper {

  val ff = CommonFactoryFinder.getFilterFactory2

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

/** @param sft        the [[SimpleFeatureType]] - must contain the replay time attribute
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
  * @param sft    the SFT
  * @param events must be ordered, with the most recent first; must consist of only [[CreateOrUpdate]] and
  *               [[Delete]] messages
  */
case class ReplaySnapshotFeatureCache(override val sft: SimpleFeatureType,
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
  * @param start      the instant at which to start the replay
  * @param end        the instant at which to end the replay; must be >= ``start``
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

object ReplayConfig extends LazyLogging {

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
      case e: IllegalArgumentException =>
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
