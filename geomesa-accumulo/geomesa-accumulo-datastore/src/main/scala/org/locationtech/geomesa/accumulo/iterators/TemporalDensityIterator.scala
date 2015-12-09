/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Date, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, Interval}
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.accumulo.iterators.KryoLazyTemporalDensityIterator.TimeSeries
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.TimeSnap

import scala.collection.mutable

@deprecated
class TemporalDensityIterator(other: FeatureAggregatingIterator[TemporalDensityIteratorResult], env: IteratorEnvironment)
  extends FeatureAggregatingIterator[TemporalDensityIteratorResult](other, env) {

  var snap: TimeSnap = null
  var dateTimeFieldName: String = null

  projectedSFTDef = KryoLazyTemporalDensityIterator.TEMPORAL_DENSITY_SFT_STRING

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                         options: JMap[String, String],
                                                         env: IteratorEnvironment): Unit = {

    dateTimeFieldName = simpleFeatureType.getDtgField.getOrElse(throw new IllegalArgumentException("dtg field required"))

    val buckets = TemporalDensityIterator.getBuckets(options)
    val bounds = TemporalDensityIterator.getTimeBounds(options)
    snap = new TimeSnap(bounds, buckets)
  }

  override def handleKeyValue(resultO: Option[TemporalDensityIteratorResult],
                              topSourceKey: Key,
                              topSourceValue: Value): TemporalDensityIteratorResult = {
    val date = originalDecoder.deserialize(topSourceValue.get()).getAttribute(dateTimeFieldName).asInstanceOf[Date]
    val dateTime = new DateTime(date.getTime)
    val result = resultO.getOrElse(TemporalDensityIteratorResult())
    addResultDate(dateTime, result.timeSeries)
    result
  }

  /** take a given Coordinate and add 1 to the result time that it corresponds to via the snap time */
  def addResultDate(date: DateTime, result: TimeSeries): Unit = {
    val t: DateTime = snap.t(snap.i(date))
    val cur: Long = result.getOrElse(t, 0L)
    result.put(t, cur + 1L)
  }
}

@deprecated
object TemporalDensityIterator extends Logging {

  val INTERVAL_KEY = "geomesa.temporal.density.bounds"
  val BUCKETS_KEY = "geomesa.temporal.density.buckets"

  val geomFactory = JTSFactoryFinder.getGeometryFactory

  def configure(cfg: IteratorSetting, interval : Interval, buckets: Int) = {
    setTimeBounds(cfg, interval)
    setBuckets(cfg, buckets)
  }

  def setTimeBounds(iterSettings: IteratorSetting, interval: Interval) : Unit = {
    iterSettings.addOption(INTERVAL_KEY,  s"${interval.getStart.getMillis},${interval.getEnd.getMillis}")
  }

  def setBuckets(iterSettings: IteratorSetting, buckets: Int): Unit = {
    iterSettings.addOption(BUCKETS_KEY, s"$buckets")
  }

  def getBuckets(options: JMap[String, String]): Int = {
    options.get(BUCKETS_KEY).toInt
  }

  def getTimeBounds(options: JMap[String, String]): Interval = {
    val Array(s, e) = options.get(INTERVAL_KEY).split(",").map(_.toLong)
    new Interval(s, e)
  }
}

@deprecated
case class TemporalDensityIteratorResult(timeSeries: TimeSeries = new mutable.HashMap[DateTime, Long]) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit =
    featureBuilder.add(KryoLazyTemporalDensityIterator.encodeTimeSeries(timeSeries))
}
