/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Date, HashMap => JHMap, Map => JMap, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.TimeSeries
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes, TimeSnap}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.{breakOut, mutable}
import scala.util.parsing.json.JSONObject

@deprecated
class TemporalDensityIterator(other: FeatureAggregatingIterator[TemporalDensityIteratorResult], env: IteratorEnvironment)
  extends FeatureAggregatingIterator[TemporalDensityIteratorResult](other, env) {

  var snap: TimeSnap = null
  var dateTimeFieldName: String = null

  projectedSFTDef = TemporalDensityIterator.TEMPORAL_DENSITY_SFT_STRING

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
object TemporalDensityIterator extends LazyLogging {

  type TimeSeries = mutable.Map[DateTime, Long]

  val TEMPORAL_DENSITY_SFT_STRING = s"timeseries:String,*geom:Point:srid=4326"
  val DEFAULT_PRIORITY = 30

  val INTERVAL_KEY = "geomesa.temporal.density.bounds"
  val BUCKETS_KEY = "geomesa.temporal.density.buckets"

  val geomFactory = JTSFactoryFinder.getGeometryFactory
  private val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

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

  def createFeatureType(baseType: SimpleFeatureType) = {
    // Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(baseType.getTypeName)
    val outNamespace = if (namespace == null) "NullNamespace" else namespace
    SimpleFeatureTypes.createType(outNamespace, name, TEMPORAL_DENSITY_SFT_STRING)
  }

  def timeSeriesToJSON(ts : TimeSeries): String = {
    val jsonMap = ts.toMap.map { case (k, v) => k.toString(df) -> v }
    new JSONObject(jsonMap).toString()
  }

  def jsonToTimeSeries(ts : String): TimeSeries = {
    val objMapper: ObjectMapper = new ObjectMapper()
    val stringMap: JHMap[String, Long] = objMapper.readValue(ts, new TypeReference[JHMap[String, java.lang.Long]]() {})
    (for((k,v) <- stringMap) yield df.parseDateTime(k) -> v)(breakOut)
  }

  def encodeTimeSeries(timeSeries: TimeSeries): String = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    for((date,count) <- timeSeries) {
      os.writeLong(date.getMillis)
      os.writeLong(count)
    }
    os.flush()
    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def decodeTimeSeries(encoded: String): TimeSeries = {
    val bytes = Base64.decodeBase64(encoded)
    val is = new DataInputStream(new ByteArrayInputStream(bytes))
    val table = new collection.mutable.HashMap[DateTime, Long]()
    while(is.available() > 0) {
      val dateIdx = new DateTime(is.readLong(), DateTimeZone.UTC)
      val weight = is.readLong()
      table.put(dateIdx, weight)
    }
    table
  }

  def combineTimeSeries(ts1: TimeSeries, ts2: TimeSeries) : TimeSeries = {
    val resultTS = new collection.mutable.HashMap[DateTime, Long]()
    (ts1.keySet ++ ts2.keySet).foreach { key =>
      resultTS.put(key, ts1.getOrElse(key, 0L) + ts2.getOrElse(key, 0L))
    }
    resultTS
  }

  def reduceTemporalFeatures(features: SFIter, query: Query): SFIter = {
    val encode = query.getHints.containsKey(RETURN_ENCODED)
    val sft = query.getHints.getReturnSft

    val timeSeriesStrings = features.map(f => decodeTimeSeries(f.getAttribute(0).asInstanceOf[String]))
    val summedTimeSeries = timeSeriesStrings.reduceOption(combineTimeSeries)

    summedTimeSeries.iterator.map { sum =>
      val time = if (encode) encodeTimeSeries(sum) else timeSeriesToJSON(sum)
      new ScalaSimpleFeature(UUID.randomUUID().toString, sft, Array(time, GeometryUtils.zeroPoint))
    }
  }
}

@deprecated
case class TemporalDensityIteratorResult(timeSeries: TimeSeries = new mutable.HashMap[DateTime, Long]) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit =
    featureBuilder.add(TemporalDensityIterator.encodeTimeSeries(timeSeries))
}
