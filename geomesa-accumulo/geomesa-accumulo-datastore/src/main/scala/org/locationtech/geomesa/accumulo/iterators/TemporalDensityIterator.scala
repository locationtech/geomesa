/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Date, HashMap => JHMap, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.index.getDtgFieldName
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.TimeSeries
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypes, TimeSnap}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.{breakOut, mutable}
import scala.util.parsing.json.JSONObject

class TemporalDensityIterator(other: TemporalDensityIterator, env: IteratorEnvironment)
  extends FeatureAggregatingIterator[TemporalDensityIteratorResult](other, env) {

  import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.{TEMPORAL_DENSITY_FEATURE_SFT_STRING, TimeSeries}

  var snap: TimeSnap = null
  var dateTimeFieldName: String = null

  projectedSFTDef = TEMPORAL_DENSITY_FEATURE_SFT_STRING

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                         options: JMap[String, String],
                                                         env: IteratorEnvironment): Unit = {

    dateTimeFieldName = getDtgFieldName(simpleFeatureType).getOrElse ( throw new IllegalArgumentException("dtg field required"))

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

object TemporalDensityIterator extends Logging {

  val INTERVAL_KEY = "geomesa.temporal.density.bounds"
  val BUCKETS_KEY = "geomesa.temporal.density.buckets"
  val TIME_SERIES: String = "timeseries"
  val TEMPORAL_DENSITY_FEATURE_SFT_STRING = s"$TIME_SERIES:String,geom:Geometry"

  type TimeSeries = collection.mutable.HashMap[DateTime, Long]

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

  def createFeatureType(origFeatureType: SimpleFeatureType) = {
    //Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(origFeatureType.getTypeName)
    val outNamespace = if (namespace == null){
        "NullNamespace"
      } else {
        namespace
      }
    SimpleFeatureTypes.createType(outNamespace, name, TemporalDensityIterator.TEMPORAL_DENSITY_FEATURE_SFT_STRING)
  }

  def combineTimeSeries(ts1: TimeSeries, ts2: TimeSeries) : TimeSeries = {
    val resultTS = new collection.mutable.HashMap[DateTime, Long]()
    for (key <- ts1.keySet ++ ts2.keySet) {
      resultTS.put(key, ts1.getOrElse(key, 0L) + ts2.getOrElse(key,0L))
    }
    resultTS
  }

  private val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

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
}

case class TemporalDensityIteratorResult(timeSeries: TimeSeries = new mutable.HashMap[DateTime, Long]) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit =
    featureBuilder.add(TemporalDensityIterator.encodeTimeSeries(timeSeries))
}
