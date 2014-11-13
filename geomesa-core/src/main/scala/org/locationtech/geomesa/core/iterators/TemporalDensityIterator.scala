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


package org.locationtech.geomesa.core.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Date
import java.{util => ju}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => ARange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, Interval}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.{FeatureEncoding, SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.index.{IndexEntryDecoder, _}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypes, TimeSnap}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random

class TemporalDensityIterator(other: TemporalDensityIterator, env: IteratorEnvironment) extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.core.iterators.TemporalDensityIterator.{TEMPORAL_DENSITY_FEATURE_STRING, TimeSeries}

  var curRange: ARange = null
  var result: TimeSeries = new collection.mutable.HashMap[DateTime, Long]()
  var projectedSFT: SimpleFeatureType = null
  var featureBuilder: SimpleFeatureBuilder = null
  var snap: TimeSnap = null
  var topTemporalDensityKey: Option[Key] = None
  var topTemporalDensityValue: Option[Value] = None
  protected var decoder: IndexEntryDecoder = null

  var simpleFeatureType: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key,Value] = null

  var topSourceKey: Key = null
  var topSourceValue: Value = null
  var originalDecoder: SimpleFeatureDecoder = null
  var temporalDensityFeatureEncoder: SimpleFeatureEncoder = null

  var dateTimeFieldName: String = null

  def this() = this(null, null)

  def init(source: SortedKeyValueIterator[Key, Value],
                    options: ju.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
    simpleFeatureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    simpleFeatureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dateTimeFieldName = getDtgFieldName(simpleFeatureType).getOrElse ( throw new IllegalArgumentException("dtg field required"))

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    originalDecoder = SimpleFeatureDecoder(simpleFeatureType, encodingOpt)

    projectedSFT = SimpleFeatureTypes.createType(simpleFeatureType.getTypeName, TEMPORAL_DENSITY_FEATURE_STRING)

    temporalDensityFeatureEncoder = SimpleFeatureEncoder(projectedSFT, encodingOpt)
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)

    val buckets = TemporalDensityIterator.getBuckets(options)
    val bounds = TemporalDensityIterator.getTimeBounds(options)
    snap = new TimeSnap(bounds, buckets)

  }

  /**
   * Combines the results from the underlying iterator stack
   * into a single feature
   */
  def findTop() = {
    // reset our 'top' (current) variables
    result.clear()
    topSourceKey = null
    topSourceValue = null

    while(source.hasTop && !curRange.afterEndKey(source.getTopKey)) {
      topSourceKey = source.getTopKey
      topSourceValue = source.getTopValue //SimpleFeature

      val date = originalDecoder.decode(topSourceValue).getAttribute(dateTimeFieldName).asInstanceOf[Date]
      val dateTime = new DateTime(date.getTime)
      addResultDate(dateTime)

      source.next()
    }

    if(topSourceKey != null) {
      featureBuilder.reset()
      featureBuilder.add(TemporalDensityIterator.encodeTimeSeries(result))
      featureBuilder.add(TemporalDensityIterator.zeroPoint) //Filler value as Feature requires a geometry
      val feature = featureBuilder.buildFeature(Random.nextString(6))
      topTemporalDensityKey = Some(topSourceKey)
      topTemporalDensityValue = Some(new Value(temporalDensityFeatureEncoder.encode(feature)))
    }
  }

  /** take a given Coordinate and add 1 to the result time that it corresponds to via the snap time */
  def addResultDate(date: DateTime) = {
    val t: DateTime = snap.t(snap.i(date))
    val cur: Long = result.get(t).getOrElse(0L)
    result.put(t, cur + 1L)
  }

  override def seek(range: ARange,
                    columnFamilies: ju.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {
    curRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  def hasTop: Boolean = topTemporalDensityKey.nonEmpty

  def getTopKey: Key = topTemporalDensityKey.orNull

  def getTopValue = topTemporalDensityValue.orNull

  def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = new TemporalDensityIterator(this, env)

  def next(): Unit = if(!source.hasTop) {
    topTemporalDensityKey = None
    topTemporalDensityValue = None
  } else {
    findTop()
  }
}

object TemporalDensityIterator extends Logging {

  val INTERVAL_KEY = "geomesa.temporal.density.bounds"
  val BUCKETS_KEY = "geomesa.temporal.density.buckets"
  val ENCODED_TIME_SERIES: String = "timeseries"
  val TEMPORAL_DENSITY_FEATURE_STRING = s"$ENCODED_TIME_SERIES:String,geom:Geometry"

  val zeroPoint = new GeometryFactory().createPoint(new Coordinate(0,0))

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

  def getBuckets(options: ju.Map[String, String]): Int = {
    options.get(BUCKETS_KEY).toInt
  }

  def getTimeBounds(options: ju.Map[String, String]): Interval = {
    val Array(s, e) = options.get(INTERVAL_KEY).split(",").map(_.toLong)
    new Interval(s, e)
  }

  def combineTimeSeries(ts1: TimeSeries, ts2: TimeSeries) : TimeSeries = {
    val resultTS = new collection.mutable.HashMap[DateTime, Long]()
    for (key <- ts1.keySet ++ ts2.keySet) {
      resultTS.put(key, ts1.getOrElse(key, 0L) + ts2.getOrElse(key,0L))
    }
    resultTS
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
      val dateIdx = new DateTime(is.readLong())
      val weight = is.readLong()
      table.put(dateIdx, weight)
    }
    table
  }
}
