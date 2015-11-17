/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{HashMap => JHMap, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
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
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.accumulo.iterators.RangeHistogramIterator.HistogramMap
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, RangeSnap, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.{breakOut, mutable}
import scala.util.parsing.json.JSONObject

class RangeHistogramIterator(other: RangeHistogramIterator, env: IteratorEnvironment)
  extends FeatureAggregatingIterator[RangeHistogramIteratorResult](other, env) {

  import org.locationtech.geomesa.accumulo.iterators.RangeHistogramIterator.{HistogramMap, RANGE_HISTOGRAM_SFT_STRING}

  var snap: RangeSnap = null
  var fieldName: String = null

  projectedSFTDef = RANGE_HISTOGRAM_SFT_STRING

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                         options: JMap[String, String],
                                                         env: IteratorEnvironment): Unit = {
    fieldName = RangeHistogramIterator.getAttribute(options)
    val buckets = RangeHistogramIterator.getBuckets(options)
    val bounds = RangeHistogramIterator.getBounds(options)
    snap = new RangeSnap(bounds, buckets)
  }

  override def handleKeyValue(resultO: Option[RangeHistogramIteratorResult],
                              topSourceKey: Key,
                              topSourceValue: Value): RangeHistogramIteratorResult = {
    val value = originalDecoder.deserialize(topSourceValue.get()).getAttribute(fieldName).asInstanceOf[java.lang.Long]
    val result = resultO.getOrElse(RangeHistogramIteratorResult())
    addValue(value, result.timeSeries)
    result
  }

  /** take a given Coordinate and add 1 to the result time that it corresponds to via the snap time */
  def addValue(value: Long, result: HistogramMap): Unit = {
    val bucketStep: Long = snap.getBucket(value)
    val cur: Long = result.getOrElse(bucketStep, 0L)
    result.put(bucketStep, cur + 1L)
  }
}

object RangeHistogramIterator extends Logging {
  val ATTRIBUTE_KEY = "geomesa.range.histogram.attribute"
  val INTERVAL_KEY = "geomesa.range.histogram.bounds"
  val BUCKETS_KEY = "geomesa.range.histogram.buckets"
  val HISTOGRAM_SERIES: String = "histogramseries"
  val RANGE_HISTOGRAM_SFT_STRING = s"$HISTOGRAM_SERIES:String,geom:Geometry"

  type HistogramMap = collection.mutable.HashMap[Long, Long]

  val geomFactory = JTSFactoryFinder.getGeometryFactory

  def configure(cfg: IteratorSetting, range: com.google.common.collect.Range[java.lang.Long], buckets: Int, attribute: String) = {
    setBounds(cfg, range)
    setBuckets(cfg, buckets)
    setAttribute(cfg, attribute)
  }

  def setBounds(iterSettings: IteratorSetting, range: com.google.common.collect.Range[java.lang.Long]): Unit = {
    iterSettings.addOption(INTERVAL_KEY, s"${range.lowerEndpoint},${range.upperEndpoint}")
  }

  def setBuckets(iterSettings: IteratorSetting, buckets: Int): Unit = {
    iterSettings.addOption(BUCKETS_KEY, s"$buckets")
  }

  def setAttribute(iterSettings: IteratorSetting, attribute: String): Unit = {
    iterSettings.addOption(ATTRIBUTE_KEY, attribute)
  }

  def getBounds(options: JMap[String, String]): com.google.common.collect.Range[java.lang.Long] = {
    val Array(s, e) = options.get(INTERVAL_KEY).split(",").map(_.toLong)
    com.google.common.collect.Ranges.closed(s, e)
  }

  def getBuckets(options: JMap[String, String]): Int = {
    options.get(BUCKETS_KEY).toInt
  }

  def getAttribute(options: JMap[String, String]): String = {
    options.get(ATTRIBUTE_KEY)
  }

  def createFeatureType(origFeatureType: SimpleFeatureType) = {
    //Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(origFeatureType.getTypeName)
    val outNamespace = if (namespace == null){
        "NullNamespace"
      } else {
        namespace
      }
    SimpleFeatureTypes.createType(outNamespace, name, RangeHistogramIterator.RANGE_HISTOGRAM_SFT_STRING)
  }

  def combineHistogramMaps(histogramMap1: HistogramMap, histogramMap2: HistogramMap) : HistogramMap = {
    val resultMap = new collection.mutable.HashMap[Long, Long]()
    for (key <- histogramMap1.keySet ++ histogramMap2.keySet) {
      resultMap.put(key, histogramMap1.getOrElse(key, 0L) + histogramMap2.getOrElse(key, 0L))
    }
    resultMap
  }

  private val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def histogramMapToJSON(histogramMap: HistogramMap): String = {
    val jsonMap = histogramMap.toMap.map { case (k, v) => k.toString -> v }
    new JSONObject(jsonMap).toString()
  }

  def jsonToHistogramMap(ts : String): HistogramMap = {
    val objMapper: ObjectMapper = new ObjectMapper()
    val stringMap: JHMap[String, Long] = objMapper.readValue(ts, new TypeReference[JHMap[String, java.lang.Long]]() {})
    (for((k, v) <- stringMap) yield k.toLong -> v)(breakOut)
  }

  def encodeHistogramMap(histogramMap: HistogramMap): String = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    for((key, count) <- histogramMap) {
      os.writeLong(key)
      os.writeLong(count)
    }
    os.flush()
    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def decodeHistogramMap(encoded: String): HistogramMap = {
    val bytes = Base64.decodeBase64(encoded)
    val is = new DataInputStream(new ByteArrayInputStream(bytes))
    val table = new collection.mutable.HashMap[Long, Long]()
    while(is.available() > 0) {
      table.put(is.readLong(), is.readLong())
    }
    table
  }

  def reduceFeatures(features: SFIter, query: Query): SFIter = {
    val encode = query.getHints.containsKey(RETURN_ENCODED)
    val sft = query.getHints.getReturnSft

    val histogramMapStrings = features.map(f => decodeHistogramMap(f.getAttribute(HISTOGRAM_SERIES).toString))
    val summedHistogramMaps = histogramMapStrings.reduceOption(combineHistogramMaps)

    val feature = summedHistogramMaps.map { sum =>
      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(sft)
      if (encode) {
        featureBuilder.add(RangeHistogramIterator.encodeHistogramMap(sum))
      } else {
        featureBuilder.add(histogramMapToJSON(sum))
      }
      featureBuilder.add(GeometryUtils.zeroPoint) // Filler value as Feature requires a geometry
      featureBuilder.buildFeature(null)
    }

    feature.iterator
  }
}

case class RangeHistogramIteratorResult(timeSeries: HistogramMap = new mutable.HashMap[Long, Long]) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit =
    featureBuilder.add(RangeHistogramIterator.encodeHistogramMap(timeSeries))
}
