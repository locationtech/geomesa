/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureDeserializers}
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.filter.function.{BasicValues, BinaryOutputEncoder, Convert2ViewerFunction, ExtendedValues}
import org.locationtech.geomesa.index.utils.SamplingIterator
import org.locationtech.geomesa.index.utils.bin.{BinAggregatingUtils, BinSorter, ByteBufferResult}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator
    extends KryoLazyAggregatingIterator[ByteBufferResult] with SamplingIterator with BinAggregatingUtils with LazyLogging {

  import BinAggregatingIterator._

  override def init(options: Map[String, String]): ByteBufferResult = initBinAggregation(options)

  override def notFull(result: ByteBufferResult): Boolean = result.buffer.position < result.buffer.limit

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writeBin(sf.asInstanceOf[KryoBufferSimpleFeature], result)

  override def encodeResult(result: ByteBufferResult): Array[Byte] = encodeBinAggregationResult(result)


  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError()

  override def isPoints: Boolean = sft.isPoints

  override def isLines: Boolean = sft.isLines
}

/**
 * Aggregates bins that have already been computed as accumulo data values
 */
class PrecomputedBinAggregatingIterator extends BinAggregatingIterator {

  var decodeBin: (Array[Byte]) => SimpleFeature = _
  var setDate: (SimpleFeature, Long) => Unit = _
  var writePrecomputedBin: (SimpleFeature, ByteBufferResult) => Unit = _

  override def init(options: Map[String, String]): ByteBufferResult = {
    import KryoLazyAggregatingIterator._

    val result = super.init(options)

    val filter = options.contains(CQL_OPT)
    val dedupe = options.contains(DUPE_OPT)
    val sample = options.contains(org.locationtech.geomesa.index.utils.SamplingIterator.SAMPLE_BY_OPT)

    val sf = new ScalaSimpleFeature("", sft)
    val gf = new GeometryFactory

    val index = try { AccumuloFeatureIndex.index(options(INDEX_OPT)) } catch {
      case NonFatal(_) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val getId = index.getIdFromRow(sft)

    // we only need to decode the parts required for the filter/dedupe/sampling check
    // note: we wouldn't be using precomputed if sample by field wasn't the track id
    decodeBin = if (filter) {
      setDate = if (isDtgArray) {
        (sf, long) => {
          val list = new java.util.ArrayList[Date](1)
          list.add(new Date(long))
          sf.setAttribute(dtgIndex, list)
        }
      } else {
        (sf, long) => sf.setAttribute(dtgIndex, new Date(long))
      }
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        setValuesFromBin(sf, gf)
        sf
      }
    } else if (sample && dedupe) {
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        setTrackIdFromBin(sf)
        sf
      }
    } else if (sample) {
      (_) => {
        setTrackIdFromBin(sf)
        sf
      }
    } else if (dedupe) {
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        sf
      }
    } else {
      (_) => null
    }

    // we are using the pre-computed bin values - we can copy the value directly into our buffer
    writePrecomputedBin = sampling match {
      case None => (_, result) => {
        val bytes = source.getTopValue.get
        result.ensureCapacity(bytes.length).put(bytes)
      }
      case Some(samp) => (sf, result) => if (samp(sf)) {
        val bytes = source.getTopValue.get
        result.ensureCapacity(bytes.length).put(bytes)
      }
    }

    result
  }

  override def decode(value: Array[Byte]): SimpleFeature = decodeBin(value)

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writePrecomputedBin(sf, result)

  /**
   * Writes a bin record into a simple feature for filtering
   */
  private def setValuesFromBin(sf: SimpleFeature, gf: GeometryFactory): Unit = {
    val values = Convert2ViewerFunction.decode(source.getTopValue.get)
    sf.setAttribute(geomIndex, gf.createPoint(new Coordinate(values.lat, values.lon)))
    sf.setAttribute(trackIndex, values.trackId)
    setDate(sf, values.dtg)
  }

  /**
   * Sets only the track id - used for sampling
   */
  private def setTrackIdFromBin(sf: SimpleFeature): Unit =
    sf.setAttribute(trackIndex, Convert2ViewerFunction.decode(source.getTopValue.get).trackId)

}

object BinAggregatingIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val zeroPoint = WKTUtils.read("POINT(0 0)")

  val DEFAULT_PRIORITY = 25

  // configuration keys
  private val BATCH_SIZE_OPT = "batch"
  private val SORT_OPT       = "sort"

  private val TRACK_OPT      = "track"
  private val GEOM_OPT       = "geom"
  private val DATE_OPT       = "dtg"
  private val LABEL_OPT      = "label"

  private val DATE_ARRAY_OPT = "dtg-array"

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configurePrecomputed(sft: SimpleFeatureType,
                           index: AccumuloFeatureIndexType,
                           filter: Option[Filter],
                           hints: Hints,
                           deduplicate: Boolean,
                           priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    sft.getBinTrackId match {
      case Some(trackId) =>
        val geom = sft.getGeomField
        val dtg = sft.getDtgField
        val batch = hints.getBinBatchSize
        val sort = hints.isBinSorting
        val sampling = hints.getSampling
        val is = configure(classOf[PrecomputedBinAggregatingIterator], sft, index, filter, trackId,
          geom, dtg, None, batch, sort, deduplicate, sampling, priority)
        is
      case None => throw new RuntimeException(s"No default trackId field found in SFT $sft")
    }
  }

  /**
   * Configure based on query hints
   */
  def configureDynamic(sft: SimpleFeatureType,
                       index: AccumuloFeatureIndexType,
                       filter: Option[Filter],
                       hints: Hints,
                       deduplicate: Boolean,
                       priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    val sampling = hints.getSampling

    configure(classOf[BinAggregatingIterator], sft, index, filter, trackId, geom, dtg,
      label, batchSize, sort, deduplicate, sampling, priority)
  }

  /**
   * Creates an iterator config that will operate on regular kryo encoded entries
   */
  private def configure(clas: Class[_ <: BinAggregatingIterator],
                        sft: SimpleFeatureType,
                        index: AccumuloFeatureIndexType,
                        filter: Option[Filter],
                        trackId: String,
                        geom: String,
                        dtg: Option[String],
                        label: Option[String],
                        batchSize: Int,
                        sort: Boolean,
                        deduplicate: Boolean,
                        sampling: Option[(Float, Option[String])],
                        priority: Int): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val is = new IteratorSetting(priority, "bin-iter", clas)
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(BATCH_SIZE_OPT, batchSize.toString)
    is.addOption(TRACK_OPT, sft.indexOf(trackId).toString)
    is.addOption(GEOM_OPT, sft.indexOf(geom).toString)
    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    is.addOption(DATE_OPT, dtgIndex.toString)
    if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
      is.addOption(DATE_ARRAY_OPT, "true")
    }
    label.foreach(l => is.addOption(LABEL_OPT, sft.indexOf(l).toString))
    is.addOption(SORT_OPT, sort.toString)
    sampling.foreach(SamplingIterator.configure(is, sft, _))
    is
  }

  /**
   * Determines if the requested fields match the precomputed bin data
   */
  def canUsePrecomputedBins(sft: SimpleFeatureType, hints: Hints): Boolean = {
    // noinspection ExistsEquals
    sft.getBinTrackId.exists(_ == hints.getBinTrackIdField) &&
        hints.getBinGeomField.forall(_ == sft.getGeomField) &&
        hints.getBinDtgField == sft.getDtgField &&
        hints.getBinLabelField.isEmpty &&
        hints.getSampleByField.forall(_ == hints.getBinTrackIdField)
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft)
    sf.setAttribute(1, zeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(BIN_ATTRIBUTE_INDEX, e.getValue.get())
      sf
    }
  }

  /**
   * Fallback for when we can't use the aggregating iterator (for example, if the features are avro encoded).
   * Instead, do bin conversion in client.
   *
   * Only encodes one bin (or one bin line) per feature
   */
  def nonAggregatedKvsToFeatures(sft: SimpleFeatureType,
                                 index: AccumuloFeatureIndex,
                                 hints: Hints,
                                 serializationType: SerializationType): (Entry[Key, Value]) => SimpleFeature = {

    // don't use return sft from query hints, as it will be bin_sft
    val returnSft = hints.getTransformSchema.getOrElse(sft)

    val trackIdIndex = returnSft.indexOf(hints.getBinTrackIdField)
    val geomIndex = hints.getBinGeomField.map(returnSft.indexOf).getOrElse(returnSft.getGeomIndex)
    val dtgIndex= hints.getBinDtgField.map(returnSft.indexOf).getOrElse(returnSft.getDtgIndex.get)
    val labelIndexOpt= hints.getBinLabelField.map(returnSft.indexOf)

    val isPoint = returnSft.isPoints
    val isLineString = !isPoint && returnSft.isLines

    val encode: (SimpleFeature) => Array[Byte] = labelIndexOpt match {
      case None if isPoint =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getPointGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
        }

      case None if isLineString =>
        val buf = new ByteArrayOutputStream()
        (sf) => {
          buf.reset()
          val trackId = getTrack(sf, trackIdIndex)
          val points = getLineGeom(sf, geomIndex)
          val dtgs = getLineDtg(sf, dtgIndex)
          if (points.length != dtgs.length) {
            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
          } else {
            var i = 0
            while (i < points.length) {
              val (lat, lon) = points(i)
              Convert2ViewerFunction.encode(BasicValues(lat, lon, dtgs(i), trackId), buf)
              i += 1
            }
          }
          buf.toByteArray
        }

      case None =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getGenericGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
        }

      case Some(lblIndex) if isPoint =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getPointGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
        }

      case Some(lblIndex) if isLineString =>
        val buf = new ByteArrayOutputStream()
        (sf) => {
          buf.reset()
          val trackId = getTrack(sf, trackIdIndex)
          val points = getLineGeom(sf, geomIndex)
          val dtgs = getLineDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          if (points.length != dtgs.length) {
            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
          } else {
            var i = 0
            while (i < points.length) {
              val (lat, lon) = points(i)
              Convert2ViewerFunction.encode(ExtendedValues(lat, lon, dtgs(i), trackId, label), buf)
              i += 1
            }
          }
          buf.toByteArray
        }

      case Some(lblIndex) =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getGenericGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
        }
    }

    if (index.serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        // set the value directly in the array, as we don't support byte arrays as properties
        new ScalaSimpleFeature(deserialized.getID, BinaryOutputEncoder.BinEncodedSft, Array(encode(deserialized), zeroPoint))
      }
    } else {
      val getId = index.getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType, SerializationOptions.withoutId)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val row = e.getKey.getRow
        deserialized.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
        // set the value directly in the array, as we don't support byte arrays as properties
        new ScalaSimpleFeature(deserialized.getID, BinaryOutputEncoder.BinEncodedSft, Array(encode(deserialized), zeroPoint))
      }
    }

  }

  private def getTrack(sf: SimpleFeature, i: Int): Int = {
    val t = sf.getAttribute(i)
    if (t == null) { 0 } else { t.hashCode }
  }

  // get a single geom
  private def getPointGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    val p = sf.getAttribute(i).asInstanceOf[Point]
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a line geometry as an array of points
  private def getLineGeom(sf: SimpleFeature, i: Int): Array[(Float, Float)] = {
    val geom = sf.getAttribute(i).asInstanceOf[LineString]
    (0 until geom.getNumPoints).map(geom.getPointN).map(p => (p.getY.toFloat, p.getX.toFloat)).toArray
  }

  // get a single geom
  private def getGenericGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val p = sf.getAttribute(i).asInstanceOf[Geometry].safeCentroid()
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a single date
  private def getDtg(sf: SimpleFeature, i: Int): Long = {
    val date = sf.getAttribute(i).asInstanceOf[Date]
    if (date == null) System.currentTimeMillis else date.getTime
  }

  // for line strings, we need an array of dates corresponding to the points in the line
  private def getLineDtg(sf: SimpleFeature, i: Int): Array[Long] = {
    import scala.collection.JavaConversions._
    val dates = sf.getAttribute(i).asInstanceOf[java.util.List[Date]]
    if (dates == null) Array.empty else dates.map(_.getTime).toArray
  }

  // get a label as a long
  private def getLabel(sf: SimpleFeature, i: Int): Long = {
    val lbl = sf.getAttribute(i)
    if (lbl == null) 0L else Convert2ViewerFunction.convertToLabel(lbl.toString)
  }
}
