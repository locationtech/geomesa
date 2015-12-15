/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Map.Entry
import java.util.{Date, Map => jMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureDeserializers}
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction, ExtendedValues}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator extends KryoLazyAggregatingIterator[ByteBufferResult] with Logging {

  import BinAggregatingIterator._

  var trackIndex: Int = -1
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var labelIndex: Int = -1
  var binSize: Int = 16
  var sort: Boolean = false

  var batchSize: Int = -1
  val result: ByteBufferResult = new ByteBufferResult(null)

  var writeBin: (KryoBufferSimpleFeature, ByteBuffer) => Unit = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)
    val options = jOptions.asScala

    geomIndex = options(GEOM_OPT).toInt
    dtgIndex = options(DATE_OPT).toInt
    trackIndex = options(TRACK_OPT).toInt
    labelIndex = options.get(LABEL_OPT).map(_.toInt).getOrElse(-1)

    binSize = if (labelIndex == -1) 16 else 24
    sort = options(SORT_OPT).toBoolean

    batchSize = options(BATCH_SIZE_OPT).toInt * binSize
    // avoid re-allocating the buffer if possible
    if (result.buffer == null || result.buffer.capacity != batchSize) {
      result.buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    }

    // derive the bin values from the features
    writeBin = if (sft.isPoints) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else if (sft.isLines) {
      if (labelIndex == -1) writeLineString else writeLineStringWithLabel
    } else {
      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
    }
  }

  override def notFull(result: ByteBufferResult): Boolean = result.buffer.position < batchSize

  override def newResult() = result

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writeBin(sf.asInstanceOf[KryoBufferSimpleFeature], result.buffer)

  override def encodeResult(result: ByteBufferResult): Array[Byte] = {
    val bytes = result.buffer.array()
    if (sort) {
      BinSorter.quickSort(bytes, 0, result.buffer.position - binSize, binSize)
    }
    if (result.buffer.position == batchSize) {
      // use the existing buffer if possible
      bytes
    } else {
      // if not, we have to copy it - values do not allow you to specify a valid range
      val copy = Array.ofDim[Byte](result.buffer.position)
      System.arraycopy(bytes, 0, copy, 0, result.buffer.position)
      copy
    }
  }

  /**
   * Writes a point to our buffer in the bin format
   */
  private def writeBinToBuffer(sf: KryoBufferSimpleFeature, pt: Point, byteBuffer: ByteBuffer): Unit = {
    val track = sf.getAttribute(trackIndex)
    if (track == null) {
      byteBuffer.putInt(0)
    } else {
      byteBuffer.putInt(track.hashCode())
    }
    val dtg = if (dtgIndex == -1) 0L else sf.getDateAsLong(dtgIndex)
    byteBuffer.putInt((dtg / 1000).toInt)
    byteBuffer.putFloat(pt.getY.toFloat) // y is lat
    byteBuffer.putFloat(pt.getX.toFloat) // x is lon
  }

  /**
   * Writes a label to the buffer in the bin format
   */
  private def writeLabelToBuffer(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit = {
    val label = sf.getAttribute(labelIndex)
    byteBuffer.putLong(if (label == null) 0L else Convert2ViewerFunction.convertToLabel(label.toString))
  }

  /**
   * Writes a bin record from a feature that has a point geometry
   */
  def writePoint(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit =
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Point], byteBuffer)

  /**
   * Writes point + label
   */
  def writePointWithLabel(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit = {
    writePoint(sf, byteBuffer)
    writeLabelToBuffer(sf, byteBuffer)
  }

  /**
   * Writes bins record from a feature that has a line string geometry.
   * The feature will be multiple bin records.
   */
  def writeLineString(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(i), byteBuffer)
      i += 1
    }
  }

  /**
   * Writes line string + label
   */
  def writeLineStringWithLabel(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(i), byteBuffer)
      writeLabelToBuffer(sf, byteBuffer)
      i += 1
    }
  }

  /**
   * Writes a bin record from a feature that has a arbitrary geometry.
   * A single internal point will be written.
   */
  def writeGeometry(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit =
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].getCentroid, byteBuffer)

  /**
   * Writes geom + label
   */
  def writeGeometryWithLabel(sf: KryoBufferSimpleFeature, byteBuffer: ByteBuffer): Unit = {
    writeGeometry(sf, byteBuffer)
    writeLabelToBuffer(sf, byteBuffer)
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

/**
 * Aggregates bins that have already been computed as accumulo data values
 */
class PrecomputedBinAggregatingIterator extends BinAggregatingIterator {

  var decodeBin: (Array[Byte]) => SimpleFeature = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)
    val options = jOptions.asScala

    val filt = options.contains(KryoLazyAggregatingIterator.CQL_OPT)
    val dedupe = options.contains(KryoLazyAggregatingIterator.DUPE_OPT)

    val sf = new ScalaSimpleFeature("", sft)
    val gf = new GeometryFactory
    val getId = Z3Table.getIdFromRow(sft) // NOTE: Z3 is the only table we use precomputed values for

    // we only need to decode the parts required for the filter/dedupe check
    decodeBin = (filt, dedupe) match {
      case (false, false) => (_) => null
      case (false, true)  => (_) => {
        sf.getIdentifier.setID(getId(source.getTopKey.getRow.getBytes))
        sf
      }
      case (true, _) => (_) => {
        sf.getIdentifier.setID(getId(source.getTopKey.getRow.getBytes))
        setValuesFromBin(sf, gf)
        sf
      }
    }
  }

  override def decode(value: Array[Byte]): SimpleFeature = decodeBin(value)

  // we are using the pre-computed bin values - we can copy the value directly into our buffer
  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    result.buffer.put(source.getTopValue.get)

  /**
   * Writes a bin record into a simple feature for filtering
   */
  def setValuesFromBin(sf: ScalaSimpleFeature, gf: GeometryFactory): Unit = {
    val values = Convert2ViewerFunction.decode(source.getTopValue.get)
    sf.setAttribute(geomIndex, gf.createPoint(new Coordinate(values.lat, values.lon)))
    sf.setAttribute(trackIndex, values.trackId)
    sf.setAttribute(dtgIndex, new Date(values.dtg))
  }
}

// wrapper for java's byte buffer that adds scala methods for the aggregating iterator
class ByteBufferResult(var buffer: ByteBuffer) {
  def isEmpty: Boolean = buffer.position == 0
  def clear(): Unit = buffer.clear()
}

object BinAggregatingIterator extends Logging {

  // need to be lazy to avoid class loading issues before init is called
  lazy val BIN_SFT = SimpleFeatureTypes.createType("bin", "bin:String,*geom:Point:srid=4326")
  val BIN_ATTRIBUTE_INDEX = 0 // index of 'bin' attribute in BIN_SFT
  private lazy val zeroPoint = WKTUtils.read("POINT(0 0)")

  val DEFAULT_PRIORITY = 25

  // configuration keys
  private val BATCH_SIZE_OPT = "batch"
  private val SORT_OPT       = "sort"

  private val TRACK_OPT      = "track"
  private val GEOM_OPT       = "geom"
  private val DATE_OPT       = "dtg"
  private val LABEL_OPT      = "label"

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configurePrecomputed(sft: SimpleFeatureType,
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
        val is = configure(classOf[PrecomputedBinAggregatingIterator], sft, filter, trackId,
          geom, dtg, None, batch , sort, deduplicate, priority)
        is
      case None => throw new RuntimeException(s"No default trackId field found in SFT $sft")
    }
  }

  /**
   * Configure based on query hints
   */
  def configureDynamic(sft: SimpleFeatureType,
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

    configure(classOf[BinAggregatingIterator], sft, filter, trackId, geom, dtg,
      label, batchSize, sort, deduplicate, priority)
  }

  /**
   * Creates an iterator config that will operate on regular kryo encoded entries
   */
  private def configure(clas: Class[_ <: BinAggregatingIterator],
                        sft: SimpleFeatureType,
                        filter: Option[Filter],
                        trackId: String,
                        geom: String,
                        dtg: Option[String],
                        label: Option[String],
                        batchSize: Int,
                        sort: Boolean,
                        deduplicate: Boolean,
                        priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "bin-iter", clas)
    KryoLazyAggregatingIterator.configure(is, sft, filter, deduplicate, None)
    is.addOption(BATCH_SIZE_OPT, batchSize.toString)
    is.addOption(TRACK_OPT, sft.indexOf(trackId).toString)
    is.addOption(GEOM_OPT, sft.indexOf(geom).toString)
    is.addOption(DATE_OPT, dtg.map(sft.indexOf).getOrElse(-1).toString)
    label.foreach(l => is.addOption(LABEL_OPT, sft.indexOf(l).toString))
    is.addOption(SORT_OPT, sort.toString)
    is
  }

  /**
   * Determines if the requested fields match the precomputed bin data
   */
  def canUsePrecomputedBins(sft: SimpleFeatureType, hints: Hints): Boolean = {
    sft.getBinTrackId.exists(_ == hints.getBinTrackIdField) &&
        hints.getBinGeomField.forall(_ == sft.getGeomField) &&
        hints.getBinDtgField == sft.getDtgField &&
        hints.getBinLabelField.isEmpty
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): FeatureFunction = {
    val sf = new ScalaSimpleFeature("", BIN_SFT)
    sf.setAttribute(1, zeroPoint)
    (e: Entry[Key, Value]) => {
      // set the value directly in the array, as we don't support byte arrays as properties
      // TODO GEOMESA-823 support byte arrays natively
      sf.values(BIN_ATTRIBUTE_INDEX) = e.getValue.get()
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
                                 hints: Hints,
                                 serializationType: SerializationType): FeatureFunction = {

    import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints

    // don't use return sft from query hints, as it will be bin_sft
    val returnSft = hints.getTransformSchema.getOrElse(sft)
    val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
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

    (e: Entry[Key, Value]) => {
      val deserialized = deserializer.deserialize(e.getValue.get())
      // set the value directly in the array, as we don't support byte arrays as properties
      new ScalaSimpleFeature(deserialized.getID, BIN_SFT, Array(encode(deserialized), zeroPoint))
    }
  }

  private def getTrack(sf: SimpleFeature, i: Int): String = {
    val t = sf.getAttribute(i)
    if (t == null) "" else t.toString
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
    val p = sf.getAttribute(i).asInstanceOf[Geometry].getCentroid
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

/**
 * Sorts aggregated bin arrays
 */
object BinSorter extends Logging {

  /**
   * If the length of an array to be sorted is less than this
   * constant, insertion sort is used in preference to Quicksort.
   *
   * This length is 'logical' length, so the array is really binSize * length
   */
  private val INSERTION_SORT_THRESHOLD = 3

  private val swapBuffers = new ThreadLocal[Array[Byte]]() {
    override def initialValue() = Array.ofDim[Byte](24) // the larger bin size
  }

  private val priorityOrdering = new Ordering[(Array[Byte], Int)]() {
    override def compare(x: (Array[Byte], Int), y: (Array[Byte], Int)) =
      BinSorter.compare(y._1, y._2, x._1, x._2) // reverse for priority queue
  }

  /**
   * Compares two bin chunks by date
   */
  def compare(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int =
    compareIntLittleEndian(left, leftOffset + 4, right, rightOffset + 4) // offset + 4 is dtg

  /**
   * Comparison based on the integer encoding used by ByteBuffer
   * original code is in private/protected java.nio packages
   */
  private def compareIntLittleEndian(left: Array[Byte],
                                     leftOffset: Int,
                                     right: Array[Byte],
                                     rightOffset: Int): Int = {
    val l3 = left(leftOffset + 3)
    val r3 = right(rightOffset + 3)
    if (l3 < r3) {
      return -1
    } else if (l3 > r3) {
      return 1
    }
    val l2 = left(leftOffset + 2) & 0xff
    val r2 = right(rightOffset + 2) & 0xff
    if (l2 < r2) {
      return -1
    } else if (l2 > r2) {
      return 1
    }
    val l1 = left(leftOffset + 1) & 0xff
    val r1 = right(rightOffset + 1) & 0xff
    if (l1 < r1) {
      return -1
    } else if (l1 > r1) {
      return 1
    }
    val l0 = left(leftOffset) & 0xff
    val r0 = right(rightOffset) & 0xff
    if (l0 == r0) {
      0
    } else if (l0 < r0) {
      -1
    } else {
      1
    }
  }

  /**
   * Takes a sequence of (already sorted) aggregates and combines them in a final sort. Uses
   * a priority queue to compare the head element across each aggregate.
   */
  def mergeSort(aggregates: Iterator[Array[Byte]], binSize: Int): Iterator[(Array[Byte], Int)] = {
    if (aggregates.isEmpty) {
      return Iterator.empty
    }
    val queue = new scala.collection.mutable.PriorityQueue[(Array[Byte], Int)]()(priorityOrdering)
    val sizes = scala.collection.mutable.ArrayBuffer.empty[Int]
    while (aggregates.hasNext) {
      val next = aggregates.next()
      sizes.append(next.length / binSize)
      queue.enqueue((next, 0))
    }

    logger.debug(s"Got back ${queue.length} aggregates with an average size of ${sizes.sum / sizes.length}" +
        s" chunks and a median size of ${sizes.sorted.apply(sizes.length / 2)} chunks")

    new Iterator[(Array[Byte], Int)] {
      override def hasNext = queue.nonEmpty
      override def next() = {
        val (aggregate, offset) = queue.dequeue()
        if (offset < aggregate.length - binSize) {
          queue.enqueue((aggregate, offset + binSize))
        }
        (aggregate, offset)
      }
    }
  }

  /**
   * Performs a merge sort into a new byte array
   */
  def mergeSort(left: Array[Byte], right: Array[Byte], binSize: Int): Array[Byte] = {
    if (left.length == 0) {
      return right
    } else if (right.length == 0) {
      return left
    }
    val result = Array.ofDim[Byte](left.length + right.length)
    var (leftIndex, rightIndex, resultIndex) = (0, 0, 0)

    while (leftIndex < left.length && rightIndex < right.length) {
      if (compare(left, leftIndex, right, rightIndex) > 0) {
        System.arraycopy(right, rightIndex, result, resultIndex, binSize)
        rightIndex += binSize
      } else {
        System.arraycopy(left, leftIndex, result, resultIndex, binSize)
        leftIndex += binSize
      }
      resultIndex += binSize
    }
    while (leftIndex < left.length) {
      System.arraycopy(left, leftIndex, result, resultIndex, binSize)
      leftIndex += binSize
      resultIndex += binSize
    }
    while (rightIndex < right.length) {
      System.arraycopy(right, rightIndex, result, resultIndex, binSize)
      rightIndex += binSize
      resultIndex += binSize
    }
    result
  }

  /**
   * Sorts the specified range of the array by Dual-Pivot Quicksort.
   * Modified version of java's DualPivotQuicksort
   *
   * @param bytes the array to be sorted
   * @param left the index of the first element, inclusive, to be sorted
   * @param right the index of the last element, inclusive, to be sorted
   */
  def quickSort(bytes: Array[Byte], left: Int, right: Int, binSize: Int): Unit =
    quickSort(bytes, left, right, binSize, leftmost = true)

  /**
   * Optimized for non-leftmost insertion sort
   */
  def quickSort(bytes: Array[Byte], left: Int, right: Int, binSize: Int, leftmost: Boolean): Unit = {

    val length = (right + binSize - left) / binSize

    if (length < INSERTION_SORT_THRESHOLD) {
      // Use insertion sort on tiny arrays
      if (leftmost) {
        // Traditional (without sentinel) insertion sort is used in case of the leftmost part
        var i = left + binSize
        while (i <= right) {
          var j = i
          val ai = getThreadLocalChunk(bytes, i, binSize)
          while (j > left && compare(bytes, j - binSize, ai, 0) > 0) {
            System.arraycopy(bytes, j - binSize, bytes, j, binSize)
            j -= binSize
          }
          if (j != i) {
            // we don't need to copy if nothing moved
            System.arraycopy(ai, 0, bytes, j, binSize)
          }
          i += binSize
        }
      } else {
        // optimized insertions sort when we know we have 'sentinel' elements to the left
        /*
         * Every element from adjoining part plays the role
         * of sentinel, therefore this allows us to avoid the
         * left range check on each iteration. Moreover, we use
         * the more optimized algorithm, so called pair insertion
         * sort, which is faster (in the context of Quicksort)
         * than traditional implementation of insertion sort.
         */
        // Skip the longest ascending sequence
        var i = left
        do {
          if (i >= right) {
            return
          }
        } while ({ i += binSize; compare(bytes, i , bytes, i - binSize) >= 0 })

        val a1 = Array.ofDim[Byte](binSize)
        val a2 = Array.ofDim[Byte](binSize)

        var k = i
        while ({ i += binSize; i } <= right) {
          if (compare(bytes, k, bytes, i) < 0) {
            System.arraycopy(bytes, k, a2, 0, binSize)
            System.arraycopy(bytes, i, a1, 0, binSize)
          } else {
            System.arraycopy(bytes, k, a1, 0, binSize)
            System.arraycopy(bytes, i, a2, 0, binSize)
          }
          while ({ k -= binSize; compare(a1, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + 2 * binSize, binSize)
          }
          k += binSize
          System.arraycopy(a1, 0, bytes, k + binSize, binSize)
          while ({ k -= binSize; compare(a2, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + binSize, binSize)
          }
          System.arraycopy(a2, 0, bytes, k + binSize, binSize)

          i += binSize
          k = i
        }

        var j = right
        val last = getThreadLocalChunk(bytes, j, binSize)
        while ({ j -= binSize; compare(last, 0, bytes, j) < 0 }) {
          System.arraycopy(bytes, j, bytes, j + binSize, binSize)
        }
        System.arraycopy(last, 0, bytes, j + binSize, binSize)
      }
      return
    }

    /*
     * Sort five evenly spaced elements around (and including) the
     * center element in the range. These elements will be used for
     * pivot selection as described below. The choice for spacing
     * these elements was empirically determined to work well on
     * a wide variety of inputs.
     */
    val seventh = (length / 7) * binSize

    val e3 = (((left + right) / binSize) / 2) * binSize // The midpoint
    val e2 = e3 - seventh
    val e1 = e2 - seventh
    val e4 = e3 + seventh
    val e5 = e4 + seventh

    def swap(left: Int, right: Int) = {
      val chunk = getThreadLocalChunk(bytes, left, binSize)
      System.arraycopy(bytes, right, bytes, left, binSize)
      System.arraycopy(chunk, 0, bytes, right, binSize)
    }

    // Sort these elements using insertion sort
    if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }

    if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
      if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
    }
    if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
      if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
        if (compare(bytes, e2, bytes, e1) < 0) {swap(e2, e1) }
      }
    }
    if (compare(bytes, e5, bytes, e4) < 0) { swap(e5, e4)
      if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
        if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
          if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
        }
      }
    }

    // Pointers
    var less  = left  // The index of the first element of center part
    var great = right // The index before the first element of right part

    if (compare(bytes, e1, bytes, e2) != 0 && compare(bytes, e2, bytes, e3) != 0 &&
        compare(bytes, e3, bytes, e4) != 0 && compare(bytes, e4, bytes, e5) != 0 ) {
      /*
       * Use the second and fourth of the five sorted elements as pivots.
       * These values are inexpensive approximations of the first and
       * second terciles of the array. Note that pivot1 <= pivot2.
       */
      val pivot1 = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e2, pivot1, 0, binSize)
      val pivot2 = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e4, pivot2, 0, binSize)

      /*
       * The first and the last elements to be sorted are moved to the
       * locations formerly occupied by the pivots. When partitioning
       * is complete, the pivots are swapped back into their final
       * positions, and excluded from subsequent sorting.
       */
      System.arraycopy(bytes, left, bytes, e2, binSize)
      System.arraycopy(bytes, right, bytes, e4, binSize)

      // Skip elements, which are less or greater than pivot values.
      while ({ less += binSize; compare(bytes, less, pivot1, 0) < 0 }) {}
      while ({ great -= binSize; compare(bytes, great, pivot2, 0) > 0 }) {}

      /*
       * Partitioning:
       *
       *   left part           center part                   right part
       * +--------------------------------------------------------------+
       * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
       * +--------------------------------------------------------------+
       *               ^                          ^       ^
       *               |                          |       |
       *              less                        k     great
       *
       * Invariants:
       *
       *              all in (left, less)   < pivot1
       *    pivot1 <= all in [less, k)     <= pivot2
       *              all in (great, right) > pivot2
       *
       * Pointer k is the first index of ?-part.
       */

      var k = less - binSize
      var loop = true
      while (loop && { k += binSize; k } <= great) {
        val ak = getThreadLocalChunk(bytes, k, binSize)
        if (compare(ak, 0, pivot1, 0) < 0) { // Move a[k] to left part
          System.arraycopy(bytes, less, bytes, k, binSize)
          System.arraycopy(ak, 0, bytes, less, binSize)
          less += binSize
        } else if (compare(ak, 0, pivot2, 0) > 0) { // Move a[k] to right part
          while (loop && compare(bytes, great, pivot2, 0) > 0) {
            if (great == k) {
              loop = false
            }
            great -= binSize
          }
          if (loop) {
            if (compare(bytes, great, pivot1, 0) < 0) { // a[great] <= pivot2
              System.arraycopy(bytes, less, bytes, k, binSize)
              System.arraycopy(bytes, great, bytes, less, binSize)
              less += binSize
            } else { // pivot1 <= a[great] <= pivot2
              System.arraycopy(bytes, great, bytes, k, binSize)
            }
            System.arraycopy(ak, 0, bytes, great, binSize)
            great -= binSize
          }
        }
      }

      // Swap pivots into their final positions
      System.arraycopy(bytes, less - binSize, bytes, left, binSize)
      System.arraycopy(pivot1, 0, bytes, less - binSize, binSize)
      System.arraycopy(bytes, great + binSize, bytes, right, binSize)
      System.arraycopy(pivot2, 0, bytes, great + binSize, binSize)

      // Sort left and right parts recursively, excluding known pivots
      quickSort(bytes, left, less - 2 * binSize, binSize, leftmost)
      quickSort(bytes, great + 2 * binSize, right, binSize, leftmost = false)

      /*
       * If center part is too large (comprises > 4/7 of the array),
       * swap internal pivot values to ends.
       */
      if (less < e1 && e5 < great) {

        // Skip elements, which are equal to pivot values.
        while (compare(bytes, less, pivot1, 0) == 0) { less += binSize }
        while (compare(bytes, great, pivot2, 0) == 0) { great -= binSize }

        /*
         * Partitioning:
         *
         *   left part         center part                  right part
         * +----------------------------------------------------------+
         * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
         * +----------------------------------------------------------+
         *              ^                        ^       ^
         *              |                        |       |
         *             less                      k     great
         *
         * Invariants:
         *
         *              all in (*,  less) == pivot1
         *     pivot1 < all in [less,  k)  < pivot2
         *              all in (great, *) == pivot2
         *
         * Pointer k is the first index of ?-part.
         */
        var k = less - binSize
        loop = true
        while (loop && { k += binSize; k } <= great) {
          val ak = getThreadLocalChunk(bytes, k, binSize)
          if (compare(ak, 0, pivot1, 0) == 0) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, binSize)
            System.arraycopy(ak, 0, bytes, less, binSize)
            less += binSize
          } else if (compare(ak, 0, pivot2, 0) == 0) { // Move a[k] to right part
            while (loop && compare(bytes, great, pivot2, 0) == 0) {
              if (great == k) {
                loop = false
              }
              great -= binSize
            }
            if (loop) {
              if (compare(bytes, great, pivot1, 0) == 0) { // a[great] < pivot2
                System.arraycopy(bytes, less, bytes, k, binSize)
                System.arraycopy(bytes, great, bytes, less, binSize)
                less += binSize
              } else { // pivot1 < a[great] < pivot2
                System.arraycopy(bytes, great, bytes, k, binSize)
              }
              System.arraycopy(ak, 0, bytes, great, binSize)
              great -= binSize
            }
          }
        }
      }

      // Sort center part recursively
      quickSort(bytes, less, great, binSize, leftmost = false)
    } else { // Partitioning with one pivot

      /*
       * Use the third of the five sorted elements as pivot.
       * This value is inexpensive approximation of the median.
       */
      val pivot = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e3, pivot, 0, binSize)

      /*
       * Partitioning degenerates to the traditional 3-way
       * (or "Dutch National Flag") schema:
       *
       *   left part    center part              right part
       * +-------------------------------------------------+
       * |  < pivot  |   == pivot   |     ?    |  > pivot  |
       * +-------------------------------------------------+
       *              ^              ^        ^
       *              |              |        |
       *             less            k      great
       *
       * Invariants:
       *
       *   all in (left, less)   < pivot
       *   all in [less, k)     == pivot
       *   all in (great, right) > pivot
       *
       * Pointer k is the first index of ?-part.
       */
      var k = less
      var loop = true
      while (loop && k <= great) {
        val comp = compare(bytes, k, pivot, 0)
        if (comp != 0) {
          val ak = getThreadLocalChunk(bytes, k, binSize)
          if (comp < 0) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, binSize)
            System.arraycopy(ak, 0, bytes, less, binSize)
            less += binSize
          } else { // a[k] > pivot - Move a[k] to right part
            while (loop && compare(bytes, great, pivot, 0) > 0) {
              if (k == great) {
                loop = false
              }
              great -= binSize
            }
            if (loop) {
              if (compare(bytes, great, pivot, 0) < 0) { // a[great] <= pivot
                System.arraycopy(bytes, less, bytes, k, binSize)
                System.arraycopy(bytes, great, bytes, less, binSize)
                less += binSize
              } else { // a[great] == pivot
                System.arraycopy(bytes, great, bytes, k, binSize)
              }
              System.arraycopy(ak, 0, bytes, great, binSize)
              great -= binSize
            }
          }
        }
        k += binSize
      }

      /*
       * Sort left and right parts recursively.
       * All elements from center part are equal
       * and, therefore, already sorted.
       */
      quickSort(bytes, left, less - binSize, binSize, leftmost)
      quickSort(bytes, great + binSize, right, binSize, leftmost = false)
    }
  }

  // take care - uses thread-local state
  private def getThreadLocalChunk(bytes: Array[Byte], offset: Int, binSize: Int): Array[Byte] = {
    val chunk = swapBuffers.get()
    System.arraycopy(bytes, offset, chunk, 0, binSize)
    chunk
  }
}