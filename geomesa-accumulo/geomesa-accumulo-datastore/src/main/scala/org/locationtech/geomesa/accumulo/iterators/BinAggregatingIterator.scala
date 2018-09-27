/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Date
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureDeserializers}
import org.locationtech.geomesa.index.iterators.{BinAggregatingScan, ByteBufferResult, SamplingIterator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodingOptions}
import org.locationtech.geomesa.utils.bin.{BinaryOutputCallback, BinaryOutputEncoder}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator
    extends BaseAggregatingIterator[ByteBufferResult] with BinAggregatingScan {
  override def notFull(result: ByteBufferResult): Boolean = result.buffer.position < result.buffer.limit
}

/**
 * Aggregates bins that have already been computed as accumulo data values
 */
class PrecomputedBinAggregatingIterator extends BinAggregatingIterator {

  var decodeBin: (Array[Byte]) => SimpleFeature = _
  var setDate: (Long) => Unit = _
  var writePrecomputedBin: (SimpleFeature, ByteBufferResult) => Unit = _

  override protected def initResult(sft: SimpleFeatureType,
                                    transform: Option[SimpleFeatureType],
                                    options: Map[String, String]): ByteBufferResult = {

    import KryoLazyAggregatingIterator._

    val result = super.initResult(sft, transform, options)

    val filter = options.contains(CQL_OPT)
    val dedupe = options.contains(DUPE_OPT)
    val sample = options.contains(SamplingIterator.Configuration.SampleByOpt)

    val sf = new ScalaSimpleFeature(sft, "")
    val gf = new GeometryFactory

    val index = try { AccumuloFeatureIndex.index(options(INDEX_OPT)) } catch {
      case NonFatal(_) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val getId = index.getIdFromRow(sft)

    // we only need to decode the parts required for the filter/dedupe/sampling check
    // note: we wouldn't be using precomputed if sample by field wasn't the track id
    decodeBin = if (filter) {
      val dtgIndex = encoding.dtgField.get
      val geomIndex = encoding.geomField.get
      val trackIndex = encoding.trackIdField.get
      setDate = if (sft.getDescriptor(dtgIndex).getType.getBinding == classOf[Date]) {
        (long) => sf.setAttributeNoConvert(dtgIndex, new Date(long))
      } else {
        (long) => {
          val list = new java.util.ArrayList[Date](1)
          list.add(new Date(long))
          sf.setAttributeNoConvert(dtgIndex, list)
        }
      }

      val callback = new BinaryOutputCallback() {
        override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit = {
          sf.setAttributeNoConvert(geomIndex, gf.createPoint(new Coordinate(lat, lon)))
          sf.setAttributeNoConvert(trackIndex, Int.box(trackId))
          setDate(dtg)
        }
        override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit =
          throw new IllegalStateException("Precomputed BIN values should only have 16 bytes, " +
              "but found 24 bytes with label")
      }
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength, sf))
        BinaryOutputEncoder.decode(source.getTopValue.get, callback)
        sf
      }
    } else if (sample) {
      val trackIndex = encoding.trackIdField.get
      val callback = new BinaryOutputCallback() {
        override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit =
          sf.setAttributeNoConvert(trackIndex, Int.box(trackId))
        override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit =
          throw new IllegalStateException("Precomputed BIN values should only have 16 bytes, " +
              "but found 24 bytes with label")
      }
      if (dedupe) {
        (_) => {
          val row = source.getTopKey.getRow
          sf.setId(getId(row.getBytes, 0, row.getLength, sf))
          BinaryOutputEncoder.decode(source.getTopValue.get, callback)
          sf
        }
      } else {
        (_) => {
          BinaryOutputEncoder.decode(source.getTopValue.get, callback)
          sf
        }
      }
    } else if (dedupe) {
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength, sf))
        sf
      }
    } else {
      (_) => null
    }

    // we are using the pre-computed bin values - we can copy the value directly into our buffer
    writePrecomputedBin =  (_, result) => {
      val bytes = source.getTopValue.get
      result.ensureCapacity(bytes.length).put(bytes)
    }

    result
  }

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writePrecomputedBin(sf, result)
}

object BinAggregatingIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val DEFAULT_PRIORITY = 25

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
          geom, dtg, None, batch, sort, deduplicate, sampling, hints, priority)
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
      label, batchSize, sort, deduplicate, sampling, hints, priority)
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
                        hints: Hints,
                        priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "bin-iter", clas)
    BaseAggregatingIterator.configure(is, deduplicate, None)
    BinAggregatingScan.configure(sft, index, filter, trackId, geom, dtg, label, batchSize, sort, hints).foreach { case (k, v) => is.addOption(k, v) }
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
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "")
    sf.setAttribute(1, GeometryUtils.zeroPoint)
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

    val trackId = Option(hints.getBinTrackIdField).filter(_ != "id").map(returnSft.indexOf)
    val geom = hints.getBinGeomField.map(returnSft.indexOf)
    val dtg = hints.getBinDtgField.map(returnSft.indexOf)
    val label = hints.getBinLabelField.map(returnSft.indexOf)

    val encoder = BinaryOutputEncoder(returnSft, EncodingOptions(geom, dtg, trackId, label))

    // noinspection ScalaDeprecation
    if (index.serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val values = Array[AnyRef](encoder.encode(deserialized), GeometryUtils.zeroPoint)
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, deserialized.getID, values)
      }
    } else {
      val getId = index.getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType, SerializationOptions.withoutId)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val row = e.getKey.getRow
        deserialized.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength, deserialized))
        val values = Array[AnyRef](encoder.encode(deserialized), GeometryUtils.zeroPoint)
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, deserialized.getID, values)
      }
    }
  }
}
