/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureDeserializers}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.index.iterators.BinAggregatingScan.ByteBufferResult
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodingOptions}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator extends BaseAggregatingIterator[ByteBufferResult] with BinAggregatingScan

object BinAggregatingIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val DEFAULT_PRIORITY = 25

  /**
   * Configure based on query hints
   */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    val sampling = hints.getSampling

    val is = new IteratorSetting(priority, "bin-iter", classOf[BinAggregatingIterator])
    BinAggregatingScan.configure(sft, index, filter, trackId, geom, dtg, label, batchSize, sort, sampling).foreach {
      case (k, v) => is.addOption(k, v)
    }
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): Entry[Key, Value] => SimpleFeature = {
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "")
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    e: Entry[Key, Value] => {
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
                                 index: GeoMesaFeatureIndex[_, _],
                                 hints: Hints,
                                 serializationType: SerializationType): Entry[Key, Value] => SimpleFeature = {

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
      e: Entry[Key, Value] => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val values = Array[AnyRef](encoder.encode(deserialized), GeometryUtils.zeroPoint)
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, deserialized.getID, values)
      }
    } else {
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType, SerializationOptions.withoutId)
      e: Entry[Key, Value] => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val row = e.getKey.getRow
        val id = index.getIdFromRow(row.getBytes, 0, row.getLength, deserialized)
        deserialized.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
        val values = Array[AnyRef](encoder.encode(deserialized), GeometryUtils.zeroPoint)
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, deserialized.getID, values)
      }
    }
  }
}
