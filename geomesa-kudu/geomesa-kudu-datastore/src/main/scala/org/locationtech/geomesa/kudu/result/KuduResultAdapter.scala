/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.result

import java.nio.ByteBuffer

import org.apache.kudu.client.RowResult
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Adapter to convert raw kudu scan results into simple features
  */
trait KuduResultAdapter {

  /**
    * The kudu columns required for this scan
    *
    * @return
    */
  def columns: Seq[String]

  /**
    * Convert raw rows into result simple features
    *
    * @param results rows
    * @return
    */
  def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature]
}

object KuduResultAdapter {

  /**
    * Turns scan results into simple features
    *
    * @param sft simple feature type
    * @param ecql filter to apply
    * @param hints query hints
    * @return (columns required, adapter for rows)
    */
  def apply(sft: SimpleFeatureType, auths: Seq[Array[Byte]], ecql: Option[Filter], hints: Hints): KuduResultAdapter = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    // TODO GEOMESA-2547 support sampling

    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField).filter(_ != "id")
      val geom = hints.getBinGeomField
      val dtg = hints.getBinDtgField
      val label = hints.getBinLabelField
      BinAdapter(sft, auths, ecql, trackId, geom, dtg, label, hints.getBinBatchSize, hints.isBinSorting)
    } else if (hints.isArrowQuery) {
      val config = ArrowAdapter.ArrowConfig(
        hints.isArrowIncludeFid,
        hints.isArrowProxyFid,
        hints.getArrowDictionaryFields,
        hints.getArrowDictionaryEncodedValues(sft),
        hints.getArrowSort,
        hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt),
        hints.isSkipReduce,
        hints.isArrowDoublePass,
        hints.isArrowMultiFile)
      ArrowAdapter(sft, auths, ecql, hints.getTransform, config)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      DensityAdapter(sft, auths, ecql, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      val encode = hints.isStatsEncode || hints.isSkipReduce
      StatsAdapter(sft, auths, ecql, hints.getTransform, hints.getStatsQuery, encode)
    } else {
      (hints.getTransform, ecql) match {
        case (None, None)                   => DirectAdapter(sft, auths)
        case (None, Some(f))                => FilteringAdapter(sft, auths, f)
        case (Some((tdefs, tsft)), None)    => TransformAdapter(sft, auths, tsft, tdefs)
        case (Some((tdefs, tsft)), Some(f)) => FilteringTransformAdapter(sft, auths, f, tsft, tdefs)
      }
    }
  }

  /**
    * Serializer for result adapters
    *
    * @tparam T class to serialize
    */
  trait KuduResultAdapterSerialization[T <: KuduResultAdapter] {
    def serialize(adapter: T, bb: ExpandingByteBuffer): Unit
    def deserialize(bb: ByteBuffer): T
  }

  def serialize(adapter: KuduResultAdapter): Array[Byte] = {
    val bb = new ExpandingByteBuffer(1024)
    adapter match {
      case EmptyAdapter                 => bb.put(0.toByte)
      case a: DirectAdapter             => bb.put(1.toByte); DirectAdapter.serialize(a, bb)
      case a: FilteringAdapter          => bb.put(2.toByte); FilteringAdapter.serialize(a, bb)
      case a: TransformAdapter          => bb.put(3.toByte); TransformAdapter.serialize(a, bb)
      case a: FilteringTransformAdapter => bb.put(4.toByte); FilteringTransformAdapter.serialize(a, bb)
      case a: ArrowAdapter              => bb.put(5.toByte); ArrowAdapter.serialize(a, bb)
      case a: BinAdapter                => bb.put(6.toByte); BinAdapter.serialize(a, bb)
      case a: StatsAdapter              => bb.put(7.toByte); StatsAdapter.serialize(a, bb)
      case a: DensityAdapter            => bb.put(8.toByte); DensityAdapter.serialize(a, bb)
      case _ => throw new IllegalArgumentException(s"Unhandled adapter $adapter")
    }
    bb.toArray
  }

  def deserialize(bytes: Array[Byte]): KuduResultAdapter = {
    val bb = ByteBuffer.wrap(bytes)
    bb.get() match {
      case 0 => EmptyAdapter
      case 1 => DirectAdapter.deserialize(bb)
      case 2 => FilteringAdapter.deserialize(bb)
      case 3 => TransformAdapter.deserialize(bb)
      case 4 => FilteringTransformAdapter.deserialize(bb)
      case 5 => ArrowAdapter.deserialize(bb)
      case 6 => BinAdapter.deserialize(bb)
      case 7 => StatsAdapter.deserialize(bb)
      case 8 => DensityAdapter.deserialize(bb)
      case b => throw new IllegalArgumentException(s"Unhandled adapter id $b")
    }
  }

  object EmptyAdapter extends KuduResultAdapter {
    override val columns: Seq[String] = Seq.empty
    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] =
      CloseableIterator.empty
  }
}
