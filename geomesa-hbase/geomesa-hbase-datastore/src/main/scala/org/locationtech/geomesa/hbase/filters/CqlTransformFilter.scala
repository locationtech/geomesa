/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters
import java.nio.charset.StandardCharsets

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.hbase.filters.CqlTransformFilter.DelegateFilter
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.cache.ByteArrayCacheKey
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * HBase filter for CQL predicates and transformations
  *
  * The internal processing logic is kept in a delegate filter that will do either filtering, transforming, or both.
  * The delegate filter instance is a lightweight wrapper that is copied from a cached value based on the
  * serialized bytes, to avoid re-processing setup code whenever possible
  *
  * @param delegate delegate filter
  * @param serialized serialized delegate filter
  */
class CqlTransformFilter(delegate: DelegateFilter, serialized: Array[Byte])
    extends org.apache.hadoop.hbase.filter.Filter {

  /**
    * From the Filter javadocs:
    *
    * A filter can expect the following call sequence:
    *
    * <ul>
    * <li> `reset()` : reset the filter state before filtering a new row. </li>
    * <li> `filterAllRemaining()`: true means row scan is over; false means keep going. </li>
    * <li> `filterRowKey(byte[],int,int)`: true means drop this row; false means include.</li>
    * <li> `filterKeyValue(Cell)`: decides whether to include or exclude this KeyValue. See `ReturnCode`. </li>
    * <li> `transform(KeyValue)`: if the KeyValue is included, let the filter transform the KeyValue. </li>
    * <li> `filterRowCells(List)`: allows direct modification of the final list to be submitted
    * <li> `filterRow()`: last chance to drop entire row based on the sequence of
    *   filter calls. Eg: filter a row if it doesn't contain a specified column. </li>
    * </ul>
    */

  override def filterKeyValue(v: Cell): ReturnCode = delegate.filterKeyValue(v)
  override def transformCell(v: Cell): Cell = delegate.transformCell(v)

  override def reset(): Unit = {}
  override def filterRowKey(buffer: Array[Byte], offset: Int, length: Int): Boolean = false
  override def filterAllRemaining(): Boolean = false
  override def transform(currentKV: KeyValue): KeyValue = currentKV
  override def filterRowCells(kvs: java.util.List[Cell]): Unit = {}
  override def hasFilterRow: Boolean = false
  override def filterRow(): Boolean = false
  override def getNextKeyHint(currentKV: KeyValue): KeyValue = null
  override def getNextCellHint(currentKV: Cell): Cell = null
  override def isFamilyEssential(name: Array[Byte]): Boolean = true
  override def toByteArray: Array[Byte] = serialized

  // overrides package-private method in Filter
  protected [filters] def areSerializedFieldsEqual(other: org.apache.hadoop.hbase.filter.Filter): Boolean = true

  override def toString: String = delegate.toString
}

object CqlTransformFilter extends StrictLogging {

  import org.locationtech.geomesa.index.FilterCacheSize

  val Priority: Int = 30

  private val cache = Caffeine.newBuilder().maximumSize(FilterCacheSize.toInt.get).build(
    new CacheLoader[ByteArrayCacheKey, DelegateFilter]() {
      override def load(key: ByteArrayCacheKey): DelegateFilter = {
        val filter = deserialize(key.bytes)
        logger.trace(s"Deserialized $filter")
        filter
      }
    }
  )

  /**
    * Override of static method from org.apache.hadoop.hbase.filter.Filter
    *
    * @param pbBytes serialized bytes
    * @throws org.apache.hadoop.hbase.exceptions.DeserializationException serialization exception
    * @return
    */
  @throws(classOf[DeserializationException])
  def parseFrom(pbBytes: Array[Byte]): org.apache.hadoop.hbase.filter.Filter =
    // note: we copy KryoBufferSimpleFeatures as they seem to cause problems if re-used, even if thread-local
    new CqlTransformFilter(cache.get(new ByteArrayCacheKey(pbBytes)).copy(), pbBytes)

  /**
    * Create a new filter. Typically, filters created by this method will just be serialized to bytes and sent
    * as part of an HBase scan
    *
    * Note: calling this without at least one of the cql filter or transform would introduce extra processing
    * but not actually do anything, and thus is not allowed
    *
    * @param sft simple feature type
    * @param filter cql filter, if any
    * @param transform transform, if any
    * @return
    */
  def apply(sft: SimpleFeatureType,
            filter: Option[Filter],
            transform: Option[(String, SimpleFeatureType)]): CqlTransformFilter = {
    if (filter.isEmpty && transform.isEmpty) {
      throw new IllegalArgumentException("The filter must have a predicate and/or transform")
    }

    val feature = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
    transform.foreach { case (tdefs, tsft) => feature.setTransforms(tdefs, tsft) }

    val delegate = filter match {
      case None => new TransformDelegate(sft, feature)
      case Some(f) if transform.isEmpty => new FilterDelegate(sft, feature, f)
      case Some(f) => new FilterTransformDelegate(sft, feature, f)
    }

    new CqlTransformFilter(delegate, serialize(delegate))
  }

  /**
    * Full serialization of the delegate filter
    *
    * @param delegate delegate filter
    * @return
    */
  private def serialize(delegate: DelegateFilter): Array[Byte] = {
    val sftBytes = SimpleFeatureTypes.encodeType(delegate.sft, includeUserData = true).getBytes(StandardCharsets.UTF_8)
    val cqlBytes = delegate.filter.map(ECQL.toCQL(_).getBytes(StandardCharsets.UTF_8)).getOrElse(Array.empty)
    delegate.transform match {
      case None =>
        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length + 12)

        var offset = 0
        ByteArrays.writeInt(sftBytes.length, array, offset)
        offset += 4
        System.arraycopy(sftBytes, 0, array, offset, sftBytes.length)
        offset += sftBytes.length
        ByteArrays.writeInt(cqlBytes.length, array, offset)
        offset += 4
        System.arraycopy(cqlBytes, 0, array, offset, cqlBytes.length)
        // write out a -1 to indicate that there aren't any transforms
        ByteArrays.writeInt(-1, array, offset + cqlBytes.length)

        array

      case Some((tdefs, tsft)) =>
        val tdefsBytes = tdefs.getBytes(StandardCharsets.UTF_8)
        val tsftBytes = SimpleFeatureTypes.encodeType(tsft).getBytes(StandardCharsets.UTF_8)

        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length + tdefsBytes.length + tsftBytes.length + 16)

        var offset = 0
        ByteArrays.writeInt(sftBytes.length, array, offset)
        offset += 4
        System.arraycopy(sftBytes, 0, array, offset, sftBytes.length)
        offset += sftBytes.length
        ByteArrays.writeInt(cqlBytes.length, array, offset)
        offset += 4
        System.arraycopy(cqlBytes, 0, array, offset, cqlBytes.length)
        offset += cqlBytes.length
        ByteArrays.writeInt(tdefsBytes.length, array, offset)
        offset += 4
        System.arraycopy(tdefsBytes, 0, array, offset, tdefsBytes.length)
        offset += tdefsBytes.length
        ByteArrays.writeInt(tsftBytes.length, array, offset)
        System.arraycopy(tsftBytes, 0, array, offset + 4, tsftBytes.length)

        array
    }
  }

  /**
    * Deserialize a new delegate instance
    *
    * @param bytes serialized bytes
    * @throws org.apache.hadoop.hbase.exceptions.DeserializationException if anything goes wrong
    * @return
    */
  @throws(classOf[DeserializationException])
  private def deserialize(bytes: Array[Byte]): DelegateFilter = {
    try {
      var offset = 0
      val sftLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      val sftString = new String(bytes, offset, sftLength)
      offset += sftLength

      val sft = IteratorCache.sft(sftString)
      val feature = IteratorCache.serializer(sftString, SerializationOptions.withoutId).getReusableFeature

      val cqlLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      val cql = if (cqlLength == 0) { null } else {
        IteratorCache.filter(sft, sftString, new String(bytes, offset, cqlLength))
      }
      offset += cqlLength

      val tdefsLength = ByteArrays.readInt(bytes, offset)

      if (tdefsLength == -1) {
        if (cql == null) {
          throw new DeserializationException("No filter or transform defined")
        } else {
          new FilterDelegate(sft, feature, cql)
        }
      } else {
        offset += 4
        val tdefs = new String(bytes, offset, tdefsLength)
        offset += tdefsLength

        val tsftLength = ByteArrays.readInt(bytes, offset)
        val tsft = IteratorCache.sft(new String(bytes, offset + 4, tsftLength))

        feature.setTransforms(tdefs, tsft)

        if (cql == null) {
          new TransformDelegate(sft, feature)
        } else {
          new FilterTransformDelegate(sft, feature, cql)
        }
      }
    } catch {
      case e: DeserializationException => throw e
      case NonFatal(e) => throw new DeserializationException("Error deserializing filter", e)
    }
  }

  /**
    * Delegate filter for handling filtering and/or transforming
    */
  private sealed trait DelegateFilter {
    def sft: SimpleFeatureType
    def filter: Option[Filter]
    def transform: Option[(String, SimpleFeatureType)]
    def filterKeyValue(v: Cell): ReturnCode
    def transformCell(v: Cell): Cell
    def copy(): DelegateFilter
  }

  /**
    * Filters without transforming
    *
    * @param sft simple feature type
    * @param feature reusable feature
    * @param filt filter
    */
  private class FilterDelegate(val sft: SimpleFeatureType, feature: KryoBufferSimpleFeature, filt: Filter)
      extends DelegateFilter {

    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = None

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        // TODO GEOMESA-1803 we need to set the id properly
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if (filt.evaluate(feature)) { ReturnCode.INCLUDE } else { ReturnCode.SKIP }
      } catch {
        case NonFatal(e) =>
          logger.error("Error evaluating filter, skipping:", e)
          ReturnCode.SKIP
      }
    }

    override def transformCell(v: Cell): Cell = v

    override def copy(): FilterDelegate = new FilterDelegate(sft, feature.copy(), FastFilterFactory.copy(sft, filt))

    override def toString: String = s"CqlFilter[${ECQL.toCQL(filt)}]"
  }

  /**
    * Transforms without filtering
    *
    * @param sft simple feature type
    * @param feature reusable feature, with transforms set
    */
  private class TransformDelegate(val sft: SimpleFeatureType, feature: KryoBufferSimpleFeature)
      extends DelegateFilter {

    override def filter: Option[Filter] = None
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        // TODO GEOMESA-1803 we need to set the id properly
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        ReturnCode.INCLUDE
      } catch {
        case NonFatal(e) =>
          logger.error("Error setting feature buffer, skipping:", e)
          ReturnCode.SKIP
      }
    }

    override def transformCell(v: Cell): Cell = {
      val value: Array[Byte] = feature.transform()
      new KeyValue(v.getRowArray, v.getRowOffset, v.getRowLength, v.getFamilyArray, v.getFamilyOffset,
        v.getFamilyLength, v.getQualifierArray, v.getQualifierOffset, v.getQualifierLength, v.getTimestamp,
        KeyValue.Type.Put, value, 0, value.length)
    }

    override def copy(): TransformDelegate = new TransformDelegate(sft, feature.copy())

    override def toString: String = s"TransformFilter[${feature.getTransform.get._1}]"
  }

  /**
    * Filters and transforms
    *
    * @param sft simple feature type
    * @param feature reusable feature, with transforms set
    * @param filt filter
    */
  private class FilterTransformDelegate(val sft: SimpleFeatureType, feature: KryoBufferSimpleFeature, filt: Filter)
      extends DelegateFilter {

    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        // TODO GEOMESA-1803 we need to set the id properly
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if (filt.evaluate(feature)) { ReturnCode.INCLUDE } else { ReturnCode.SKIP }
      } catch {
        case NonFatal(e) =>
          logger.error("Error evaluating filter, skipping:", e)
          ReturnCode.SKIP
      }
    }

    override def transformCell(v: Cell): Cell = {
      val value: Array[Byte] = feature.transform()
      new KeyValue(v.getRowArray, v.getRowOffset, v.getRowLength, v.getFamilyArray, v.getFamilyOffset,
        v.getFamilyLength, v.getQualifierArray, v.getQualifierOffset, v.getQualifierLength, v.getTimestamp,
        KeyValue.Type.Put, value, 0, value.length)
    }

    override def copy(): FilterTransformDelegate =
      new FilterTransformDelegate(sft, feature.copy(), FastFilterFactory.copy(sft, filt))

    override def toString: String = s"CqlTransformFilter[${ECQL.toCQL(filt)}, ${feature.getTransform.get._1}]"
  }
}
