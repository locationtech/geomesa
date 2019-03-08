/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.hbase.filters.CqlTransformFilter.DelegateFilter
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.{ByteArrays, IndexMode}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * HBase filter for CQL predicates and transformations
  *
  * The internal processing logic is kept in a delegate filter that will do either filtering, transforming, or both.
  *
  * @param delegate delegate filter
  */
class CqlTransformFilter(delegate: DelegateFilter) extends FilterBase {

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
  override def toByteArray: Array[Byte] = CqlTransformFilter.serialize(delegate)
  override def toString: String = delegate.toString
}

object CqlTransformFilter extends StrictLogging {

  val Priority: Int = 30

  /**
    * Override of static method from org.apache.hadoop.hbase.filter.Filter
    *
    * @param pbBytes serialized bytes
    * @throws org.apache.hadoop.hbase.exceptions.DeserializationException serialization exception
    * @return
    */
  @throws(classOf[DeserializationException])
  def parseFrom(pbBytes: Array[Byte]): org.apache.hadoop.hbase.filter.Filter =
    new CqlTransformFilter(deserialize(pbBytes))

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
  def apply(
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): CqlTransformFilter = {
    if (filter.isEmpty && transform.isEmpty) {
      throw new IllegalArgumentException("The filter must have a predicate and/or transform")
    }

    val feature = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
    feature.setIdParser(index.getIdFromRow(_, _, _, null))
    transform.foreach { case (tdefs, tsft) => feature.setTransforms(tdefs, tsft) }

    val delegate = filter match {
      case None => new TransformDelegate(sft, index, feature)
      case Some(f) if transform.isEmpty => new FilterDelegate(sft, index, feature, f)
      case Some(f) => new FilterTransformDelegate(sft, index, feature, f)
    }

    new CqlTransformFilter(delegate)
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
    val indexBytes = delegate.index.identifier.getBytes(StandardCharsets.UTF_8)
    val indexSftBytes = if (delegate.index.sft == delegate.sft) { Array.empty[Byte] } else {
      SimpleFeatureTypes.encodeType(delegate.index.sft, includeUserData = true).getBytes(StandardCharsets.UTF_8)
    }

    delegate.transform match {
      case None =>
        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length +
            indexBytes.length + indexSftBytes.length + 20)

        var offset = 0
        ByteArrays.writeInt(sftBytes.length, array, offset)
        offset += 4
        System.arraycopy(sftBytes, 0, array, offset, sftBytes.length)
        offset += sftBytes.length
        ByteArrays.writeInt(cqlBytes.length, array, offset)
        offset += 4
        System.arraycopy(cqlBytes, 0, array, offset, cqlBytes.length)
        offset += cqlBytes.length
        // write out a -1 to indicate that there aren't any transforms
        ByteArrays.writeInt(-1, array, offset)
        offset += 4
        ByteArrays.writeInt(indexBytes.length, array, offset)
        offset += 4
        System.arraycopy(indexBytes, 0, array, offset, indexBytes.length)
        offset += indexBytes.length
        if (indexSftBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
        } else {
          ByteArrays.writeInt(indexSftBytes.length, array, offset)
          offset += 4
          System.arraycopy(indexSftBytes, 0, array, offset, indexSftBytes.length)
        }


        array

      case Some((tdefs, tsft)) =>
        val tdefsBytes = tdefs.getBytes(StandardCharsets.UTF_8)
        val tsftBytes = SimpleFeatureTypes.encodeType(tsft).getBytes(StandardCharsets.UTF_8)

        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length + tdefsBytes.length + tsftBytes.length +
            indexBytes.length + indexSftBytes.length + 24)

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
        offset += 4
        System.arraycopy(tsftBytes, 0, array, offset, tsftBytes.length)
        offset += tsftBytes.length
        ByteArrays.writeInt(indexBytes.length, array, offset)
        offset += 4
        System.arraycopy(indexBytes, 0, array, offset, indexBytes.length)
        offset += indexBytes.length
        if (indexSftBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
        } else {
          ByteArrays.writeInt(indexSftBytes.length, array, offset)
          offset += 4
          System.arraycopy(indexSftBytes, 0, array, offset, indexSftBytes.length)
        }

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
      val spec = new String(bytes, offset, sftLength)
      offset += sftLength

      val sft = IteratorCache.sft(spec)
      val feature = IteratorCache.serializer(spec, SerializationOptions.withoutId).getReusableFeature

      val cqlLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      val cql = if (cqlLength == 0) { null } else {
        IteratorCache.filter(sft, spec, new String(bytes, offset, cqlLength))
      }
      offset += cqlLength

      val tdefsLength = ByteArrays.readInt(bytes, offset)

      if (tdefsLength == -1) {
        if (cql == null) {
          throw new DeserializationException("No filter or transform defined")
        } else {
          val index = deserializeIndex(sft, spec, bytes, offset + 4)
          feature.setIdParser(index.getIdFromRow(_, _, _, null))
          new FilterDelegate(sft, index, feature, cql)
        }
      } else {
        offset += 4
        val tdefs = new String(bytes, offset, tdefsLength)
        offset += tdefsLength

        val tsftLength = ByteArrays.readInt(bytes, offset)
        offset += 4
        val tsft = IteratorCache.sft(new String(bytes, offset, tsftLength))

        feature.setTransforms(tdefs, tsft)

        val index = deserializeIndex(sft, spec, bytes, offset + tsftLength)
        feature.setIdParser(index.getIdFromRow(_, _, _, null))

        if (cql == null) {
          new TransformDelegate(sft, index, feature)
        } else {
          new FilterTransformDelegate(sft, index, feature, cql)
        }
      }
    } catch {
      case e: DeserializationException => throw e
      case NonFatal(e) => throw new DeserializationException("Error deserializing filter", e)
    }
  }

  /**
    * Deserialize the feature index
    *
    * @param sft simple feature type
    * @param spec simple feature type spec string
    * @param bytes raw byte array
    * @param start offset into the byte array to start reading
    * @return
    */
  private def deserializeIndex(
      sft: SimpleFeatureType,
      spec: String,
      bytes: Array[Byte],
      start: Int): GeoMesaFeatureIndex[_, _] = {
    if (bytes.length <= start) {
      // we're reading a filter serialized without the index - just use a placeholder instead
      // note: serializing this filter will fail, but it shouldn't ever be serialized
      NullFeatureIndex
    } else {
      var offset = start
      val identifierLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      val identifier = new String(bytes, offset, identifierLength, StandardCharsets.UTF_8)
      offset += identifierLength
      val indexSftLength = ByteArrays.readInt(bytes, offset)
      if (indexSftLength == 0) {
        IteratorCache.index(sft, spec, identifier)
      } else {
        val indexSpec = new String(bytes, offset + 4, indexSftLength, StandardCharsets.UTF_8)
        IteratorCache.index(IteratorCache.sft(indexSpec), indexSpec, identifier)
      }
    }
  }

  /**
    * Delegate filter for handling filtering and/or transforming
    */
  private sealed trait DelegateFilter {
    def sft: SimpleFeatureType
    def index: GeoMesaFeatureIndex[_, _]
    def filter: Option[Filter]
    def transform: Option[(String, SimpleFeatureType)]
    def filterKeyValue(v: Cell): ReturnCode
    def transformCell(v: Cell): Cell
  }

  /**
    * Filters without transforming
    *
    * @param sft simple feature type
    * @param feature reusable feature
    * @param filt filter
    */
  private class FilterDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature,
      filt: Filter
    ) extends DelegateFilter {

    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = None

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if (filt.evaluate(feature)) { ReturnCode.INCLUDE } else { ReturnCode.SKIP }
      } catch {
        case NonFatal(e) =>
          logger.error("Error evaluating filter, skipping:", e)
          ReturnCode.SKIP
      }
    }

    override def transformCell(v: Cell): Cell = v

    override def toString: String = s"CqlFilter[${ECQL.toCQL(filt)}]"
  }

  /**
    * Transforms without filtering
    *
    * @param sft simple feature type
    * @param feature reusable feature, with transforms set
    */
  private class TransformDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature
    ) extends DelegateFilter {

    override def filter: Option[Filter] = None
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
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

    override def toString: String = s"TransformFilter[${feature.getTransform.get._1}]"
  }

  /**
    * Filters and transforms
    *
    * @param sft simple feature type
    * @param feature reusable feature, with transforms set
    * @param filt filter
    */
  private class FilterTransformDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature,
      filt: Filter
    ) extends DelegateFilter {

    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
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

    override def toString: String = s"CqlTransformFilter[${ECQL.toCQL(filt)}, ${feature.getTransform.get._1}]"
  }

  private object NullFeatureIndex extends GeoMesaFeatureIndex[Any, Any](null, null, "", 0, Seq.empty, IndexMode.Read) {

    override def keySpace: IndexKeySpace[Any, Any] = throw new NotImplementedError()

    override def tieredKeySpace: Option[IndexKeySpace[_, _]] = throw new NotImplementedError()

    override def getFilterStrategy(
        filter: Filter,
        transform: Option[SimpleFeatureType],
        stats: Option[GeoMesaStats]): Option[FilterStrategy] = throw new NotImplementedError()

    override def getIdFromRow(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String = ""
  }
}
