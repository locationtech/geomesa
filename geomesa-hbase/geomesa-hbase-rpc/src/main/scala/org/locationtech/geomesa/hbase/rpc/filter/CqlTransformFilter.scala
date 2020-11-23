/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.hbase.rpc.filter.CqlTransformFilter.DelegateFilter
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.iterators.{IteratorCache, SamplingIterator}
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

object CqlTransformFilter extends StrictLogging with SamplingIterator {

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
    * @param hints is used to get sampling options
    * @return
    */
  def apply(
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      hints:Hints): CqlTransformFilter = {
    if (filter.isEmpty && transform.isEmpty && hints.getSampling.isEmpty) {
      throw new IllegalArgumentException("The filter must have a predicate and/or transform")
    }

    val feature = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
    feature.setIdParser(index.getIdFromRow(_, _, _, null))
    transform.foreach { case (tdefs, tsft) => feature.setTransforms(tdefs, tsft) }

    val samplingOptions: Option[(Float, Option[String])] = hints.getSampling

    val delegate = (filter, transform, samplingOptions) match {
      case (None, None, Some(_))  => new FilterDelegate(sft, index, feature, Filter.INCLUDE, samplingOptions)
      case (None, Some(_), Some(_)) => new FilterTransformDelegate(sft, index, feature, Filter.INCLUDE, samplingOptions)
      case (Some(f), None , _) => new FilterDelegate(sft, index, feature, f, samplingOptions)
      case (Some(f), Some(_), _) => new FilterTransformDelegate(sft, index, feature, f, samplingOptions)
      case (None, Some(_), _) => new TransformDelegate(sft, index, feature, samplingOptions)
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
    val indexSftBytes = if (delegate.index == NullFeatureIndex || delegate.index.sft == delegate.sft) { Array.empty[Byte] } else {
      SimpleFeatureTypes.encodeType(delegate.index.sft, includeUserData = true).getBytes(StandardCharsets.UTF_8)
    }
    val samplingFactor: Option[Float] = delegate.samplingOptions.map(s => s._1)
    val samplingField: Option[String] = delegate.samplingOptions.flatMap(s => s._2)

    val samplingFactorBytes = samplingFactor.map(f => ByteBuffer.allocate(4).putFloat(f).array()).getOrElse(Array.empty)
    val samplingFieldBytes = samplingField.map(field => field.getBytes(StandardCharsets.UTF_8)).getOrElse(Array.empty)


    delegate.transform match {
      case None =>
        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length +
            indexBytes.length + indexSftBytes.length + samplingFactorBytes.length + samplingFieldBytes.length + 4*7) //4 bytes (length info) per 7 fields

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
          if(delegate.index == NullFeatureIndex) {
            ByteArrays.writeInt(-1, array, offset)
            offset += 4
          }else{
            ByteArrays.writeInt(0, array, offset)
            offset += 4
          }
        } else {
          ByteArrays.writeInt(indexSftBytes.length, array, offset)
          offset += 4
          System.arraycopy(indexSftBytes, 0, array, offset, indexSftBytes.length)
          offset += indexSftBytes.length
        }

        if (samplingFactorBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
          offset += 4
        } else {
          ByteArrays.writeInt(samplingFactorBytes.length, array, offset)
          offset += 4
          System.arraycopy(samplingFactorBytes, 0, array, offset, samplingFactorBytes.length)
          offset += samplingFactorBytes.length
        }

        if (samplingFieldBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
        } else {
          ByteArrays.writeInt(samplingFieldBytes.length, array, offset)
          offset += 4
          System.arraycopy(samplingFieldBytes, 0, array, offset, samplingFieldBytes.length)
        }


        array

      case Some((tdefs, tsft)) =>
        val tdefsBytes = tdefs.getBytes(StandardCharsets.UTF_8)
        val tsftBytes = SimpleFeatureTypes.encodeType(tsft).getBytes(StandardCharsets.UTF_8)

        val array = Array.ofDim[Byte](sftBytes.length + cqlBytes.length + tdefsBytes.length + tsftBytes.length +
            indexBytes.length + indexSftBytes.length +samplingFactorBytes.length + samplingFieldBytes.length + 4*8) //4 bytes (length info) per 8 fields

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
          if(delegate.index == NullFeatureIndex) {
            ByteArrays.writeInt(-1, array, offset)
            offset += 4
          }else{
            ByteArrays.writeInt(0, array, offset)
            offset += 4
          }
        } else {
          ByteArrays.writeInt(indexSftBytes.length, array, offset)
          offset += 4
          System.arraycopy(indexSftBytes, 0, array, offset, indexSftBytes.length)
          offset += indexSftBytes.length
        }

        if (samplingFactorBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
          offset += 4
        } else {
          ByteArrays.writeInt(samplingFactorBytes.length, array, offset)
          offset += 4
          System.arraycopy(samplingFactorBytes, 0, array, offset, samplingFactorBytes.length)
          offset += samplingFactorBytes.length
        }

        if (samplingFieldBytes.isEmpty) {
          ByteArrays.writeInt(0, array, offset)
        } else {
          ByteArrays.writeInt(samplingFieldBytes.length, array, offset)
          offset += 4
          System.arraycopy(samplingFieldBytes, 0, array, offset, samplingFieldBytes.length)
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
          val (index,newOffset) = deserializeIndex(sft, spec, bytes, offset + 4)
          offset = newOffset
          feature.setIdParser(index.getIdFromRow(_, _, _, null))

          val samplingOptions = deserializeSamplingOptions(bytes,offset)

          new FilterDelegate(sft, index, feature, cql,samplingOptions)
        }
      } else {
        offset += 4
        val tdefs = new String(bytes, offset, tdefsLength)
        offset += tdefsLength

        val tsftLength = ByteArrays.readInt(bytes, offset)
        offset += 4
        val tsft = IteratorCache.sft(new String(bytes, offset, tsftLength))

        feature.setTransforms(tdefs, tsft)

        val (index,newOffset) = deserializeIndex(sft, spec, bytes, offset + tsftLength)
        offset = newOffset
        feature.setIdParser(index.getIdFromRow(_, _, _, null))

        val samplingOptions = deserializeSamplingOptions(bytes,offset)

        if (cql == null) {
          new TransformDelegate(sft, index, feature,samplingOptions)
        } else {
          new FilterTransformDelegate(sft, index, feature, cql,samplingOptions)
        }
      }
    } catch {
      case e: DeserializationException => throw e
      case NonFatal(e) => throw new DeserializationException("Error deserializing filter", e)
    }
  }

  /**
    * Deserialize the sampling options
    *
    * @param bytes raw byte array
    * @param start offset into the byte array to start reading
    * @return the sampling options
    */
  private def deserializeSamplingOptions(
                                bytes: Array[Byte],
                                start: Int) : Option[(Float,Option[String])]={

    var samplingOption: Option[(Float,Option[String])]= Option.empty

    var offset = start
    val factorLength = ByteArrays.readInt(bytes, offset)
    offset += 4
    if(factorLength != 0) {
      val samplingFactor = ByteBuffer.wrap(bytes,offset,factorLength).getFloat
      offset+=factorLength
      val fieldNameLength = ByteArrays.readInt(bytes, offset)
      offset+=4
      if (fieldNameLength!=0){
        val fieldName = new String(bytes, offset, fieldNameLength,StandardCharsets.UTF_8)
        samplingOption = Option.apply((samplingFactor,Option.apply(fieldName)))
      }
      else {
        samplingOption = Option.apply((samplingFactor,Option.empty))
      }
    }
    samplingOption

  }

  /**
    * Deserialize the feature index
    *
    * @param sft simple feature type
    * @param spec simple feature type spec string
    * @param bytes raw byte array
    * @param start offset into the byte array to start reading
    * @return index and the new offset
    */
  private def deserializeIndex(
      sft: SimpleFeatureType,
      spec: String,
      bytes: Array[Byte],
      start: Int): (GeoMesaFeatureIndex[_, _],Int) = {
    if (bytes.length <= start) {
      // we're reading a filter serialized without the index - just use a placeholder instead
      // note: serializing this filter will fail, but it shouldn't ever be serialized
      (NullFeatureIndex,start)
    } else {
      var offset = start
      val identifierLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      val identifier = new String(bytes, offset, identifierLength, StandardCharsets.UTF_8)
      offset += identifierLength
      val indexSftLength = ByteArrays.readInt(bytes, offset)
      offset += 4
      if(indexSftLength == -1) {
        (NullFeatureIndex,offset)
      }else if (indexSftLength == 0) {
        (IteratorCache.index(sft, spec, identifier), offset)
      } else {
        val indexSpec = new String(bytes, offset, indexSftLength, StandardCharsets.UTF_8)
        offset += indexSftLength
        (IteratorCache.index(IteratorCache.sft(indexSpec), indexSpec, identifier), offset)
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
    def samplingOptions:Option[(Float,Option[String])]
  }

  /**
    * Filters without transforming
    *
    * @param sft simple feature type
    * @param feature reusable feature
    * @param filt filter
    * @param samplingOpt sampling options
    */
  private class FilterDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature,
      filt: Filter,
      samplingOpt:Option[(Float,Option[String])]
    ) extends DelegateFilter {


    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = None

    override def samplingOptions: Option[(Float,Option[String])] = samplingOpt
    val sampling = createSamplingFunction(sft,samplingOptions)

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if (filt.evaluate(feature) && sampling(feature)) { ReturnCode.INCLUDE } else { ReturnCode.SKIP }
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
    * Create the sampling function: return a tautology if sampling is disabled
    *
    * @param sft simple feature type
    * @param samplingOptions
    */
  private def createSamplingFunction(sft: SimpleFeatureType,samplingOptions:Option[(Float,Option[String])]) = {
    samplingOptions
      .flatMap(it => sample(SamplingIterator.configure(sft, it))).getOrElse({_ :SimpleFeature => true})
  }

  /**
    * Transforms without filtering
    *
    * @param sft simple feature type
    * @param feature reusable feature, with transforms set
    * @param samplingOpt sampling options
    */
  private class TransformDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature,
      samplingOpt:Option[(Float,Option[String])]
    ) extends DelegateFilter {

    override def filter: Option[Filter] = None
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def samplingOptions: Option[(Float,Option[String])] = samplingOpt
    val sampling = createSamplingFunction(sft,samplingOptions)

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if(sampling(feature))ReturnCode.INCLUDE else ReturnCode.SKIP
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
    * @param samplingOpt sampling options
    */
  private class FilterTransformDelegate(
      val sft: SimpleFeatureType,
      val index: GeoMesaFeatureIndex[_, _],
      feature: KryoBufferSimpleFeature,
      filt: Filter,
      samplingOpt:Option[(Float,Option[String])]
    ) extends DelegateFilter {

    override def samplingOptions: Option[(Float,Option[String])] = samplingOpt
    val sampling = createSamplingFunction(sft,samplingOptions)

    override def filter: Option[Filter] = Some(filt)
    override def transform: Option[(String, SimpleFeatureType)] = feature.getTransform

    override def filterKeyValue(v: Cell): ReturnCode = {
      try {
        feature.setIdBuffer(v.getRowArray, v.getRowOffset, v.getRowLength)
        feature.setBuffer(v.getValueArray, v.getValueOffset, v.getValueLength)
        if (filt.evaluate(feature)&& sampling(feature)) { ReturnCode.INCLUDE } else { ReturnCode.SKIP }
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
