/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues, filterToString}
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.ByteArrays.{OneByteArray, ZeroByteArray}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.Try

/**
  * Attribute index key
  *
  * @param sft simple feature type
  * @param attributeField attribute being indexed
  */
class AttributeIndexKeySpace(val sft: SimpleFeatureType, val sharding: ShardStrategy, attributeField: String)
    extends IndexKeySpace[AttributeIndexValues[Any], AttributeIndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  protected val fieldIndex: Int = sft.indexOf(attributeField)
  protected val fieldIndexShort: Short = fieldIndex.toShort
  protected val isList: Boolean = sft.getDescriptor(fieldIndex).isList

  private val descriptor = sft.getDescriptor(fieldIndex)
  private val binding = if (isList) { descriptor.getListType() } else { descriptor.getType.getBinding }

  protected val decodeValue: String => AnyRef = {
    val alias = binding.getSimpleName.toLowerCase(Locale.US)
    if (isList) {
      // Note that for collection types, only a single entry of the collection will be decoded - this is
      // because the collection entries have been broken up into multiple rows
      encoded => Collections.singletonList(AttributeIndexKey.decode(alias, encoded))
    } else {
      AttributeIndexKey.decode(alias, _)
    }
  }

  require(AttributeIndexKey.encodable(binding),
    s"Indexing is not supported for field $attributeField of type ${binding.getName} - supported types are: " +
        AttributeIndexKey.lexicoders.map(_.getName).mkString(", "))

  override val attributes: Seq[String] = Seq(attributeField)

  override val sharing: Array[Byte] = Array.empty

  override val indexKeyByteLength: Left[(Array[Byte], Int, Int) => Int, Int] = Left(idOffset)

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
    val shard = sharding(writable)

    if (isList) {
      val attribute = writable.getAttribute[java.util.List[Any]](fieldIndex)
      if (attribute == null) {
        MultiRowKeyValue(Seq.empty, sharing, shard, Seq.empty, tier, id, writable.values)
      } else {
        val rows = Seq.newBuilder[Array[Byte]]
        rows.sizeHint(attribute.size())
        val keys = Seq.newBuilder[AttributeIndexKey]
        keys.sizeHint(attribute.size())

        var i = 0
        while (i < attribute.size()) {
          val encoded = AttributeIndexKey.typeEncode(attribute.get(i))
          val value = encoded.getBytes(StandardCharsets.UTF_8)

          // create the byte array - allocate a single array up front to contain everything
          val bytes = Array.ofDim[Byte](shard.length + tier.length + id.length + 1 + value.length) // +1 for null byte
          if (shard.isEmpty) {
            System.arraycopy(value, 0, bytes, 0, value.length)
            bytes(value.length) = ByteArrays.ZeroByte
            System.arraycopy(tier, 0, bytes, value.length + 1, tier.length)
            System.arraycopy(id, 0, bytes, value.length + 1 + tier.length, id.length)
          } else {
            bytes(0) = shard.head // shard is only a single byte
            System.arraycopy(value, 0, bytes, 1, value.length)
            bytes(value.length + 1) = ByteArrays.ZeroByte
            System.arraycopy(tier, 0, bytes, value.length + 2, tier.length)
            System.arraycopy(id, 0, bytes, value.length + 2 + tier.length, id.length)
          }

          rows += bytes
          keys += AttributeIndexKey(fieldIndexShort, encoded)

          i += 1
        }
        MultiRowKeyValue(rows.result, sharing, shard, keys.result, tier, id, writable.values)
      }
    } else {
      val attribute = writable.getAttribute[Any](fieldIndex)
      if (attribute == null) {
        MultiRowKeyValue(Seq.empty, sharing, shard, Seq.empty, tier, id, writable.values)
      } else {
        val encoded = AttributeIndexKey.typeEncode(attribute)
        val value = encoded.getBytes(StandardCharsets.UTF_8)

        // create the byte array - allocate a single array up front to contain everything
        val bytes = Array.ofDim[Byte](shard.length + tier.length + id.length + 1 + value.length) // +1 for null byte
        if (shard.isEmpty) {
          System.arraycopy(value, 0, bytes, 0, value.length)
          bytes(value.length) = ByteArrays.ZeroByte
          System.arraycopy(tier, 0, bytes, value.length + 1, tier.length)
          System.arraycopy(id, 0, bytes, value.length + 1 + tier.length, id.length)
        } else {
          bytes(0) = shard.head // shard is only a single byte
          System.arraycopy(value, 0, bytes, 1, value.length)
          bytes(value.length + 1) = ByteArrays.ZeroByte
          System.arraycopy(tier, 0, bytes, value.length + 2, tier.length)
          System.arraycopy(id, 0, bytes, value.length + 2 + tier.length, id.length)
        }

        SingleRowKeyValue(bytes, sharing, shard, AttributeIndexKey(fieldIndexShort, encoded), tier, id, writable.values)
      }
    }
  }

  override def getIndexValues(filter: Filter, explain: Explainer): AttributeIndexValues[Any] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attributeField, binding)

    if (bounds.isEmpty) {
      // we have an attribute, but weren't able to extract any bounds
      logger.warn(s"Unable to extract any attribute bounds from: ${filterToString(filter)}")
    }

    AttributeIndexValues[Any](attributeField, fieldIndex,
      bounds.asInstanceOf[FilterValues[Bounds[Any]]], binding.asInstanceOf[Class[Any]])
  }

  override def getRanges(values: AttributeIndexValues[Any],
                         multiplier: Int): Iterator[ScanRange[AttributeIndexKey]] = {

    import AttributeIndexKey.encodeForQuery
    import org.locationtech.geomesa.filter.WILDCARD_SUFFIX

    if (values.values.isEmpty) {
      // we have an attribute, but weren't able to extract any bounds... scan all values
      Iterator.single(UnboundedRange(AttributeIndexKey(fieldIndexShort, null, inclusive = false)))
    } else {
      values.values.values.iterator.flatMap { bounds =>
        bounds.bounds match {
          case (None, None) => // not null
            Iterator.single(UnboundedRange(AttributeIndexKey(fieldIndexShort, null, inclusive = false)))

          case (Some(lower), None) =>
            val start = AttributeIndexKey(fieldIndexShort, encodeForQuery(lower, binding), bounds.lower.inclusive)
            Iterator.single(LowerBoundedRange(start))

          case (None, Some(upper)) =>
            val end = AttributeIndexKey(fieldIndexShort, encodeForQuery(upper, binding), bounds.upper.inclusive)
            Iterator.single(UpperBoundedRange(end))

          case (Some(lower), Some(upper)) =>
            if (lower == upper) {
              val row = AttributeIndexKey(fieldIndexShort, encodeForQuery(lower, binding), inclusive = true)
              Iterator.single(SingleRowRange(row))
            } else if (lower + WILDCARD_SUFFIX == upper) {
              val row = AttributeIndexKey(fieldIndexShort, encodeForQuery(lower, binding), inclusive = true)
              Iterator.single(PrefixRange(row))
            } else {
              val start = AttributeIndexKey(fieldIndexShort, encodeForQuery(lower, binding), bounds.lower.inclusive)
              val end = AttributeIndexKey(fieldIndexShort, encodeForQuery(upper, binding), bounds.upper.inclusive)
              Iterator.single(BoundedRange(start, end))
            }
        }
      }
    }
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]], tier: Boolean): Iterator[ByteRange] = {
    if (tier) {
      getTieredRangeBytes(ranges, sharding.shards)
    } else {
      getStandardRangeBytes(ranges, sharding.shards)
    }
  }

  override def useFullFilter(values: Option[AttributeIndexValues[Any]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if we have an attribute, but weren't able to extract any bounds, values.values will be empty
    values.forall(v => v.values.isEmpty || !v.values.precise)
  }

  /**
    * Decodes an attribute value out of row string
    *
    * @param row row bytes
    * @param offset offset into the row bytes
    * @param length length of the row bytes, from the offset
    * @return
    */
  def decodeRowValue(row: Array[Byte], offset: Int, length: Int): Try[AnyRef] = Try {
    val valueStart = offset + sharding.length // start of the encoded value
    // null byte indicates end of value
    val valueEnd = math.min(row.indexOf(ByteArrays.ZeroByte, valueStart), offset + length)
    decodeValue(new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8))
  }

  protected def getTieredRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]],
                                    prefixes: Seq[Array[Byte]]): Iterator[ByteRange] = {
    import org.locationtech.geomesa.utils.index.ByteArrays.concat

    val bytes = ranges.map {
      case SingleRowRange(row) =>
        SingleRowByteRange(lower(row))

      case BoundedRange(lo, hi) =>
        tieredUpper(hi) match {
          case None     => LowerBoundedByteRange(lower(lo), upper(hi))
          case Some(up) => BoundedByteRange(lower(lo), up)
        }

      case PrefixRange(prefix) =>
        UnboundedByteRange(lower(prefix, prefix = true), upper(prefix, prefix = true))

      case LowerBoundedRange(lo) =>
        LowerBoundedByteRange(lower(lo), upper(AttributeIndexKey(lo.i, null)))

      case UpperBoundedRange(hi) =>
        tieredUpper(hi) match {
          case None     => UnboundedByteRange(lower(AttributeIndexKey(hi.i, null)), upper(hi))
          case Some(up) => UpperBoundedByteRange(lower(AttributeIndexKey(hi.i, null)), up)
        }

      case UnboundedRange(empty) =>
        UnboundedByteRange(lower(empty), upper(empty))
    }

    if (prefixes.isEmpty) { bytes } else {
      bytes.flatMap {
        case SingleRowByteRange(row)       => prefixes.map(p => SingleRowByteRange(concat(p, row)))
        case BoundedByteRange(lo, hi)      => prefixes.map(p => BoundedByteRange(concat(p, lo), concat(p, hi)))
        case LowerBoundedByteRange(lo, hi) => prefixes.map(p => LowerBoundedByteRange(concat(p, lo), concat(p, hi)))
        case UpperBoundedByteRange(lo, hi) => prefixes.map(p => UpperBoundedByteRange(concat(p, lo), concat(p, hi)))
        case UnboundedByteRange(lo, hi)    => prefixes.map(p => UnboundedByteRange(concat(p, lo), concat(p, hi)))
      }
    }
  }

  protected def getStandardRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]],
                                      prefixes: Seq[Array[Byte]]): Iterator[ByteRange] = {

    import org.locationtech.geomesa.utils.index.ByteArrays.concat

    val bytes = ranges.map {
      case SingleRowRange(row)   => BoundedByteRange(lower(row), upper(row))
      case BoundedRange(lo, hi)  => BoundedByteRange(lower(lo), upper(hi))
      case PrefixRange(prefix)   => BoundedByteRange(lower(prefix, prefix = true), upper(prefix, prefix = true))
      case LowerBoundedRange(lo) => BoundedByteRange(lower(lo), upper(AttributeIndexKey(lo.i, null)))
      case UpperBoundedRange(hi) => BoundedByteRange(lower(AttributeIndexKey(hi.i, null)), upper(hi))
      case UnboundedRange(empty) => BoundedByteRange(lower(empty), upper(empty))
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }

    if (prefixes.isEmpty) { bytes } else {
      bytes.flatMap {
        case BoundedByteRange(lo, hi) => prefixes.map(p => BoundedByteRange(concat(p, lo), concat(p, hi)))
      }
    }
  }

  /**
    * Gets a lower range bound for an attribute value. The bound can be used with additional tiering or not
    *
    * @param key attribute value
    * @param prefix if this is a prefix scan or not
    * @return
    */
  protected def lower(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
    if (key.value == null || key.value.isEmpty) {
      Array.empty
    } else if (prefix) {
      // note: inclusive doesn't make sense for prefix ranges
      key.value.getBytes(StandardCharsets.UTF_8)
    } else if (key.inclusive) {
      ByteArrays.concat(key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
    } else {
      ByteArrays.concat(key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
    }
  }

  /**
    * Gets an upper range bound for an attribute value. The bound is only suitable when there is no additional tiering
    *
    * @param key attribute value
    * @param prefix if this is a prefix scan or not
    * @return
    */
  protected def upper(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
    if (key.value == null || key.value.isEmpty) {
      ByteRange.UnboundedUpperRange
    } else if (prefix) {
      // get the row following the prefix, then get the next row
      // note: inclusiveness doesn't really make sense for prefix ranges
      ByteArrays.rowFollowingPrefix(key.value.getBytes(StandardCharsets.UTF_8))
    } else if (key.inclusive) {
      // row following prefix, after the delimiter
      ByteArrays.concat(key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
    } else {
      // exclude the row
      ByteArrays.concat(key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
    }
  }

  /**
    * Gets an upper bound for a range that will be tiered. A bound will only be returned if it
    * supports additional tiering.
    *
    * @param key attribute value
    * @return
    */
  protected def tieredUpper(key: AttributeIndexKey): Option[Array[Byte]] = {
    // note: we can't tier exclusive end points, as we can't calculate previous rows
    if (key.value == null || !key.inclusive) { None } else {
      // match the final row, and count on remaining range to exclude the rest
      Some(ByteArrays.concat(key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray))
    }
  }

  // null byte indicates end of value
  private def idOffset(row: Array[Byte], offset: Int, length: Int): Int =
    row.indexOf(ByteArrays.ZeroByte, offset + sharding.length) + 1 - offset
}

object AttributeIndexKeySpace extends IndexKeySpaceFactory[AttributeIndexValues[Any], AttributeIndexKey] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean = {
    if (attributes.lengthCompare(1) != 0) { false } else {
      val descriptor = sft.getDescriptor(attributes.head)
      descriptor != null && AttributeIndexKey.encodable(descriptor)
    }
  }

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): AttributeIndexKeySpace =
    new AttributeIndexKeySpace(sft, AttributeShardStrategy(sft), attributes.head)
}
