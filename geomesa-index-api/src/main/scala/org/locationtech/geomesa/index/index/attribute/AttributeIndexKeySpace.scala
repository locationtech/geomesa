/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues, filterToString}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.ByteArrays.{OneByteArray, ZeroByteArray}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object AttributeIndexKeySpace extends AttributeIndexKeySpace

trait AttributeIndexKeySpace extends IndexKeySpace[AttributeIndexValues[Any], AttributeIndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getAttributeDescriptors.asScala.exists(_.isIndexed)

  override def indexKeyByteLength: Int =
    throw new IllegalArgumentException("Attribute key space has variable length index keys")

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[AttributeIndexKey] = {
    val indexedAttributes =
      SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => (sft.indexOf(d.getName), d.isList))
    (feature) => indexedAttributes.flatMap { case (i, list) =>
      AttributeIndexKey.encodeForIndex(feature.getAttribute(i), list).map(v => AttributeIndexKey(i.toShort, v))
    }
  }

  override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map { d =>
      val i = sft.indexOf(d.getName)
      (i, AttributeIndexKey.indexToBytes(i), d.isList)
    }
    (prefix, feature, suffix) => {
      val baseLength = prefix.map(_.length).sum + suffix.length + 3 // 2 for attributed i, 1 for null byte
      indexedAttributes.flatMap { case (idx, idxBytes, list) =>
        AttributeIndexKey.encodeForIndex(feature.getAttribute(idx), list).map { encoded =>
          val value = encoded.getBytes(StandardCharsets.UTF_8)
          // create the byte array - allocate a single array up front to contain everything
          val bytes = Array.ofDim[Byte](baseLength + value.length)
          var i = 0
          prefix.foreach { p => System.arraycopy(p, 0, bytes, i, p.length); i += p.length }
          bytes(i) = idxBytes(0)
          i += 1
          bytes(i) = idxBytes(1)
          i += 1
          System.arraycopy(value, 0, bytes, i, value.length)
          i += value.length
          bytes(i) = ByteArrays.ZeroByte
          System.arraycopy(suffix, 0, bytes, i + 1, suffix.length)
          bytes
        }
      }
    }
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): AttributeIndexValues[Any] = {
    val attribute = {
      val names = FilterHelper.propertyNames(filter, sft)
      require(names.lengthCompare(1) == 0,
        s"Couldn't extract single attribute name from filter '${filterToString(filter)}'")
      names.head
    }

    val i = sft.indexOf(attribute)

    require(i != -1, s"Attribute '$attribute' from filter '${filterToString(filter)}' does not exist in '$sft'")

    val descriptor = sft.getDescriptor(i)
    val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, binding)

    if (bounds.isEmpty) {
      // we have an attribute, but weren't able to extract any bounds
      logger.warn(s"Unable to extract any attribute bounds from: ${filterToString(filter)}")
    }

    AttributeIndexValues(attribute, i, bounds.asInstanceOf[FilterValues[Bounds[Any]]], binding.asInstanceOf[Class[Any]])
  }

  override def getRanges(values: AttributeIndexValues[Any],
                         multiplier: Int): Iterator[ScanRange[AttributeIndexKey]] = {

    import AttributeIndexKey.encodeForQuery
    import org.locationtech.geomesa.filter.WILDCARD_SUFFIX

    val AttributeIndexValues(_, i, fb, binding) = values

    if (fb.isEmpty) {
      // we have an attribute, but weren't able to extract any bounds... scan all values
      Iterator.single(UnboundedRange(AttributeIndexKey(i.toShort, null, inclusive = false)))
    } else {
      fb.values.iterator.flatMap { bounds =>
        bounds.bounds match {
          case (None, None) => // not null
            Iterator.single(UnboundedRange(AttributeIndexKey(i.toShort, null, inclusive = false)))

          case (Some(lower), None) =>
            val start = AttributeIndexKey(i.toShort, encodeForQuery(lower, binding), bounds.lower.inclusive)
            Iterator.single(LowerBoundedRange(start))

          case (None, Some(upper)) =>
            val end = AttributeIndexKey(i.toShort, encodeForQuery(upper, binding), bounds.upper.inclusive)
            Iterator.single(UpperBoundedRange(end))

          case (Some(lower), Some(upper)) =>
            if (lower == upper) {
              val row = AttributeIndexKey(i.toShort, encodeForQuery(lower, binding), inclusive = true)
              Iterator.single(SingleRowRange(row))
            } else if (lower + WILDCARD_SUFFIX == upper) {
              val row = AttributeIndexKey(i.toShort, encodeForQuery(lower, binding), inclusive = true)
              Iterator.single(PrefixRange(row))
            } else {
              val start = AttributeIndexKey(i.toShort, encodeForQuery(lower, binding), bounds.lower.inclusive)
              val end = AttributeIndexKey(i.toShort, encodeForQuery(upper, binding), bounds.upper.inclusive)
              Iterator.single(BoundedRange(start, end))
            }
        }
      }
    }
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]],
                             prefixes: Seq[Array[Byte]],
                             tier: Boolean): Iterator[ByteRange] = {
    if (tier) {
      getTieredRangeBytes(ranges, prefixes)
    } else {
      getStandardRangeBytes(ranges, prefixes)
    }
  }

  override def useFullFilter(values: Option[AttributeIndexValues[Any]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if we have an attribute, but weren't able to extract any bounds, values.values will be empty
    values.forall(v => v.values.isEmpty || !v.values.precise)
  }

  private def getTieredRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]],
                                  prefixes: Seq[Array[Byte]]): Iterator[ByteRange] = {
    import org.locationtech.geomesa.utils.index.ByteArrays.concat

    val bytes = ranges.map {
      case SingleRowRange(row) =>
        SingleRowByteRange(lower(row))

      case BoundedRange(lo, hi) =>
        tieredUpper(hi) match {
          case None     => TieredByteRange(lower(lo), upper(hi), lowerTierable = true)
          case Some(up) => BoundedByteRange(lower(lo), up)
        }

      case PrefixRange(prefix) =>
        TieredByteRange(lower(prefix, prefix = true), upper(prefix, prefix = true))

      case LowerBoundedRange(lo) =>
        TieredByteRange(lower(lo), upper(AttributeIndexKey(lo.i, null)), lowerTierable = true)

      case UpperBoundedRange(hi) =>
        tieredUpper(hi) match {
          case None     => TieredByteRange(lower(AttributeIndexKey(hi.i, null)), upper(hi))
          case Some(up) => TieredByteRange(lower(AttributeIndexKey(hi.i, null)), up, upperTierable = true)
        }

      case UnboundedRange(empty) =>
        TieredByteRange(lower(empty), upper(empty))
    }

    if (prefixes.isEmpty) { bytes } else {
      bytes.flatMap {
        case SingleRowByteRange(row)  => prefixes.map(p => SingleRowByteRange(concat(p, row)))
        case BoundedByteRange(lo, hi) => prefixes.map(p => BoundedByteRange(concat(p, lo), concat(p, hi)))
        case t: TieredByteRange => prefixes.map(p => t.copy(lower = concat(p, t.lower), upper = concat(p, t.upper)))
      }
    }
  }

  private def getStandardRangeBytes(ranges: Iterator[ScanRange[AttributeIndexKey]],
                                    prefixes: Seq[Array[Byte]]): Iterator[ByteRange] = {

    import org.locationtech.geomesa.utils.index.ByteArrays.concat

    val bytes = ranges.map {
      case SingleRowRange(row)   => SingleRowByteRange(lower(row))
      case BoundedRange(lo, hi)  => BoundedByteRange(lower(lo), upper(hi))
      case PrefixRange(prefix)   => BoundedByteRange(lower(prefix, prefix = true), upper(prefix, prefix = true))
      case LowerBoundedRange(lo) => BoundedByteRange(lower(lo), upper(AttributeIndexKey(lo.i, null)))
      case UpperBoundedRange(hi) => BoundedByteRange(lower(AttributeIndexKey(hi.i, null)), upper(hi))
      case UnboundedRange(empty) => BoundedByteRange(lower(empty), upper(empty))
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }

    if (prefixes.isEmpty) { bytes } else {
      bytes.flatMap {
        case SingleRowByteRange(row)  => prefixes.map(p => SingleRowByteRange(concat(p, row)))
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
  private def lower(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
    val index = AttributeIndexKey.indexToBytes(key.i)
    if (key.value == null) {
      index
    } else if (prefix) {
      // note: inclusive doesn't make sense for prefix ranges
      ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8))
    } else if (key.inclusive) {
      ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
    } else {
      ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
    }
  }

  /**
    * Gets an upper range bound for an attribute value. The bound is only suitable when there is no additional tiering
    *
    * @param key attribute value
    * @param prefix if this is a prefix scan or not
    * @return
    */
  private def upper(key: AttributeIndexKey, prefix: Boolean = false): Array[Byte] = {
    val index = AttributeIndexKey.indexToBytes(key.i)
    if (key.value == null) {
      ByteArrays.rowFollowingPrefix(index)
    } else if (prefix) {
      // get the row following the prefix, then get the next row
      // note: inclusiveness doesn't really make sense for prefix ranges
      ByteArrays.rowFollowingPrefix(ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8)))
    } else if (key.inclusive) {
      // row following prefix, after the delimiter
      ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8), OneByteArray)
    } else {
      // exclude the row
      ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray)
    }
  }

  /**
    * Gets an upper bound for a range that will be tiered. A bound will only be returned if it
    * supports additional tiering.
    *
    * @param key attribute value
    * @return
    */
  private def tieredUpper(key: AttributeIndexKey): Option[Array[Byte]] = {
    // note: we can't tier exclusive end points, as we can't calculate previous rows
    if (key.value == null || !key.inclusive) { None } else {
      // match the final row, and count on remaining range to exclude the rest
      val index = AttributeIndexKey.indexToBytes(key.i)
      Some(ByteArrays.concat(index, key.value.getBytes(StandardCharsets.UTF_8), ZeroByteArray))
    }
  }
}
