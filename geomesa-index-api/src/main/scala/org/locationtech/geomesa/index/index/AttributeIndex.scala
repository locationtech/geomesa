/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets
import java.util.{Date, Locale, Collection => JCollection}

import com.google.common.primitives.{Bytes, Shorts, UnsignedBytes}
import com.typesafe.scalalogging.LazyLogging
import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.util.Converters
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Attribute index with secondary z-curve indexing. Z-indexing is based on the sft and will be
  * one of Z3, XZ3, Z2, XZ2.
  */
trait AttributeIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R]
    extends GeoMesaFeatureIndex[DS, F, W] with IndexAdapter[DS, F, W, R] with AttributeFilterStrategy[DS, F, W]
    with AttributeRowDecoder with LazyLogging {

  import AttributeIndex._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getAttributeDescriptors.exists(_.isIndexed)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft)
    (wf) => getRows(wf).map { case (_, r) => createInsert(r, wf) }
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft)
    (wf) => getRows(wf).map { case (_, r) => createDelete(r, wf) }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    // drop the encoded value and the date field (12 bytes) if it's present - the rest of the row is the ID
    val shard = getShards(sft).head.length
    // exclude feature byte and 2 index bytes and shard bytes
    val from = if (sft.isTableSharing) { 3 + shard } else { 2 + shard }
    val secondary = getSecondaryIndexKeyLength(sft)
    (row, offset, length) => {
      val start = row.indexOf(NullByte, from + offset) + secondary + 1
      new String(row, start, length + offset - start, StandardCharsets.UTF_8)
    }
  }

  override def decodeRowValue(sft: SimpleFeatureType, index: Int): (Array[Byte], Int, Int) => Try[Any] = {
    val shard = getShards(sft).head.length
    // exclude feature byte and 2 index bytes and shard bytes
    val from = if (sft.isTableSharing) { 3 + shard } else { 2 + shard }
    val descriptor = sft.getDescriptor(index)
    (row, offset, length) => Try {
      val valueStart = offset + from // start of the encoded value
      val end = offset + length // end of the row, max search space
      var valueEnd = valueStart // end of the encoded value
      while (valueEnd < end && row(valueEnd) != NullByte) { // null byte indicates end of value
        valueEnd += 1
      }
      val encoded = new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8)
      decode(encoded, descriptor)
    }
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    val sharing = sft.getTableSharingBytes
    val indices = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => sft.indexOf(d.getLocalName))
    val shards = getShards(sft)
    for (i <- indices; s <- shards) yield {
      Bytes.concat(sharing, indexToBytes(i), s)
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {

    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val primary = filter.primary.getOrElse {
      throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
    }

    // pull out any dates from the filter to help narrow down the attribute ranges
    val secondaryRanges = filter.secondary.map(getSecondaryIndexRanges(sft, _, explain)).getOrElse(Seq.empty)

    // TODO GEOMESA-1336 fix exclusive AND handling for list types

    val attribute = {
      val names = DataUtilities.attributeNames(primary)
      require(names.length == 1, s"Couldn't extract single attribute name from filter '${filterToString(primary)}'")
      names(0)
    }

    val i = sft.indexOf(attribute)
    require(i != -1, s"Attribute '$attribute' from filter '${filterToString(primary)}' does not exist in '$sft'")

    val binding = {
      val descriptor = sft.getDescriptor(i)
      if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
    }

    require(classOf[Comparable[_]].isAssignableFrom(binding), s"Attribute '$attribute' is not comparable")

    val fb = FilterHelper.extractAttributeBounds(primary, attribute, binding)

    implicit val byteComparator = UnsignedBytes.lexicographicalComparator()
    lazy val lowerSecondary = secondaryRanges.map(_._1).minOption.getOrElse(Array.empty)
    lazy val upperSecondary = secondaryRanges.map(_._2).maxOption.getOrElse(Array.empty)

    val shards = getShards(sft)

    val ranges = fb.values.flatMap { bounds =>
      bounds.bounds match {
        case (None, None) => // not null
          val starts = lowerBounds(sft, i, shards)
          val ends = upperBounds(sft, i, shards)
          shards.indices.map(i => range(starts(i), ends(i)))

        case (Some(lower), None) =>
          val starts = startRows(sft, i, shards, lower, bounds.inclusive, lowerSecondary)
          val ends = upperBounds(sft, i, shards)
          shards.indices.map(i => range(starts(i), ends(i)))

        case (None, Some(upper)) =>
          val starts = lowerBounds(sft, i, shards)
          val ends = endRows(sft, i, shards, upper, bounds.inclusive, upperSecondary)
          shards.indices.map(i => range(starts(i), ends(i)))

        case (Some(lower), Some(upper)) =>
          if (lower == upper) {
            equals(sft, i, shards, lower, secondaryRanges)
          } else if (lower + WILDCARD_SUFFIX == upper) {
            val prefix = rowPrefix(sft, i)
            val value = encodeForQuery(lower, sft.getDescriptor(i))
            shards.map(shard => rangePrefix(Bytes.concat(prefix, shard, value)))
          } else {
            val starts = startRows(sft, i, shards, lower, bounds.inclusive, lowerSecondary)
            val ends = endRows(sft, i, shards, upper, bounds.inclusive, upperSecondary)
            shards.indices.map(i => range(starts(i), ends(i)))
          }
      }
    }

    scanPlan(sft, ds, filter, hints, ranges, filter.secondary)
  }

  /**
    * Shards to use for attribute indices. Subclasses can override to disable shards by returning
    * `IndexedSeq(Array.empty[Byte])`
    *
    * @param sft simple feature type
    * @return number of shards
    */
  protected def getShards(sft: SimpleFeatureType): IndexedSeq[Array[Byte]] =
    Option(sft.getAttributeShards).filter(_ > 1).map(SplitArrays.apply).getOrElse(SplitArrays.EmptySplits)

  /**
    * Rows in the attribute table have the following layout:
    *
    * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
    * - 2 bytes storing the index of the attribute in the sft
    * - 1 byte shard (OPTIONAL)
    * - n bytes storing the lexicoded attribute value
    * - NULLBYTE as a separator
    * - n bytes storing the secondary z-index of the feature - identified by getSecondaryIndexKeyLength
    * - n bytes storing the feature ID
    */
  protected def getRowKeys(sft: SimpleFeatureType): (F) => Seq[(Int, Array[Byte])] = {
    val prefix = sft.getTableSharingBytes
    val getSecondaryKey = getSecondaryIndexKey(sft)
    val getShard: (F) => Array[Byte] = {
      val shards = getShards(sft)
      if (shards.length == 1) {
        val shard = shards.head
        (_) => shard
      } else {
        (wf: F) => shards(wf.idHash % shards.length)
      }
    }

    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map { d =>
      val i = sft.indexOf(d.getName)
      (d, i, indexToBytes(i))
    }

    (wf) => {
      val secondary = getSecondaryKey(wf)
      val shard = getShard(wf)
      indexedAttributes.flatMap { case (descriptor, idx, idxBytes) =>
        val attributes = encodeForIndex(wf.feature.getAttribute(idx), descriptor)
        attributes.map(a => (idx, Bytes.concat(prefix, idxBytes, shard, a, NullByteArray, secondary, wf.idBytes)))
      }
    }
  }

  protected def secondaryIndex(sft: SimpleFeatureType): Option[IndexKeySpace[_]] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index).find(_.supports(sft))

  protected def getSecondaryIndexKeyLength(sft: SimpleFeatureType): Int =
    secondaryIndex(sft).map(_.indexKeyLength).getOrElse(0)

  // ranges for querying - equals
  private def equals(sft: SimpleFeatureType,
                     i: Int,
                     shards: Seq[Array[Byte]],
                     value: Any,
                     secondary: Seq[(Array[Byte], Array[Byte])]): Seq[R] = {
    val prefixes = {
      val sharing = sft.getTableSharingBytes
      val index = indexToBytes(i)
      shards.map(shard => Bytes.concat(sharing, index, shard))
    }
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    if (secondary.isEmpty) {
      // if no secondary ranges, use a prefix range terminated with a null byte to match all secondary values
      prefixes.map(prefix => rangePrefix(Bytes.concat(prefix, encoded, NullByteArray)))
    } else {
      prefixes.flatMap { prefix =>
        secondary.map { case (lo, hi) =>
          val start = Bytes.concat(prefix, encoded, NullByteArray, lo)
          val end = IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded, NullByteArray, hi))
          range(start, end)
        }
      }
    }
  }

  private def getSecondaryIndexKey(sft: SimpleFeatureType): (F) => Array[Byte] = {
    secondaryIndex(sft).map(_.toIndexKey(sft)) match {
      case None        => (f) => Array.empty
      case Some(toKey) => (f) => toKey(f.feature)
    }
  }

  private def getSecondaryIndexRanges(sft: SimpleFeatureType,
                                      filter: Filter,
                                      explain: Explainer): Seq[(Array[Byte], Array[Byte])] = {
    secondaryIndex(sft).map { secondary =>
      try { secondary.getRanges(sft, filter, explain).toSeq } finally {
        secondary.clearProcessingValues()
      }
    }.getOrElse(Seq.empty)
  }
}

trait AttributeRowDecoder {

  /**
    * Decodes an attribute value out of row string
    *
    * @param sft simple feature type
    * @param i attribute index
    * @return (bytes, offset, length) => decoded value
    */
  def decodeRowValue(sft: SimpleFeatureType, i: Int): (Array[Byte], Int, Int) => Try[Any]
}

/**
  * Attribute plus date composite index
  */
trait AttributeDateIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R]
    extends AttributeIndex[DS, F, W, R] {

  import AttributeIndex._

  override protected def secondaryIndex(sft: SimpleFeatureType): Option[IndexKeySpace[_]] =
    Some(DateIndexKeySpace).filter(_.supports(sft))

  object DateIndexKeySpace extends IndexKeySpace[Unit] {

    override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined

    override val indexKeyLength: Int = 12

    override def toIndexKey(sft: SimpleFeatureType): (SimpleFeature) => Array[Byte] = {
      val dtgIndex = sft.getDtgIndex.getOrElse(-1)
      (feature) => {
        val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
        timeToBytes(if (dtg == null) { 0L } else { dtg.getTime })
      }
    }

    override def getRanges(sft: SimpleFeatureType,
                           filter: Filter,
                           explain: Explainer): Iterator[(Array[Byte], Array[Byte])] = {
      val intervals = sft.getDtgField.map(FilterHelper.extractIntervals(filter, _)).getOrElse(FilterValues.empty)
      intervals.values.iterator.map { case (lo, hi) =>
        (timeToBytes(lo.getMillis), roundUpTime(timeToBytes(hi.getMillis)))
      }
    }

    // store the first 12 hex chars of the time - that is roughly down to the minute interval
    private def timeToBytes(t: Long): Array[Byte] =
      typeRegistry.encode(t).substring(0, 12).getBytes(StandardCharsets.UTF_8)

    // rounds up the time to ensure our range covers all possible times given our time resolution
    private def roundUpTime(time: Array[Byte]): Array[Byte] = {
      // find the last byte in the array that is not 0xff
      var changeIndex: Int = time.length - 1
      while (changeIndex > -1 && time(changeIndex) == 0xff.toByte) { changeIndex -= 1 }

      if (changeIndex < 0) {
        // the array is all 1s - it's already at time max given our resolution
        time
      } else {
        // increment the selected byte
        time.updated(changeIndex, (time(changeIndex) + 1).asInstanceOf[Byte])
      }
    }
  }
}

object AttributeIndex {

  val NullByte: Byte = 0
  val NullByteArray  = Array(NullByte)

  val typeRegistry   = LexiTypeEncoders.LEXI_TYPES

  // store 2 bytes for the index of the attribute in the sft - this allows up to 32k attributes in the sft.
  def indexToBytes(i: Int): Array[Byte] = Shorts.toByteArray(i.toShort)

  // convert back from bytes to the index of the attribute
  def bytesToIndex(b0: Byte, b1: Byte): Short = Shorts.fromBytes(b0, b1)

  /**
    * Gets the row prefix for a given attribute
    *
    * @param sft simple feature type
    * @param i index of the attribute
    * @return
    */
  private def rowPrefix(sft: SimpleFeatureType, i: Int): Array[Byte] =
    Bytes.concat(sft.getTableSharingBytes, indexToBytes(i))

  /**
    * Lexicographically encode the value. Collections will return multiple rows, one for each entry.
    */
  def encodeForIndex(value: Any, descriptor: AttributeDescriptor): Seq[Array[Byte]] = {
    val strings = if (value == null) {
      Seq.empty
    } else if (descriptor.isList) {
      // encode each value into a separate row
      value.asInstanceOf[JCollection[_]].toSeq.filter(_ != null).map(typeEncode).filter(_ != null)
    } else {
      Seq(typeEncode(value)).filter(_ != null)
    }
    strings.map(_.getBytes(StandardCharsets.UTF_8))
  }

  /**
    * Lexicographically encode the value. Will convert types appropriately.
    */
  def encodeForQuery(value: Any, descriptor: AttributeDescriptor): Array[Byte] =
    if (value == null) { Array.empty } else {
      val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
      val converted = Option(Converters.convert(value, binding)).getOrElse(value)
      val encoded = typeEncode(converted)
      if (encoded == null || encoded.isEmpty) {
        Array.empty
      } else {
        encoded.getBytes(StandardCharsets.UTF_8)
      }
    }

  // Lexicographically encode a value using it's runtime class
  private def typeEncode(value: Any): String = Try(typeRegistry.encode(value)).getOrElse(value.toString)

  /**
    * Decode an encoded value. Note that for collection types, only a single entry of the collection
    * will be decoded - this is because the collection entries have been broken up into multiple rows.
    *
    * @param encoded lexicoded value
    * @param descriptor attribute descriptor
    * @return
    */
  def decode(encoded: String, descriptor: AttributeDescriptor): Any = {
    if (descriptor.isList) {
      // get the alias from the type of values in the collection
      val alias = descriptor.getListType().getSimpleName.toLowerCase(Locale.US)
      Seq(typeRegistry.decode(alias, encoded)).asJava
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, encoded)
    }
  }

  // gets a lower bound for a range
  private def startRows(sft: SimpleFeatureType,
                        attributeIndex: Int,
                        shards: Seq[Array[Byte]],
                        value: Any,
                        inclusive: Boolean,
                        secondary: Array[Byte]): Seq[Array[Byte]] = {
    val prefixes = {
      val prefix = rowPrefix(sft, attributeIndex)
      shards.map(shard => Bytes.concat(prefix, shard))
    }
    val encoded = encodeForQuery(value, sft.getDescriptor(attributeIndex))
    if (inclusive) {
      prefixes.map(prefix => Bytes.concat(prefix, encoded, NullByteArray, secondary))
    } else {
      // get the next row, then append the secondary range
      prefixes.map { prefix =>
        val following = IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded))
        Bytes.concat(following, NullByteArray, secondary)
      }
    }
  }

  // gets an upper bound for a range
  private def endRows(sft: SimpleFeatureType,
                      attributeIndex: Int,
                      shards: Seq[Array[Byte]],
                      value: Any,
                      inclusive: Boolean,
                      secondary: Array[Byte]): Seq[Array[Byte]] = {
    val prefixes = {
      val prefix = rowPrefix(sft, attributeIndex)
      shards.map(shard => Bytes.concat(prefix, shard))
    }
    val encoded = encodeForQuery(value, sft.getDescriptor(attributeIndex))
    if (inclusive) {
      // append secondary range, then get the next row - this will match anything with the same value, up to the secondary
      prefixes.map(prefix => IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded, NullByteArray, secondary)))
    } else {
      // can't use secondary range on an exclusive upper, as there aren't any methods to calculate previous rows
      prefixes.map(prefix => Bytes.concat(prefix, encoded, NullByteArray))
    }
  }

  // lower bound for all values of the attribute, inclusive
  private def lowerBounds(sft: SimpleFeatureType, i: Int, shards: Seq[Array[Byte]]): Seq[Array[Byte]] = {
    val prefix = rowPrefix(sft, i)
    shards.map(shard => Bytes.concat(prefix, shard))
  }

  // upper bound for all values of the attribute, exclusive
  private def upperBounds(sft: SimpleFeatureType, i: Int, shards: Seq[Array[Byte]]): Seq[Array[Byte]] = {
    val prefix = rowPrefix(sft, i)
    shards.map(shard => IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, shard)))
  }
}
