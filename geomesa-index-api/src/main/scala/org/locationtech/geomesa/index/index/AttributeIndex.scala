/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets
import java.util.{Locale, Collection => JCollection}

import com.google.common.primitives.{Bytes, Shorts, UnsignedBytes}
import com.typesafe.scalalogging.LazyLogging
import org.calrissian.mango.types.{LexiTypeEncoders, TypeRegistry}
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.util.Converters
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.AttributeIndex.AttributeRowDecoder
import org.locationtech.geomesa.index.index.z2.{XZ2IndexKeySpace, Z2IndexKeySpace}
import org.locationtech.geomesa.index.index.z3.{XZ3IndexKeySpace, Z3IndexKeySpace}
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Attribute index with secondary z-curve indexing. Z-indexing is based on the sft and will be
  * one of Z3, XZ3, Z2, XZ2.
  */
trait AttributeIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends GeoMesaFeatureIndex[DS, F, W] with IndexAdapter[DS, F, W, R, C] with AttributeFilterStrategy[DS, F, W]
    with AttributeRowDecoder with LazyLogging {

  import AttributeIndex._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = AttributeIndex.Name

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getAttributeDescriptors.exists(_.isIndexed)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft, lenient = false)
    (wf) => getRows(wf).map { case (_, r) => createInsert(r, wf) }
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft, lenient = true)
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

  override def decodeRowValue(sft: SimpleFeatureType, index: Int): (Array[Byte], Int, Int) => Try[AnyRef] = {
    val shard = getShards(sft).head.length
    // exclude feature byte and 2 index bytes and shard bytes
    val from = if (sft.isTableSharing) { 3 + shard } else { 2 + shard }
    val descriptor = sft.getDescriptor(index)
    val decode: (String) => AnyRef = if (descriptor.isList) {
      // get the alias from the type of values in the collection
      val alias = descriptor.getListType().getSimpleName.toLowerCase(Locale.US)
      // Note that for collection types, only a single entry of the collection will be decoded - this is
      // because the collection entries have been broken up into multiple rows
      (encoded) => Seq(typeRegistry.decode(alias, encoded)).asJava
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, _)
    }
    (row, offset, length) => Try {
      val valueStart = offset + from // start of the encoded value
      val end = offset + length // end of the row, max search space
      var valueEnd = valueStart // end of the encoded value
      while (valueEnd < end && row(valueEnd) != NullByte) { // null byte indicates end of value
        valueEnd += 1
      }
      decode(new String(row, valueStart, valueEnd - valueStart, StandardCharsets.UTF_8))
    }
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val sharing = sft.getTableSharingBytes
    val indices = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => sft.indexOf(d.getLocalName))
    val shards = nonEmpty(getShards(sft))

    val splitter = sft.getTableSplitter.getOrElse(classOf[DefaultSplitter]).newInstance().asInstanceOf[TableSplitter]
    val result = indices.flatMap { indexOf =>
      val singleAttributeType = {
        val builder = new SimpleFeatureTypeBuilder()
        builder.setName(sft.getName)
        builder.add(sft.getDescriptor(indexOf))
        builder.buildFeatureType()
      }
      val bytes = indexToBytes(indexOf)
      val splits = nonEmpty(splitter.getSplits(singleAttributeType, name, sft.getTableSplitterOptions))
      for (shard <- shards; split <- splits) yield {
        Bytes.concat(sharing, bytes, shard, split)
      }
    }

    // if not sharing, or the first feature in the table, drop the first split, which will otherwise be empty
    if (sharing.isEmpty || sharing.head == 0.toByte) {
      result.drop(1)
    } else {
      result
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

    val shards = getShards(sft)
    val fb = FilterHelper.extractAttributeBounds(primary, attribute, binding)

    if (fb.isEmpty) {
      // we have an attribute, but weren't able to extract any bounds... scan all values and apply the filter
      logger.warn(s"Unable to extract any attribute bounds from: ${filterToString(primary)}")
      val starts = lowerBounds(sft, i, shards)
      val ends = upperBounds(sft, i, shards)
      val ranges = shards.indices.map(i => range(starts(i), ends(i)))
      scanPlan(sft, ds, filter, scanConfig(sft, ds, filter, ranges, filter.filter, hints))
    } else {
      val ordering = Ordering.comparatorToOrdering(UnsignedBytes.lexicographicalComparator)
      lazy val lowerSecondary = secondaryRanges.map(_._1).minOption(ordering).getOrElse(Array.empty)
      lazy val upperSecondary = secondaryRanges.map(_._2).maxOption(ordering).getOrElse(Array.empty)

      val ranges = fb.values.flatMap { bounds =>
        bounds.bounds match {
          case (None, None) => // not null
            val starts = lowerBounds(sft, i, shards)
            val ends = upperBounds(sft, i, shards)
            shards.indices.map(i => range(starts(i), ends(i)))

          case (Some(lower), None) =>
            val starts = startRows(sft, i, shards, lower, bounds.lower.inclusive, lowerSecondary)
            val ends = upperBounds(sft, i, shards)
            shards.indices.map(i => range(starts(i), ends(i)))

          case (None, Some(upper)) =>
            val starts = lowerBounds(sft, i, shards)
            val ends = endRows(sft, i, shards, upper, bounds.upper.inclusive, upperSecondary)
            shards.indices.map(i => range(starts(i), ends(i)))

          case (Some(lower), Some(upper)) =>
            if (lower == upper) {
              equals(sft, i, shards, lower, secondaryRanges)
            } else if (lower + WILDCARD_SUFFIX == upper) {
              val prefix = rowPrefix(sft, i)
              val value = encodeForQuery(lower, sft.getDescriptor(i))
              shards.map(shard => rangePrefix(Bytes.concat(prefix, shard, value)))
            } else {
              val starts = startRows(sft, i, shards, lower, bounds.lower.inclusive, lowerSecondary)
              val ends = endRows(sft, i, shards, upper, bounds.upper.inclusive, upperSecondary)
              shards.indices.map(i => range(starts(i), ends(i)))
            }
        }
      }

      val ecql = if (fb.precise) { filter.secondary } else { filter.filter }
      scanPlan(sft, ds, filter, scanConfig(sft, ds, filter, ranges, ecql, hints))
    }
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
  protected def getRowKeys(sft: SimpleFeatureType, lenient: Boolean): (F) => Seq[(Int, Array[Byte])] = {
    val prefix = sft.getTableSharingBytes
    val getSecondaryKey = getSecondaryIndexKey(sft, lenient)
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
    Seq(Z3IndexKeySpace, XZ3IndexKeySpace, Z2IndexKeySpace, XZ2IndexKeySpace).find(_.supports(sft))

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
          val end = Bytes.concat(prefix, encoded, NullByteArray, hi)
          range(start, end)
        }
      }
    }
  }

  private def getSecondaryIndexKey(sft: SimpleFeatureType, lenient: Boolean): (F) => Array[Byte] = {
    secondaryIndex(sft).map(_.toIndexKey(sft, lenient)) match {
      case None        => (_) => Array.empty
      case Some(toKey) => (f) => toKey(f.feature)
    }
  }

  private def getSecondaryIndexRanges(sft: SimpleFeatureType,
                                      filter: Filter,
                                      explain: Explainer): Seq[(Array[Byte], Array[Byte])] = {
    secondaryIndex(sft).map { secondary =>
      val indexValues = secondary.getIndexValues(sft, filter, explain)
      secondary.asInstanceOf[IndexKeySpace[Any]].getRanges(sft, indexValues).toSeq
    }.getOrElse(Seq.empty)
  }
}

object AttributeIndex {

  val Name = "attr"

  val NullByte: Byte = 0
  val NullByteArray = Array(NullByte)

  val typeRegistry: TypeRegistry[String] = LexiTypeEncoders.LEXI_TYPES

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
    encodeForQuery(value, if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding })

  /**
    * Lexicographically encode the value. Will convert types appropriately.
    */
  def encodeForQuery(value: Any, binding: Class[_]): Array[Byte] = {
    if (value == null) { Array.empty } else {
      val converted = Option(Converters.convert(value, binding)).getOrElse(value)
      val encoded = typeEncode(converted)
      if (encoded == null || encoded.isEmpty) {
        Array.empty
      } else {
        encoded.getBytes(StandardCharsets.UTF_8)
      }
    }
  }

  // Lexicographically encode a value using it's runtime class
  private def typeEncode(value: Any): String = Try(typeRegistry.encode(value)).getOrElse(value.toString)

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
      if (secondary.length == 0) {
        // use a rowFollowingPrefix to match any secondary values
        prefixes.map(prefix => IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded, NullByteArray)))
      } else {
        // matches anything with the same value, up to the secondary
        prefixes.map(prefix => Bytes.concat(prefix, encoded, NullByteArray, secondary))
      }
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
}
