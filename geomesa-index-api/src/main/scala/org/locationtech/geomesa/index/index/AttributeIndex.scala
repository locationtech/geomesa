/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets
import java.util.{Date, Locale, Collection => JCollection}

import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.LazyLogging
import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.util.Converters
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

trait AttributeIndex[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] extends GeoMesaFeatureIndex[DS, F, W, Q]
    with IndexAdapter[DS, F, W, Q, R] with AttributeFilterStrategy[DS, F, W, Q] with LazyLogging {

  import AttributeIndex._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getAttributeDescriptors.exists(_.isIndexed)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft)
    (wf) => getRows(wf).map(createInsert(_, wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val getRows = getRowKeys(sft)
    (wf) => getRows(wf).map(createDelete(_, wf))
  }

  /**
    * Rows in the attribute table have the following layout:
    *
    * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
    * - 2 bytes storing the index of the attribute in the sft
    * - n bytes storing the lexicoded attribute value
    * - NULLBYTE as a separator
    * - 12 bytes storing the dtg of the feature (OPTIONAL - only if the sft has a dtg field)
    * - n bytes storing the feature ID
    */
  private def getRowKeys(sft: SimpleFeatureType): (F) => Seq[Array[Byte]] = {
    val prefix = sft.getTableSharingBytes
    val getSuffix: (F) => Array[Byte] = sft.getDtgIndex match {
      case None => (wf: F) => wf.feature.getID.getBytes(StandardCharsets.UTF_8)
      case Some(dtgIndex) =>
        (wf: F) => {
          val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
          val timeBytes = timeToBytes(if (dtg == null) 0L else dtg.getTime)
          val idBytes = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
          Bytes.concat(timeBytes, idBytes)
        }
    }
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map { d =>
      val i = sft.indexOf(d.getName)
      (d, i, indexToBytes(i))
    }

    (wf) => {
      val suffix = getSuffix(wf)
      indexedAttributes.flatMap { case (descriptor, idx, idxBytes) =>
        val attributes = encodeForIndex(wf.feature.getAttribute(idx), descriptor)
        attributes.map(a => Bytes.concat(prefix, idxBytes, a, NullByteArray, suffix))
      }
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    // drop the encoded value and the date field (12 bytes) if it's present - the rest of the row is the ID
    val from = if (sft.isTableSharing) 3 else 2  // exclude feature byte and 2 index bytes
    val prefix = if (sft.getDtgField.isDefined) 13 else 1
    (row, offset, length) => {
      val start = row.indexOf(NullByte, from + offset) + prefix
      new String(row, start, length + offset - start, StandardCharsets.UTF_8)
    }
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    val sharing = sft.getTableSharingBytes
    val indices = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => sft.indexOf(d.getLocalName))
    indices.map(i => Bytes.concat(sharing, indexToBytes(i)))
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W, Q],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W, Q] = {
    val primary = filter.primary.getOrElse {
      throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
    }

    lazy val disjointDates = (Long.MinValue, Long.MinValue)

    // pull out any dates from the filter to help narrow down the attribute ranges
    val dates = for {
      dtgField  <- sft.getDtgField
      secondary <- filter.secondary
      intervals = FilterHelper.extractIntervals(secondary, dtgField)
      if intervals.nonEmpty
    } yield {
      if (intervals == FilterHelper.DisjointInterval) {
        disjointDates
      } else {
        (intervals.map(_._1.getMillis).min, intervals.map(_._2.getMillis).max)
      }
    }

    if (dates.exists(_ == disjointDates)) {
      // empty plan
      scanPlan(sft, ds, filter, hints, Seq.empty, None)
    } else {
      scanPlan(sft, ds, filter, hints, getRanges(sft, primary, dates), filter.secondary)
    }
  }

  /**
    * Gets the property name from the filter and a range that covers the filter in the attribute table.
    * Note that if the filter is not a valid attribute filter this method will throw an exception.
    *
    */
  def getRanges(sft: SimpleFeatureType, filter: Filter, dates: Option[(Long, Long)]): Seq[R] = {
    // TODO GEOMESA-1336 fix exclusive AND handling for list types

    val attribute = {
      val names = DataUtilities.attributeNames(filter)
      require(names.length == 1, s"Couldn't extract single attribute name from filter '${filterToString(filter)}'")
      names(0)
    }

    val i = sft.indexOf(attribute)
    require(i != -1, s"Attribute '$attribute' from filter '${filterToString(filter)}' does not exist in '$sft'")

    val binding = {
      val descriptor = sft.getDescriptor(i)
      if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
    }

    require(classOf[Comparable[_]].isAssignableFrom(binding), s"Attribute '$attribute' is not comparable")

    val fb = FilterHelper.extractAttributeBounds(filter, attribute, binding).getOrElse {
      throw new RuntimeException(s"Unhandled filter type in attribute strategy: ${filterToString(filter)}")
    }

    lazy val lowerDate = dates.map(_._1)
    lazy val upperDate = dates.map(_._2)

    fb.bounds.map { bounds =>
      bounds.bounds match {
        case (None, None) => range(lowerBound(sft, i), upperBound(sft, i)) // not null
        case (Some(lower), None) => range(startRow(sft, i, lower, bounds.inclusive, lowerDate), upperBound(sft, i))
        case (None, Some(upper)) => range(lowerBound(sft, i), endRow(sft, i, upper, bounds.inclusive, upperDate))
        case (Some(lower), Some(upper)) =>
          if (lower == upper) {
            equals(sft, i, lower, dates)
          } else if (lower + WILDCARD_SUFFIX == upper) {
            rangePrefix(Bytes.concat(rowPrefix(sft, i), encodeForQuery(lower, sft.getDescriptor(i))))
          } else {
            val start = startRow(sft, i, lower, bounds.inclusive, lowerDate)
            val end = endRow(sft, i, upper, bounds.inclusive, upperDate)
            range(start, end)
          }
      }
    }
  }

  // ranges for querying - equals
  private def equals(sft: SimpleFeatureType, i: Int, value: Any, times: Option[(Long, Long)]): R = {
    val prefix = Bytes.concat(sft.getTableSharingBytes, indexToBytes(i))
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    times match {
      case None =>
        // if no time, use a prefix range terminated with a null byte to match all times
        rangePrefix(Bytes.concat(prefix, encoded, NullByteArray))
      case Some((t1, t2)) =>
        val (t1Bytes, t2Bytes) = (timeToBytes(t1), roundUpTime(timeToBytes(t2)))
        val start = Bytes.concat(prefix, encoded, NullByteArray, t1Bytes)
        val end = IndexAdapter.rowFollowingRow(Bytes.concat(prefix, encoded, NullByteArray, t2Bytes))
        range(start, end)
    }
  }
}


object AttributeIndex {

  private val NullByte: Byte = 0
  private val NullByteArray  = Array(NullByte)

  private val typeRegistry   = LexiTypeEncoders.LEXI_TYPES

  // store 2 bytes for the index of the attribute in the sft - this allows up to 32k attributes in the sft.
  def indexToBytes(i: Int) = Array((i << 8).asInstanceOf[Byte], i.asInstanceOf[Byte])

  // store the first 12 hex chars of the time - that is roughly down to the minute interval
  def timeToBytes(t: Long) = typeRegistry.encode(t).substring(0, 12).getBytes(StandardCharsets.UTF_8)

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

  /**
    * Gets the row prefix for a given attribute
    * @param sft simple feature type
    * @param i index of the attribute
    * @return
    */
  private def rowPrefix(sft: SimpleFeatureType, i: Int): Array[Byte] =
    Bytes.concat(sft.getTableSharingBytes, indexToBytes(i))

  /**
    * Decodes an attribute value out of row string
    */
  def decodeRow(sft: SimpleFeatureType, i: Int, row: Array[Byte]): Try[Any] = Try {
    val from = if (sft.isTableSharing) 3 else 2 // exclude feature byte and index bytes
    // null byte indicates end of value
    val length = row.indexOf(NullByte, from + 1) - from
    val encoded = new String(row, from, length, StandardCharsets.UTF_8)
    decode(encoded, sft.getDescriptor(i))
  }

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

  // gets a lower bound for a range (inclusive)
  private def startRow(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Array[Byte] = {
    val prefix = rowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    val timeBytes = time.map(timeToBytes).getOrElse(Array.empty)
    if (inclusive) {
      Bytes.concat(prefix, encoded, NullByteArray, timeBytes)
    } else {
      // get the next row, then append the time
      val following = IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded))
      Bytes.concat(following, NullByteArray, timeBytes)
    }
  }

  // gets an upper bound for a range (exclusive)
  private def endRow(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Array[Byte] = {
    val prefix = rowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    if (inclusive) {
      // append time, then get the next row - this will match anything with the same value, up to the time
      val timeBytes = time.map(t => roundUpTime(timeToBytes(t))).getOrElse(Array.empty)
      IndexAdapter.rowFollowingPrefix(Bytes.concat(prefix, encoded, NullByteArray, timeBytes))
    } else {
      // can't use time on an exclusive upper, as there aren't any methods to calculate previous rows
      Bytes.concat(prefix, encoded, NullByteArray)
    }
  }

  // lower bound for all values of the attribute, inclusive
  private def lowerBound(sft: SimpleFeatureType, i: Int): Array[Byte] = rowPrefix(sft, i)

  // upper bound for all values of the attribute, exclusive
  private def upperBound(sft: SimpleFeatureType, i: Int): Array[Byte] =
    IndexAdapter.rowFollowingPrefix(rowPrefix(sft, i))
}
