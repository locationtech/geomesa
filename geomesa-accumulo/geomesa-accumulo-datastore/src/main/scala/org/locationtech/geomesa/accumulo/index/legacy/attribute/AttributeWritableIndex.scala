/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.index.legacy.attribute

import java.nio.charset.StandardCharsets
import java.util.{Date, Locale, Collection => JCollection}

import com.google.common.collect.ImmutableSortedSet
import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.{LexiTypeEncoders, SimpleTypeEncoders, TypeEncoder}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AttributeSplittable}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Contains logic for converting between accumulo and geotools for the attribute index
 */
trait AttributeWritableIndex extends AccumuloFeatureIndex with AttributeSplittable with LazyLogging {

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    val indices = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map(d => sft.indexOf(d.getLocalName))
    indices.map(i => AttributeWritableIndex.getRowPrefix(sft, i))
  }

  override def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    val splits = getSplits(sft)
    if (splits.nonEmpty) {
      val texts = splits.map(new Text(_))
      ds.tableOps.addSplits(getTableName(sft.getTypeName, ds), ImmutableSortedSet.copyOf(texts.toArray))
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    // drop the encoded value and the date field (12 bytes) if it's present - the rest of the row is the ID
    val from = if (sft.isTableSharing) 3 else 2  // exclude feature byte and index bytes
    val prefix = if (sft.getDtgField.isDefined) 13 else 1
    (row, offset, length) => {
      val start = row.indexOf(AttributeWritableIndex.NullByteArray.head, from + offset) + prefix
      new String(row, start, length + offset - start, StandardCharsets.UTF_8)
    }
  }

  def getRowKeys(sft: SimpleFeatureType): (AccumuloFeature) => Seq[(AttributeDescriptor, Array[Byte])] = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).map { d =>
      val i = sft.indexOf(d.getName)
      (d, i, AttributeWritableIndex.indexToBytes(i))
    }
    val prefix = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    val getSuffix: (AccumuloFeature) => Array[Byte] = sft.getDtgIndex match {
      case None => (fw: AccumuloFeature) => fw.feature.getID.getBytes(StandardCharsets.UTF_8)
      case Some(dtgIndex) =>
        (fw: AccumuloFeature) => {
          val dtg = fw.feature.getAttribute(dtgIndex).asInstanceOf[Date]
          val timeBytes = AttributeWritableIndex.timeToBytes(if (dtg == null) 0L else dtg.getTime)
          val idBytes = fw.feature.getID.getBytes(StandardCharsets.UTF_8)
          Bytes.concat(timeBytes, idBytes)
        }
    }
    AttributeWritableIndex.getRowKeys(indexedAttributes, prefix, getSuffix)
  }
}

object AttributeWritableIndex extends LazyLogging {

  private val NullByte = "\u0000"
  private val NullByteArray = NullByte.getBytes(StandardCharsets.UTF_8)

  private val typeRegistry   = LexiTypeEncoders.LEXI_TYPES
  private val simpleEncoders = SimpleTypeEncoders.SIMPLE_TYPES.getAllEncoders
  private val dateFormat     = ISODateTimeFormat.dateTime()

  private type TryEncoder = Try[(TypeEncoder[Any, String], TypeEncoder[_, String])]

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
  private def getRowKeys(indexedAttributes: Seq[(AttributeDescriptor, Int, Array[Byte])],
                         prefix: Array[Byte],
                         suffix: (AccumuloFeature) => Array[Byte])
                        (fw: AccumuloFeature): Seq[(AttributeDescriptor, Array[Byte])] = {
    val suffixBytes = suffix(fw)
    indexedAttributes.flatMap { case (descriptor, idx, idxBytes) =>
      val attributes = encodeForIndex(fw.feature.getAttribute(idx), descriptor)
      attributes.map { a =>
        (descriptor, Bytes.concat(prefix, idxBytes, a.getBytes(StandardCharsets.UTF_8), NullByteArray, suffixBytes))
      }
    }
  }

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
   * Gets a prefix for an attribute row - this includes the sft and the attribute index only
   */
  def getRowPrefix(sft: SimpleFeatureType, i: Int): Array[Byte] =
    Bytes.concat(sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8), indexToBytes(i))

  // ranges for querying - equals
  def equals(sft: SimpleFeatureType, i: Int, value: Any, times: Option[(Long, Long)]): AccRange = {
    val prefix = getRowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    times match {
      case None =>
        // if no time, use a prefix range terminated with a null byte to match all times
        AccRange.prefix(new Text(Bytes.concat(prefix, encoded, NullByteArray)))
      case Some((t1, t2)) =>
        val (t1Bytes, t2Bytes) = (timeToBytes(t1), roundUpTime(timeToBytes(t2)))
        val start = new Text(Bytes.concat(prefix, encoded, NullByteArray, t1Bytes))
        val end = new Text(Bytes.concat(prefix, encoded, NullByteArray, t2Bytes))
        new AccRange(start, true, end, true)
    }
  }

  // ranges for querying - less than
  def lt(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lowerBound(sft, i), true, upper(sft, i, value, inclusive = false, time), false)

  // ranges for querying - less than or equal to
  def lte(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lowerBound(sft, i), true, upper(sft, i, value, inclusive = true, time), false)

  // ranges for querying - greater than
  def gt(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lower(sft, i, value, inclusive = false, time), true, upperBound(sft, i), false)

  // ranges for querying - greater than or equal to
  def gte(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lower(sft, i, value, inclusive = true, time), true, upperBound(sft, i), false)

  // ranges for querying - between
  def between(sft: SimpleFeatureType,
              i: Int,
              values: (Any, Any),
              times: Option[(Long, Long)],
              inclusive: Boolean): AccRange = {
    val start = lower(sft, i, values._1, inclusive, times.map(_._1))
    val end = upper(sft, i, values._2, inclusive, times.map(_._2))
    new AccRange(start, true, end, false)
  }

  // ranges for querying - prefix (doesn't account for time)
  def prefix(sft: SimpleFeatureType, i: Int, value: Any): AccRange = {
    val prefix = getRowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    AccRange.prefix(new Text(Bytes.concat(prefix, encoded)))
  }

  // ranges for querying - all values for this attribute
  def all(sft: SimpleFeatureType, i: Int): AccRange =
    new AccRange(lowerBound(sft, i), true, upperBound(sft, i), false)

  // gets a lower bound for a range (inclusive)
  private def lower(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Text = {
    val prefix = getRowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    val timeBytes = time.map(timeToBytes).getOrElse(Array.empty)
    if (inclusive) {
      new Text(Bytes.concat(prefix, encoded, NullByteArray, timeBytes))
    } else {
      // get the next row, then append the time
      val following = AccRange.followingPrefix(new Text(Bytes.concat(prefix, encoded))).getBytes
      new Text(Bytes.concat(following, NullByteArray, timeBytes))
    }
  }

  // gets an upper bound for a range (exclusive)
  private def upper(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Text = {
    val prefix = getRowPrefix(sft, i)
    val encoded = encodeForQuery(value, sft.getDescriptor(i))
    if (inclusive) {
      // append time, then get the next row - this will match anything with the same value, up to the time
      val timeBytes = time.map(t => roundUpTime(timeToBytes(t))).getOrElse(Array.empty)
      AccRange.followingPrefix(new Text(Bytes.concat(prefix, encoded, NullByteArray, timeBytes)))
    } else {
      // can't use time on an exclusive upper, as there aren't any methods to calculate previous rows
      new Text(Bytes.concat(prefix, encoded, NullByteArray))
    }
  }

  // lower bound for all values of the attribute, inclusive
  private def lowerBound(sft: SimpleFeatureType, i: Int): Text = new Text(getRowPrefix(sft, i))

  // upper bound for all values of the attribute, exclusive
  private def upperBound(sft: SimpleFeatureType, i: Int): Text =
    AccRange.followingPrefix(new Text(getRowPrefix(sft, i)))

  /**
   * Decodes an attribute value out of row string
   */
  def decodeRow(sft: SimpleFeatureType, i: Int, row: Text): Try[Any] = Try {
    val from = if (sft.isTableSharing) 3 else 2 // exclude feature byte and index bytes
    // null byte indicates end of value
    val rawBytes = row.getBytes
    val length = rawBytes.indexOf(NullByteArray(0), from + 1) - from
    val encoded = new String(rawBytes, from, length, StandardCharsets.UTF_8)
    decode(encoded, sft.getDescriptor(i))
  }

  /**
   * Lexicographically encode the value. Collections will return multiple rows, one for each entry.
   */
  def encodeForIndex(value: Any, descriptor: AttributeDescriptor): Seq[String] =
    if (value == null) {
      Seq.empty
    } else if (descriptor.isList) {
      // encode each value into a separate row
      value.asInstanceOf[JCollection[_]].toSeq.filter(_ != null).map(typeEncode)
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Seq.empty
    } else {
      Seq(typeEncode(value))
    }

  /**
   * Lexicographically encode the value. Will convert types appropriately.
   */
  def encodeForQuery(value: Any, descriptor: AttributeDescriptor): Array[Byte] =
    if (value == null) {
      Array.empty
    } else {
      val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
      val converted = convertType(value, value.getClass, binding)
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
   * @param encoded
   * @param descriptor
   * @return
   */
  def decode(encoded: String, descriptor: AttributeDescriptor): Any = {
    if (descriptor.isList) {
      // get the alias from the type of values in the collection
      val alias = descriptor.getListType().getSimpleName.toLowerCase(Locale.US)
      Seq(typeRegistry.decode(alias, encoded)).asJava
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Map.empty.asJava
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, encoded)
    }
  }

  /**
   * Tries to convert a value from one class to another. When querying attributes, the query
   * literal has to match the class of the attribute for lexicoding to work.
   *
   * @param value
   * @param current
   * @param desired
   * @return
   */
  def convertType(value: Any, current: Class[_], desired: Class[_]): Any = {
    if (current == desired) {
      return value
    }
    val result = if (desired == classOf[Date] && current == classOf[String]) {
      // try to parse the string as a date - right now we support just ISO format
      Try(dateFormat.parseDateTime(value.asInstanceOf[String]).toDate)
    } else {
      // cheap way to convert between basic classes (string, int, double, etc) - encode the value
      // to a string and then decode to the desired class
      val encoderOpt = simpleEncoders.find(_.resolves() == current)
      val decoderOpt = simpleEncoders.find(_.resolves() == desired)
      (encoderOpt, decoderOpt) match {
        case (Some(e), Some(d)) => Try(d.decode(e.asInstanceOf[TypeEncoder[Any, String]].encode(value)))
        case _ => Failure(new RuntimeException("No matching encoder/decoder"))
      }
    }
    result match {
      case Success(converted) => converted
      case Failure(e) =>
        logger.warn(s"Error converting type for '$value' from ${current.getSimpleName} to " +
            s"${desired.getSimpleName}: ${e.toString}")
        value
    }
  }
}
