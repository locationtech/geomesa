/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.regex.Pattern
import java.util.{Date, Locale}

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Hex
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}
import org.opengis.feature.simple.SimpleFeatureType

import org.locationtech.geomesa.utils.date.DateUtils.toInstant

object StringSerialization extends LazyLogging {

  private val dateFormat: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)

  private val AlphaNumericPattern = Pattern.compile("^[a-zA-Z0-9]+$")
  private val AlphaNumeric = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  /**
    * Encode a map of sequences as a string
    *
    * @param map map of keys to sequences of values
    * @return
    */
  def encodeSeqMap(map: Map[String, Seq[AnyRef]]): String = {
    val sb = new java.lang.StringBuilder
    val printer = new CSVPrinter(sb, CSVFormat.DEFAULT)
    map.foreach { case (k, v) =>
      val strings = v.headOption match {
        case Some(_: Date) => v.map(d => ZonedDateTime.ofInstant(toInstant(d.asInstanceOf[Date]), ZoneOffset.UTC).format(dateFormat))
        case _ => v
      }
      printer.print(k)
      strings.foreach(printer.print)
      printer.println()
    }
    sb.toString
  }


  /**
    * Decode a map of sequences from a string encoded by @see encodeSeqMap
    *
    * @param encoded encoded map
    * @return decoded map
    */
  def decodeSeqMap(sft: SimpleFeatureType, encoded: String): Map[String, Array[AnyRef]] = {
    import scala.collection.JavaConversions._
    val bindings = sft.getAttributeDescriptors.map(d => d.getLocalName -> d.getType.getBinding)
    decodeSeqMap(encoded, bindings.toMap[String, Class[_]])
  }

  /**
    * Decode a map of sequences from a string encoded by @see encodeSeqMap
    *
    * @param encoded encoded map
    * @return decoded map
    */
  def decodeSeqMap(encoded: String, bindings: Map[String, Class[_]]): Map[String, Array[AnyRef]] = {
    import scala.collection.JavaConversions._
    // encoded as CSV, first element of each row is key, rest is value
    val parser = CSVParser.parse(encoded, CSVFormat.DEFAULT)
    parser.iterator.map { record =>
      val iter = record.iterator
      val key = iter.next
      val values = bindings.get(key) match {
        case Some(c) if c == classOf[String]              => iter.toArray[AnyRef]
        case Some(c) if c == classOf[Integer]             => iter.map(Integer.valueOf).toArray[AnyRef]
        case Some(c) if c == classOf[java.lang.Long]      => iter.map(java.lang.Long.valueOf).toArray[AnyRef]
        case Some(c) if c == classOf[java.lang.Float]     => iter.map(java.lang.Float.valueOf).toArray[AnyRef]
        case Some(c) if c == classOf[java.lang.Double]    => iter.map(java.lang.Double.valueOf).toArray[AnyRef]
        case Some(c) if classOf[Date].isAssignableFrom(c) => iter.map(v => Date.from(ZonedDateTime.parse(v, dateFormat).toInstant)).toArray[AnyRef]
        case Some(c) if c == classOf[java.lang.Boolean]   => iter.map(java.lang.Boolean.valueOf).toArray[AnyRef]
        case c => logger.warn(s"No conversion defined for encoded attribute '$key' of type ${c.orNull}"); iter.toArray[AnyRef]
      }
      key -> values
    }.toMap
  }

  /**
    * Encode non-alphanumeric characters in a string with
    * underscore plus hex digits representing the bytes. Note
    * that multibyte characters will be represented with multiple
    * underscores and bytes...e.g. _8a_2f_3b
    */
  def alphaNumericSafeString(input: String): String = {
    if (AlphaNumericPattern.matcher(input).matches()) { input } else {
      val sb = new StringBuilder
      input.foreach { c =>
        if (AlphaNumeric.contains(c)) { sb.append(c) } else {
          val hex = Hex.encodeHex(c.toString.getBytes(StandardCharsets.UTF_8))
          val encoded = hex.grouped(2).map(arr => "_" + arr(0) + arr(1)).mkString.toLowerCase(Locale.US)
          sb.append(encoded)
        }
      }
      sb.toString()
    }
  }

  def decodeAlphaNumericSafeString(input: String): String = {
    if (AlphaNumericPattern.matcher(input).matches()) { input } else {
      val sb = new StringBuilder
      var i = 0
      while (i < input.length) {
        val c = input.charAt(i)
        if (c != '_') { sb.append(c) } else {
          i += 2
          sb.append(Hex.decodeHex(Array(input.charAt(i - 1), input.charAt(i))))
        }
        i += 1
      }
      sb.toString()
    }
  }
}
