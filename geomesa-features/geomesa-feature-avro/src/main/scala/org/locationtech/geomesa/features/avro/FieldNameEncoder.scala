/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.codec.binary.Hex
import org.locationtech.geomesa.features.avro.FieldNameEncoder._

import scala.collection.JavaConversions._

class FieldNameEncoder(serializationVersion: Int, forceFullEncoding: Boolean = false) {
  private val m = if (serializationVersion < 4 || forceFullEncoding) cachePreV4 else cacheV4

  private val encodeFn: String => String = if (serializationVersion < 4 || forceFullEncoding) encodePreV4 else hexEscape
  private val decodeFn: String => String = if (serializationVersion < 4 || forceFullEncoding) decodePreV4 else deHexEscape

  def encode(s: String): String = m.getOrElseUpdate(s, encodeFn(s))
  def decode(s: String): String = m.getOrElseUpdate(s, decodeFn(s))
}

object FieldNameEncoder {
  // Need a separate cache for reading old data files generated before v4
  val cacheV4    = new ConcurrentHashMap[String, String]()
  val cachePreV4 = new ConcurrentHashMap[String, String]()

  def encodePreV4(s: String): String = "_" + Hex.encodeHexString(s.getBytes("UTF8"))

  def decodePreV4(s: String): String = new String(Hex.decodeHex(s.substring(1).toCharArray), "UTF8")

  def hexEscape(s: String): String = {
    val sb = new StringBuilder
    s.toCharArray.foreach {
      case c if c.isLetterOrDigit => sb.append(c)
      case c =>
        val encoded = "_" + Hex.encodeHexString(c.toString.getBytes("UTF8")).toLowerCase
        sb.append(encoded)
    }
    sb.toString()
  }

  def deHexEscape(s: String): String = {
    val chars = s.toCharArray
    var idx = 0
    val sb = new StringBuilder
    while (idx < chars.length) {
      if (chars(idx).isLetterOrDigit) {
        sb.append(chars(idx))
        idx += 1
      } else {
        sb.append(new String(Hex.decodeHex(Array(chars(idx + 1), chars(idx + 2))), "UTF8"))
        idx += 3
      }
    }
    sb.toString()
  }

  private def isSafeName(s: String): Boolean =  s.toCharArray.forall(_.isLetterOrDigit)

}