/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.commons.codec.binary.Hex
import org.locationtech.geomesa.features.avro.FieldNameEncoder._

import java.util.concurrent.ConcurrentHashMap

class FieldNameEncoder(serializationVersion: Int, forceFullEncoding: Boolean = false) {

  private val cache = new ConcurrentHashMap[String, String]()

  private val encodeFn = {
    val fn: String => String = if (serializationVersion < 4 || forceFullEncoding) { encodePreV4 } else { hexEscape }
    new java.util.function.Function[String, String]() {
      override def apply(t: String): String = fn(t)
    }
  }

  private val decodeFn = {
    val fn: String => String = if (serializationVersion < 4 || forceFullEncoding) { decodePreV4 } else { deHexEscape }
    new java.util.function.Function[String, String]() {
      override def apply(t: String): String = fn(t)
    }
  }

  def encode(s: String): String = cache.computeIfAbsent(s, encodeFn)
  def decode(s: String): String = cache.computeIfAbsent(s, decodeFn)
}

object FieldNameEncoder {

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
}
