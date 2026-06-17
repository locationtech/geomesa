/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import org.apache.commons.codec.binary.Hex

import java.nio.charset.StandardCharsets
import java.util.Locale

// TODO verify/clean this up

/**
 * Provides mappings to and from attribute names and valid parquet column names
 */
object ColumnName {

  private val AlphaNumeric = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  /**
   * Convert an attribute name into a valid parquet column name
   *
   * @param name attribute name
   * @return
   */
  def apply(name: String): String = {
    if (name.startsWith("__")) {
      // internal field (fid, bbox, etc)
      name
    } else {
      // TODO make this a little cleaner
      var i = 0
      var isUnderscore = false
      while (i < name.length) {
        name.charAt(i) match {
          case '_' if isUnderscore => return encode(name, i, isUnderscore = true)
          case '_' => isUnderscore = true
          case c if AlphaNumeric.contains(c) => isUnderscore = false
          case _ => return encode(name, i, isUnderscore = false)
        }
        i += 1
      }
      name
    }
  }

  /**
   * Convert from a parquet column name back into an attribute name
   *
   * Usage: val ColumnName(name) = encoded
   *
   * @param name parquet column name
   * @return
   */
  def unapply(name: String): Option[String] = {
    if (name.startsWith("__")) {
      // internal field (fid, bbox, etc)
      Some(name)
    } else {
      var i = 0
      var isUnderscore = false
      while (i < name.length) {
        name.charAt(i) match {
          case '_' if isUnderscore => return Some(decode(name, i, isUnderscore = true))
          case '_' => isUnderscore = true
          case c if AlphaNumeric.contains(c) => isUnderscore = false
          case _ => return Some(decode(name, i, isUnderscore = false))
        }
        i += 1
      }
      Some(name)
    }
  }

  private def encode(input: String, from: Int, isUnderscore: Boolean): String = {
    val startEncoding = if (isUnderscore) from - 1 else from
    val sb = new StringBuilder(input.substring(0, startEncoding))
    var i = startEncoding
    while (i < input.length) {
      val c = input.charAt(i)
      if (AlphaNumeric.contains(c)) {
        sb.append(c)
      } else if (c == '_') {
        val prevIsUnderscore = i > 0 && input.charAt(i - 1) == '_'
        val nextIsUnderscore = i < input.length - 1 && input.charAt(i + 1) == '_'
        if (prevIsUnderscore || nextIsUnderscore) {
          val hex = Hex.encodeHex(c.toString.getBytes(StandardCharsets.UTF_8))
          val encoded = hex.grouped(2).map(arr => "__" + arr(0) + arr(1)).mkString.toLowerCase(Locale.US)
          sb.append(encoded)
        } else {
          sb.append(c)
        }
      } else {
        val hex = Hex.encodeHex(c.toString.getBytes(StandardCharsets.UTF_8))
        val encoded = hex.grouped(2).map(arr => "__" + arr(0) + arr(1)).mkString.toLowerCase(Locale.US)
        sb.append(encoded)
      }
      i += 1
    }
    sb.toString()
  }

  private def decode(input: String, from: Int, isUnderscore: Boolean): String = {
    val startDecoding = if (isUnderscore) from - 1 else from
    val sb = new StringBuilder(input.substring(0, startDecoding))
    var i = startDecoding
    while (i < input.length) {
      if (i + 3 < input.length && input.charAt(i) == '_' && input.charAt(i + 1) == '_') {
        val hex1 = input.charAt(i + 2)
        val hex2 = input.charAt(i + 3)
        if (isHexDigit(hex1) && isHexDigit(hex2)) {
          val hexChars = scala.collection.mutable.ArrayBuffer[Char]()
          var j = i
          var continue = true
          while (continue && j + 3 < input.length && input.charAt(j) == '_' && input.charAt(j + 1) == '_') {
            val h1 = input.charAt(j + 2)
            val h2 = input.charAt(j + 3)
            if (isHexDigit(h1) && isHexDigit(h2)) {
              hexChars.append(h1)
              hexChars.append(h2)
              j += 4
            } else {
              continue = false
            }
          }
          val decoded = new String(Hex.decodeHex(hexChars.toArray), StandardCharsets.UTF_8)
          sb.append(decoded)
          i = j
        } else {
          sb.append(input.charAt(i))
          i += 1
        }
      } else {
        sb.append(input.charAt(i))
        i += 1
      }
    }
    sb.toString()
  }

  private def isHexDigit(c: Char): Boolean = {
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
  }
}
