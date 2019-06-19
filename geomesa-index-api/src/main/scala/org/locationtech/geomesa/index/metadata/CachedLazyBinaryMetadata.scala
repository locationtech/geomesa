/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.metadata.CachedLazyBinaryMetadata.decodeRow
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.util.control.NonFatal

/**
  * Backs metadata with a cache to save repeated database reads. Underlying table will be lazily created
  * when required. Keys and values are both a single byte array
  *
  * @tparam T type param
  */
trait CachedLazyBinaryMetadata[T] extends CachedLazyMetadata[T] {

  // separator used between type names and keys
  val typeNameSeparator: Char = '~'

  def encodeRow(typeName: String, key: String): Array[Byte] =
    CachedLazyBinaryMetadata.encodeRow(typeName, key, typeNameSeparator)

  override protected def write(typeName: String, rows: Seq[(String, Array[Byte])]): Unit =
    write(rows.map { case (k, v) => (encodeRow(typeName, k), v) })

  override protected def delete(typeName: String, keys: Seq[String]): Unit =
    delete(keys.map(k => encodeRow(typeName, k)))

  override protected def scanValue(typeName: String, key: String): Option[Array[Byte]] =
    scanValue(encodeRow(typeName, key))

  override protected def scanValues(typeName: String, prefix: String): CloseableIterator[(String, Array[Byte])] = {
    scanRows(Some(encodeRow(typeName, prefix))).flatMap { case (row, value) =>
      try {
        val key = decodeRow(row, typeNameSeparator)._2
        CloseableIterator.single((key, value))
      } catch {
        case NonFatal(_) =>
          logger.warn(s"Ignoring unexpected row in catalog table: ${new String(row, StandardCharsets.UTF_8)}")
          CloseableIterator.empty
      }
    }
  }

  override protected def scanKeys(): CloseableIterator[(String, String)] = {
    scanRows(None).flatMap { case (row, _) =>
      try {
        val key = decodeRow(row, typeNameSeparator)
        CloseableIterator.single(key)
      } catch {
        case NonFatal(_) =>
          logger.warn(s"Ignoring unexpected row in catalog table: ${new String(row, StandardCharsets.UTF_8)}")
          CloseableIterator.empty
      }
    }
  }

  /**
    * Writes row/value pairs
    *
    * @param rows row/values
    */
  protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit

  /**
    * Deletes multiple rows
    *
    * @param rows rows
    */
  protected def delete(rows: Seq[Array[Byte]])

  /**
    * Reads a value from the underlying table
    *
    * @param row row
    * @return value, if it exists
    */
  protected def scanValue(row: Array[Byte]): Option[Array[Byte]]

  /**
    * Reads row keys from the underlying table
    *
    * @param prefix row key prefix
    * @return matching row keys and values
    */
  protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])]
}

object CachedLazyBinaryMetadata {

  def encodeRow(typeName: String, key: String, separator: Char): Array[Byte] = {
    // escaped to %U+XXXX unicode since decodeRow splits by separator
    val escape = s"%${"U+%04X".format(separator.toInt)}"
    s"${typeName.replace(separator.toString, escape)}$separator$key".getBytes(StandardCharsets.UTF_8)
  }

  def decodeRow(row: Array[Byte], separator: Char): (String, String) = {
    // escaped to %U+XXXX unicode since decodeRow splits by separator
    val escape = s"%${"U+%04X".format(separator.toInt)}"
    val all = new String(row, StandardCharsets.UTF_8)
    val split = all.indexOf(separator)
    (all.substring(0, split).replace(escape, separator.toString), all.substring(split + 1, all.length))
  }
}
