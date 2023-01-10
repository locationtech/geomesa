/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Date, Locale, UUID}

/**
 * Serialization of lists and map types as opaque byte arrays, previously contained in AvroSimpleFeatureUtils
 */
object CollectionSerialization {

  import scala.collection.JavaConverters._

  /**
   * Encodes a list of primitives or Dates into a byte buffer. The list items must be all of the same
   * class.
   *
   * @param list list
   * @return
   */
  def encodeList(list: java.util.List[_], binding: String): ByteBuffer = {
    val size = Option(list).map(_.size)
    size match {
      case Some(s) if s == 0 => encodeEmptyCollection
      case Some(s)           => encodeNonEmptyList(list, s, binding)
      case None              => encodeNullCollection
    }
  }

  /**
   * Decodes a byte buffer created with @see encodeList back into a list
   *
   * @param bb buffer
   * @return
   */
  def decodeList(bb: ByteBuffer): java.util.List[AnyRef] = {
    val size = bb.getInt
    if (size < 0) {
      null
    } else if (size == 0) {
      java.util.Collections.emptyList()
    } else {
      val list = new java.util.ArrayList[Object](size)
      val label = getString(bb)
      val readMethod = getReadMethod(label, bb)
      var i = 0
      while (i < size) {
        list.add(readMethod())
        i += 1
      }
      list
    }
  }

  /**
   * Encodes a map of primitives or Dates into a byte buffer. The map keys must be all of the same
   * class, and the map values must all be of the same class.
   *
   * @param map map
   * @return
   */
  def encodeMap(map: java.util.Map[_, _], keyBinding: String, valueBinding: String): ByteBuffer = {
    val size = Option(map).map(_.size)
    size match {
      case Some(s) if s == 0 => encodeEmptyCollection
      case Some(s)           => encodeNonEmptyMap(map, s, keyBinding, valueBinding)
      case None              => encodeNullCollection
    }
  }

  /**
   * Decodes a byte buffer created with @see encodeMap back into a map.
   *
   * @param bb buffer
   * @return
   */
  def decodeMap(bb: ByteBuffer): java.util.Map[AnyRef, AnyRef] = {
    val size = bb.getInt
    if (size < 0) {
      null
    } else if (size == 0) {
      java.util.Collections.emptyMap()
    } else {
      val map = new java.util.HashMap[Object, Object](size)
      val keyType = getString(bb)
      val valueType = getString(bb)
      val keyReadMethod = getReadMethod(keyType, bb)
      val valueReadMethod = getReadMethod(valueType, bb)
      var i = 0
      while (i < size) {
        val key = keyReadMethod()
        val value = valueReadMethod()
        map.put(key, value)
        i += 1
      }
      map
    }
  }

  private def encodeNullCollection: ByteBuffer =
    ByteBuffer.allocate(4).putInt(-1).flip.asInstanceOf[ByteBuffer]

  private def encodeEmptyCollection: ByteBuffer =
    ByteBuffer.allocate(4).putInt(0).flip.asInstanceOf[ByteBuffer]

  /**
   * Encodes a list that has entries.
   *
   * @param list list
   * @param size size of list
   * @return
   */
  private def encodeNonEmptyList(list: java.util.List[_], size: Int, label: String): ByteBuffer = {
    // get the appropriate write method for the list type
    val (bytesPerItem, putMethod): (Int, (ByteBuffer, Any) => Unit) = getWriteMethod(label)
    // calculate the total size needed to encode the list
    val totalBytes = getTotalBytes(bytesPerItem, size, list.iterator().asScala, label)

    val labelBytes = label.getBytes(StandardCharsets.UTF_8)
    // 4 bytes for list size + 4 bytes for label bytes size + label bytes + item bytes
    val bb = ByteBuffer.allocate(4 + 4 + labelBytes.size + totalBytes)
    // first put the size of the list
    bb.putInt(size)
    // put the type of the list
    putString(bb, label)
    // put each item
    list.asScala.foreach(v => putMethod(bb, v))
    // flip (reset) the buffer so that it's ready for reading
    bb.flip
    bb
  }

  /**
   * Encodes a map that has entries.
   *
   * @param map map
   * @param size size of map
   * @return
   */
  private def encodeNonEmptyMap(
      map: java.util.Map[_, _],
      size: Int,
      keyLabel: String,
      valueLabel: String): ByteBuffer = {
    // get the appropriate write methods and approximate sizes for keys and values
    val (bytesPerKeyItem, keyPutMethod)     = getWriteMethod(keyLabel)
    val (bytesPerValueItem, valuePutMethod) = getWriteMethod(valueLabel)

    // get the exact size in bytes for keys and values
    val totalKeyBytes   = getTotalBytes(bytesPerKeyItem, size, map.asScala.keysIterator, keyLabel)
    val totalValueBytes = getTotalBytes(bytesPerValueItem, size, map.asScala.valuesIterator, valueLabel)

    val keyLabelBytes = keyLabel.getBytes(StandardCharsets.UTF_8)
    val valueLabelBytes = valueLabel.getBytes(StandardCharsets.UTF_8)
    // 4 bytes for map size + 8 bytes for label bytes size + label bytes + key bytes + value bytes
    val totalBytes = 4 + 8 + keyLabelBytes.size + valueLabelBytes.size + totalKeyBytes + totalValueBytes
    val bb = ByteBuffer.allocate(totalBytes)
    // first put the size of the map
    bb.putInt(size)
    // put the types of the keys and values
    putString(bb, keyLabel)
    putString(bb, valueLabel)
    // put each key value pair
    map.asScala.foreach { case (k, v) =>
      keyPutMethod(bb, k)
      valuePutMethod(bb, v)
    }
    // flip (reset) the buffer so that it's ready for reading
    bb.flip
    bb
  }

  /**
   * Gets the appropriate byte buffer method for the given object type.
   *
   * @param label class type
   * @return size per item (if known, otherwise -1) + read method
   */
  private def getWriteMethod(label: String): (Int, (ByteBuffer, Any) => Unit) =
    label.toLowerCase(Locale.US) match {
      case "string"  => (-1, (bb, v) => putString(bb, v.asInstanceOf[String]))
      case "int" |
           "integer" => (4, (bb, v) => bb.putInt(v.asInstanceOf[Int]))
      case "double"  => (8, (bb, v) => bb.putDouble(v.asInstanceOf[Double]))
      case "long"    => (8, (bb, v) => bb.putLong(v.asInstanceOf[Long]))
      case "float"   => (4, (bb, v) => bb.putFloat(v.asInstanceOf[Float]))
      case "date"    => (8, (bb, v) => bb.putLong(v.asInstanceOf[Date].getTime))
      case "boolean" => (1, (bb, v) => if (v.asInstanceOf[Boolean]) bb.put(1.toByte) else bb.put(0.toByte))
      case "uuid"    => (16, (bb, v) => putUUID(bb, v.asInstanceOf[UUID]))
      case "byte[]"  => (-1, (bb, v) => putBytes(bb, v.asInstanceOf[Array[Byte]]))
      case _         =>
        val msg = s"Invalid collection type: '$label'. Only primitives and Dates are supported."
        throw new IllegalArgumentException(msg)
    }

  /**
   * Gets the appropriate byte buffer method for the given object type.
   *
   * @param label class type
   * @param bb buffer
   * @return
   */
  private def getReadMethod(label: String, bb: ByteBuffer): () => Object =
    label.toLowerCase(Locale.US) match {
      case "string"  => () => getString(bb)
      case "int" |
           "integer" => () => bb.getInt.asInstanceOf[Object]
      case "double"  => () => bb.getDouble.asInstanceOf[Object]
      case "long"    => () => bb.getLong.asInstanceOf[Object]
      case "float"   => () => bb.getFloat.asInstanceOf[Object]
      case "boolean" => () => java.lang.Boolean.valueOf(bb.get > 0)
      case "date"    => () => new Date(bb.getLong())
      case "uuid"    => () => getUUID(bb)
      case "byte[]"  => () => getBytes(bb)
      case _         =>
        val msg = s"Invalid collection type: '$label'. Only primitives and Dates are supported."
        throw new IllegalArgumentException(msg)
    }

  /**
   * Gets the total bytes needed to encode the given values. For most types, the size is fixed, but
   * Strings and bytes are encoded with a dynamic length.
   *
   * @param bytesPerItem bytes per item
   * @param size number of items
   * @param values values
   * @return
   */
  private def getTotalBytes(bytesPerItem: Int, size: Int, values: Iterator[_], label: String): Int =
    if (bytesPerItem == -1) {
      // bytes are variable, we need to calculate them based on content
      // this only happens with strings
      // add 4 to each to use for length encoding
      label.toLowerCase match {
        case "string" => values.map(_.asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length + 4).sum
        case "byte[]" => values.map(_.asInstanceOf[Array[Byte]].length + 4).sum
        case _ => throw new IllegalArgumentException("invalid type")
      }
    } else {
      bytesPerItem * size
    }

  /**
   * Reads a string from a byte buffer that has been written using @see putString.
   *
   * @param bb buffer
   * @return
   */
  private def getString(bb: ByteBuffer): String = {
    val size = bb.getInt
    val buf = new Array[Byte](size)
    bb.get(buf)
    new String(buf, StandardCharsets.UTF_8)
  }

  /**
   * Writes a string to a byte buffer by encoding the length first, then the bytes of the string.
   *
   * @param bb buffer
   * @param s string
   * @return
   */
  private def putString(bb: ByteBuffer, s: String): ByteBuffer = putBytes(bb, s.getBytes(StandardCharsets.UTF_8))

  /**
    * Writes a byte array to a byte buffer by encoding the length first, then the bytes
    *
    * @param bb buffer
    * @param arr array
    * @return
    */
  private def putBytes(bb: ByteBuffer, arr: Array[Byte]): ByteBuffer = bb.putInt(arr.length).put(arr)

  /**
    * Reads a byte array from a byte buffer that has been written using @see putBytes
    *
    * @param bb buffer
    * @return
    */
  private def getBytes(bb: ByteBuffer): Array[Byte] = {
    val sz = bb.getInt
    val bytes = new Array[Byte](sz)
    bb.get(bytes, 0, sz)
    bytes
  }

  private def putUUID(bb: ByteBuffer, uuid: UUID): ByteBuffer =
    bb.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)

  private def getUUID(bb: ByteBuffer): UUID = new UUID(bb.getLong, bb.getLong)
}
