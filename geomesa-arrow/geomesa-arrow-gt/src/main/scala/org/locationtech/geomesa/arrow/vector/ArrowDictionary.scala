/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.ImmutableBiMap
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}
import org.locationtech.geomesa.arrow.TypeBindings

/**
  * Holder for dictionary values
  *
  * @param values dictionary values. When encoded, values are replaced with their index in the seq
  * @param encoding dictionary id and int width, must be unique per arrow file
  */
class ArrowDictionary(val values: Seq[AnyRef], val encoding: DictionaryEncoding) {

  def id: Long = encoding.getId

  lazy private val (map, inverse) = {
    val builder = ImmutableBiMap.builder[AnyRef, Integer]
    var i = 0
    values.foreach { value =>
      builder.put(value, i)
      i += 1
    }
    builder.put("[other]", i) // for non-string types, this should evaluate to null
    val m = builder.build()
    (m, m.inverse())
  }

  /**
    * Dictionary encode a value to an int
    *
    * @param value value to encode
    * @return dictionary encoded int
    */
  def index(value: AnyRef): Int = {
    val result = map.get(value)
    if (result == null) {
      values.size // 'other'
    } else {
      result.intValue()
    }
  }

  /**
    * Decode a dictionary int to a value
    *
    * @param i dictionary encoded int
    * @return value
    */
  def lookup(i: Int): AnyRef = inverse.get(i)
}

object ArrowDictionary {

  trait HasArrowDictionary {
    def dictionary: ArrowDictionary
    def dictionaryType: TypeBindings
  }

  private val values = new SecureRandom().longs(0, Long.MaxValue).iterator()
  private val ids = new AtomicLong(values.next)

  def nextId: Long = ids.getAndSet(values.next)

  def create(values: Seq[AnyRef]): ArrowDictionary = new ArrowDictionary(values, createEncoding(nextId, values))

  // use the smallest int type possible to minimize bytes used
  private def createEncoding(id: Long, values: Seq[Any]): DictionaryEncoding = {
    if (values.length < Byte.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(8, true))
    } else if (values.length < Short.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(16, true))
    } else {
      new DictionaryEncoding(id, false, new ArrowType.Int(32, true))
    }
  }
}
