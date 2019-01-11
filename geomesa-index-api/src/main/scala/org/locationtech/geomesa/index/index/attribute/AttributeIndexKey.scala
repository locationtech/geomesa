/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import org.calrissian.mango.types.LexiTypeEncoders
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.ByteArrays

import scala.util.Try

case class AttributeIndexKey(i: Short, value: String, inclusive: Boolean = true) extends Ordered[AttributeIndexKey] {
  override def compare(that: AttributeIndexKey): Int = {
    val indexOrder = Ordering.Short.compare(i, that.i)
    if (indexOrder != 0) { indexOrder } else {
      // if i is the same, then value must be of the same type
      val valueOrder = SimpleFeatureOrdering.nullCompare(value.asInstanceOf[Comparable[Any]], that.value)
      if (valueOrder != 0) { valueOrder } else {
        Ordering.Boolean.compare(inclusive, that.inclusive)
      }
    }
  }
}

object AttributeIndexKey {

  private val typeRegistry = LexiTypeEncoders.LEXI_TYPES

  // store 2 bytes for the index of the attribute in the sft - this allows up to 32k attributes in the sft.
  def indexToBytes(i: Int): Array[Byte] = ByteArrays.toBytes(i.toShort)

  /**
    * Lexicographically encode the value. Collections will return multiple rows, one for each entry.
    */
  def encodeForIndex(value: Any, list: Boolean): Seq[String] = {
    import scala.collection.JavaConverters._

    if (value == null) {
      Seq.empty
    } else if (list) {
      // encode each value into a separate row
      value.asInstanceOf[java.util.List[_]].asScala.collect { case v if v != null => typeEncode(v) }
    } else {
      Seq(typeEncode(value))
    }
  }

  /**
    * Lexicographically encode the value. Will convert types appropriately.
    */
  def encodeForQuery(value: Any, binding: Class[_]): String = {
    if (value == null) { null } else {
      typeEncode(Option(FastConverter.convert(value, binding)).getOrElse(value))
    }
  }

  // Lexicographically encode a value using it's runtime class
  def typeEncode(value: Any): String = Try(typeRegistry.encode(value)).getOrElse(value.toString)
}
