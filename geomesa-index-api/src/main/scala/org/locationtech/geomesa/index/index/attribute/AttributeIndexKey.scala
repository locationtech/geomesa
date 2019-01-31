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
import org.opengis.feature.`type`.AttributeDescriptor

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

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val typeRegistry = LexiTypeEncoders.LEXI_TYPES

  val lexicoders: Seq[Class[_]] = AttributeIndexKey.typeRegistry.getAllEncoders.asScala.map(_.resolves()).toList

  // store 2 bytes for the index of the attribute in the sft - this allows up to 32k attributes in the sft.
  def indexToBytes(i: Int): Array[Byte] = ByteArrays.toBytes(i.toShort)

  /**
    * Lexicographically encode the value, converting types appropriately
    *
    * @param value query value
    * @param binding binding of the attribute being queried
    * @return
    */
  def encodeForQuery(value: Any, binding: Class[_]): String = {
    if (value == null) { null } else {
      Try(typeEncode(Option(FastConverter.convert(value, binding)).getOrElse(value))).getOrElse(value.toString)
    }
  }

  /**
    * Lexicographically encode a value using it's runtime class
    *
    * @param value value
    * @return
    */
  def typeEncode(value: Any): String = typeRegistry.encode(value)

  /**
    * Decode a lexicoded value
    *
    * @param alias type alias used for decoding
    * @param value encoded value
    * @return
    */
  def decode(alias: String, value: String): AnyRef = typeRegistry.decode(alias, value)

  /**
    * Is the type supported for lexicoding
    *
    * @param descriptor attribute descriptor
    * @return
    */
  def encodable(descriptor: AttributeDescriptor): Boolean =
    encodable(if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding })

  /**
    * Is the type supported for lexicoding
    *
    * @param binding class binding
    * @return
    */
  def encodable(binding: Class[_]): Boolean = lexicoders.exists(_.isAssignableFrom(binding))
}
