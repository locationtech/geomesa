/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import java.sql.Timestamp
import java.util.Locale

import org.calrissian.mango.types.encoders.lexi.LongEncoder
import org.calrissian.mango.types.{LexiTypeEncoders, TypeEncoder, TypeRegistry}
import org.locationtech.geomesa.utils.geotools.AttributeOrdering
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.`type`.AttributeDescriptor

import scala.util.Try

case class AttributeIndexKey(i: Short, value: String, inclusive: Boolean = true) extends Ordered[AttributeIndexKey] {
  override def compare(that: AttributeIndexKey): Int = {
    val indexOrder = Ordering.Short.compare(i, that.i)
    if (indexOrder != 0) { indexOrder } else {
      // if i is the same, then value must be of the same type
      val valueOrder = AttributeOrdering.StringOrdering.compare(value, that.value)
      if (valueOrder != 0) { valueOrder } else {
        Ordering.Boolean.compare(inclusive, that.inclusive)
      }
    }
  }
}

object AttributeIndexKey {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val TypeRegistry = new TypeRegistry[String](LexiTypeEncoders.LEXI_TYPES, TimestampEncoder)

  val lexicoders: Seq[Class[_]] = TypeRegistry.getAllEncoders.asScala.map(_.resolves()).toList

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
  def typeEncode(value: Any): String = TypeRegistry.encode(value)

  /**
   * Gets the type alias used for decoding a value
   *
   * @param binding type binding
   * @return
   */
  def alias(binding: Class[_]): String = binding.getSimpleName.toLowerCase(Locale.US)

  /**
    * Decode a lexicoded value
    *
    * @param alias type alias used for decoding
    * @param value encoded value
    * @return
    */
  def decode(alias: String, value: String): AnyRef = TypeRegistry.decode(alias, value)

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

  /**
    * Encoder for java.sql.Timestamp
    */
  object TimestampEncoder extends TypeEncoder[Timestamp, String] {

    private val longEncoder = new LongEncoder()

    override val getAlias: String = "timestamp"

    override def resolves(): Class[Timestamp] = classOf[Timestamp]

    override def encode(value: Timestamp): String = {
      if (value == null) {
        throw new NullPointerException("Null values are not allowed")
      }
      longEncoder.encode(value.getTime)
    }

    override def decode(value: String): Timestamp = new Timestamp(longEncoder.decode(value))
  }
}
