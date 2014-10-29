/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature

import java.nio.ByteBuffer
import java.util.{Date, Locale, UUID}

import com.google.common.collect.Maps
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBWriter
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.codec.binary.Hex
import org.geotools.util.Converters
import org.locationtech.geomesa.utils.geotools.Conversions.RichAttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object AvroSimpleFeatureUtils {

  val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"

  // Increment whenever encoding changes and handle in reader and writer
  val VERSION: Int = 2
  val AVRO_NAMESPACE: String = "org.geomesa"

  val attributeNameLookUp = Maps.newConcurrentMap[String, String]()

  def encode(s: String): String = "_" + Hex.encodeHexString(s.getBytes("UTF8"))

  def decode(s: String): String = new String(Hex.decodeHex(s.substring(1).toCharArray), "UTF8")

  def encodeAttributeName(s: String): String = attributeNameLookUp.getOrElseUpdate(s, encode(s))

  def decodeAttributeName(s: String): String = attributeNameLookUp.getOrElseUpdate(s, decode(s))

  def generateSchema(sft: SimpleFeatureType): Schema = {
    val initialAssembler: SchemaBuilder.FieldAssembler[Schema] =
      SchemaBuilder.record(encodeAttributeName(sft.getTypeName))
        .namespace(AVRO_NAMESPACE)
        .fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    val result =
      sft.getAttributeDescriptors.foldLeft(initialAssembler) { case (assembler, ad) =>
        addField(assembler, encodeAttributeName(ad.getLocalName), ad.getType.getBinding, ad.isNillable)
      }

    result.endRecord
  }

  def addField(assembler: SchemaBuilder.FieldAssembler[Schema],
               name: String,
               ct: Class[_],
               nillable: Boolean): SchemaBuilder.FieldAssembler[Schema] = {
    val baseType = if (nillable) assembler.name(name).`type`.nullable() else assembler.name(name).`type`
    ct match {
      case c if classOf[String].isAssignableFrom(c)              => baseType.stringType.noDefault
      case c if classOf[java.lang.Integer].isAssignableFrom(c)   => baseType.intType.noDefault
      case c if classOf[java.lang.Long].isAssignableFrom(c)      => baseType.longType.noDefault
      case c if classOf[java.lang.Double].isAssignableFrom(c)    => baseType.doubleType.noDefault
      case c if classOf[java.lang.Float].isAssignableFrom(c)     => baseType.floatType.noDefault
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)   => baseType.booleanType.noDefault
      case c if classOf[UUID].isAssignableFrom(c)                => baseType.bytesType.noDefault
      case c if classOf[Date].isAssignableFrom(c)                => baseType.longType.noDefault
      case c if classOf[Geometry].isAssignableFrom(c)            => baseType.bytesType.noDefault
      case c if classOf[java.util.List[_]].isAssignableFrom(c)   => baseType.bytesType.noDefault
      case c if classOf[java.util.Map[_, _]].isAssignableFrom(c) => baseType.bytesType.noDefault
    }
  }

  val primitiveTypes =
    List(
      classOf[String],
      classOf[java.lang.Integer],
      classOf[Int],
      classOf[java.lang.Long],
      classOf[Long],
      classOf[java.lang.Double],
      classOf[Double],
      classOf[java.lang.Float],
      classOf[Float],
      classOf[java.lang.Boolean],
      classOf[Boolean]
    )

  case class Binding(clazz: Class[_], conv: AnyRef => Any)

  // Resulting functions in map are not thread-safe...use only as
  // member variable, not in a static context
  def createTypeMap(sft: SimpleFeatureType, wkbWriter: WKBWriter): Map[String, Binding] = {
    sft.getAttributeDescriptors.map { ad =>
      val conv = ad.getType.getBinding match {
        case t if primitiveTypes.contains(t) => (v: AnyRef) => v

        case t if classOf[UUID].isAssignableFrom(t) => (v: AnyRef) => encodeUUID(v.asInstanceOf[UUID])

        case t if classOf[Date].isAssignableFrom(t) => (v: AnyRef) => v.asInstanceOf[Date].getTime

        case t if classOf[Geometry].isAssignableFrom(t) =>
          (v: AnyRef) => ByteBuffer.wrap(wkbWriter.write(v.asInstanceOf[Geometry]))

        case t if ad.isCollection => (v: AnyRef) =>
          encodeList(v.asInstanceOf[java.util.List[_]],
            ad.getUserData.get("subtype").asInstanceOf[Class[_]])

        case t if ad.isMap => (v: AnyRef) =>
          val keyclass   = ad.getUserData.get("keyclass").asInstanceOf[Class[_]]
          val valueclass = ad.getUserData.get("valueclass").asInstanceOf[Class[_]]
          encodeMap(v.asInstanceOf[java.util.Map[_, _]], keyclass, valueclass)

        case _ =>
          (v: AnyRef) =>
            Option(Converters.convert(v, classOf[String])).getOrElse { a: AnyRef => a.toString }
      }

      (encodeAttributeName(ad.getLocalName), Binding(ad.getType.getBinding, conv))
    }.toMap
  }

  def encodeUUID(uuid: UUID) =
    ByteBuffer.allocate(16)
        .putLong(uuid.getMostSignificantBits)
        .putLong(uuid.getLeastSignificantBits)
        .flip.asInstanceOf[ByteBuffer]

  def decodeUUID(bb: ByteBuffer): UUID = new UUID(bb.getLong, bb.getLong)

  /**
   * Encodes a list of primitives or Dates into a byte buffer. The list items must be all of the same
   * class.
   *
   * @param list
   * @return
   */
  def encodeList(list: java.util.List[_], binding: Class[_]): ByteBuffer = {
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
   * @param bb
   * @return
   */
  def decodeList(bb: ByteBuffer): java.util.List[_] = {
    val size = bb.getInt
    if (size < 0) {
      null
    } else if (size == 0) {
      java.util.Collections.emptyList()
    } else {
      val list = new java.util.ArrayList[Object](size)
      val label = AvroSimpleFeatureUtils.getString(bb)
      val readMethod = getReadMethod(label, bb)
      (0 to size - 1).foreach(_ => list.add(readMethod()))
      list
    }
  }

  /**
   * Encodes a map of primitives or Dates into a byte buffer. The map keys must be all of the same
   * class, and the map values must all be of the same class.
   *
   * @param map
   * @return
   */
  def encodeMap(map: java.util.Map[_, _], keyBinding: Class[_], valueBinding: Class[_]): ByteBuffer = {
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
   * @param bb
   * @return
   */
  def decodeMap(bb: ByteBuffer): java.util.Map[_, _] = {
    val size = bb.getInt
    if (size < 0) {
      null
    } else if (size == 0) {
      java.util.Collections.emptyMap()
    } else {
      val map = new java.util.HashMap[Object, Object](size)
      val keyType = AvroSimpleFeatureUtils.getString(bb)
      val valueType = AvroSimpleFeatureUtils.getString(bb)
      val keyReadMethod = getReadMethod(keyType, bb)
      val valueReadMethod = getReadMethod(valueType, bb)
      (0 to size - 1).foreach { _ =>
        val key = keyReadMethod()
        val value = valueReadMethod()
        map.put(key, value)
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
   * @param list
   * @param size
   * @return
   */
  private def encodeNonEmptyList(list: java.util.List[_], size: Int, binding: Class[_]): ByteBuffer = {
    // get the class label for the list items
    val label = binding.getSimpleName
    // get the appropriate write method for the list type
    val (bytesPerItem, putMethod): (Int, (ByteBuffer, Any) => Unit) = getWriteMethod(label)
    // calculate the total size needed to encode the list
    val totalBytes = getTotalBytes(bytesPerItem, size, list.iterator())

    val labelBytes = label.getBytes("UTF-8")
    // 4 bytes for list size + 4 bytes for label bytes size + label bytes + item bytes
    val bb = ByteBuffer.allocate(4 + 4 + labelBytes.size + totalBytes)
    // first put the size of the list
    bb.putInt(size)
    // put the type of the list
    AvroSimpleFeatureUtils.putString(bb, labelBytes)
    // put each item
    list.foreach(v => putMethod(bb, v))
    // flip (reset) the buffer so that it's ready for reading
    bb.flip
    bb
  }

  /**
   * Encodes a map that has entries.
   *
   * @param map
   * @param size
   * @return
   */
  private def encodeNonEmptyMap(map: java.util.Map[_, _],
                                size: Int,
                                keyBinding: Class[_],
                                valueBinding: Class[_]): ByteBuffer = {
    // pull out the class labels for the map keys/values
    val keyLabel = keyBinding.getSimpleName
    val valueLabel = valueBinding.getSimpleName

    // get the appropriate write methods and approximate sizes for keys and values
    val (bytesPerKeyItem, keyPutMethod)     = getWriteMethod(keyLabel)
    val (bytesPerValueItem, valuePutMethod) = getWriteMethod(valueLabel)

    // get the exact size in bytes for keys and values
    val totalKeyBytes   = getTotalBytes(bytesPerKeyItem, size, map.keysIterator)
    val totalValueBytes = getTotalBytes(bytesPerValueItem, size, map.valuesIterator)

    val keyLabelBytes = keyLabel.getBytes("UTF-8")
    val valueLabelBytes = valueLabel.getBytes("UTF-8")
    // 4 bytes for map size + 8 bytes for label bytes size + label bytes + key bytes + value bytes
    val totalBytes = 4 + 8 + keyLabelBytes.size + valueLabelBytes.size + totalKeyBytes + totalValueBytes
    val bb = ByteBuffer.allocate(totalBytes)
    // first put the size of the map
    bb.putInt(size)
    // put the types of the keys and values
    AvroSimpleFeatureUtils.putString(bb, keyLabelBytes)
    AvroSimpleFeatureUtils.putString(bb, valueLabelBytes)
    // put each key value pair
    map.foreach { case (k, v) =>
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
   * @param label
   * @return size per item (if known, otherwise -1) + read method
   */
  private def getWriteMethod(label: String): (Int, (ByteBuffer, Any) => Unit) =
    label.toLowerCase(Locale.US) match {
      case "string"  => (-1, (bb, v) => putString(bb, v.asInstanceOf[String].getBytes("UTF-8")))
      case "int" |
           "integer" => (4, (bb, v) => bb.putInt(v.asInstanceOf[Int]))
      case "double"  => (8, (bb, v) => bb.putDouble(v.asInstanceOf[Double]))
      case "long"    => (8, (bb, v) => bb.putLong(v.asInstanceOf[Long]))
      case "float"   => (4, (bb, v) => bb.putFloat(v.asInstanceOf[Float]))
      case "date"    => (8, (bb, v) => bb.putLong(v.asInstanceOf[Date].getTime))
      case "boolean" => (1, (bb, v) => if (v.asInstanceOf[Boolean]) bb.put(1.toByte) else bb.put(0.toByte))
      case _         =>
        val msg = s"Invalid collection type: '$label'. Only primitives and Dates are supported."
        throw new IllegalArgumentException(msg)
    }

  /**
   * Gets the appropriate byte buffer method for the given object type.
   *
   * @param label
   * @param bb
   * @return
   */
  private def getReadMethod(label: String, bb: ByteBuffer): () => Object =
    label.toLowerCase(Locale.US) match {
      case "string"  => () => AvroSimpleFeatureUtils.getString(bb)
      case "int" |
           "integer" => () => bb.getInt.asInstanceOf[Object]
      case "double"  => () => bb.getDouble.asInstanceOf[Object]
      case "long"    => () => bb.getLong.asInstanceOf[Object]
      case "float"   => () => bb.getFloat.asInstanceOf[Object]
      case "boolean" => () => java.lang.Boolean.valueOf(bb.get > 0)
      case "date"    => () => new Date(bb.getLong())
      case _         =>
        val msg = s"Invalid collection type: '$label'. Only primitives and Dates are supported."
        throw new IllegalArgumentException(msg)
    }

  /**
   * Gets the total bytes needed to encode the given values. For most types, the size is fixed, but
   * Strings are encoded with a dynamic length.
   *
   * @param bytesPerItem
   * @param size
   * @param values
   * @return
   */
  private def getTotalBytes(bytesPerItem: Int, size: Int, values: Iterator[_]): Int =
    if (bytesPerItem == -1) {
      // bytes are variable, we need to calculate them based on content
      // this only happens with strings
      // add 4 to each to use for length encoding
      values.map(_.asInstanceOf[String].getBytes("UTF-8").size + 4).sum
    } else {
      bytesPerItem * size
    }

  /**
   * Reads a string from a byte buffer that has been written using @see putString.
   *
   * @param bb
   * @return
   */
  private def getString(bb: ByteBuffer): String = {
    val size = bb.getInt
    val buf = new Array[Byte](size)
    bb.get(buf)
    new String(buf, "UTF-8")
  }

  /**
   * Writes a string to a byte buffer by encoding the length first, then the bytes of the string.
   *
   * @param bb
   * @param s
   * @return
   */
  private def putString(bb: ByteBuffer, s: Array[Byte]): ByteBuffer = bb.putInt(s.size).put(s)

}
