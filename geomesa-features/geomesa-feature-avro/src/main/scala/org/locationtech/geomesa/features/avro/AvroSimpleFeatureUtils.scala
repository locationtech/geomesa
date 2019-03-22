/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Date, Locale, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBWriter
import org.apache.avro.{Schema, SchemaBuilder}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object AvroSimpleFeatureUtils extends LazyLogging {

  val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  val AVRO_SIMPLE_FEATURE_USERDATA: String = "__userdata__"

  // Increment whenever encoding changes and handle in reader and writer
  // Version 2 changed the WKT geom to a binary geom
  // Version 3 adds byte array types to the schema...and is backwards compatible with V2
  // Version 4 adds a custom name encoder function for the avro schema
  //           v4 can read version 2 and 3 files but version 3 cannot read version 4
  val VERSION: Int = 4
  val AVRO_NAMESPACE: String = "org.geomesa"

  def generateSchema(sft: SimpleFeatureType,
                     withUserData: Boolean,
                     withFeatureId: Boolean,
                     namespace: String = AVRO_NAMESPACE): Schema = {
    val nameEncoder = new FieldNameEncoder(VERSION)
    val initialAssembler: SchemaBuilder.FieldAssembler[Schema] =
      SchemaBuilder.record(nameEncoder.encode(sft.getTypeName))
        .namespace(namespace)
        .fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault

    val withFid = if (withFeatureId) {
      initialAssembler.name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault
    } else {
      initialAssembler
    }

    val withFields =
      sft.getAttributeDescriptors.foldLeft(withFid) { case (assembler, ad) =>
        addField(assembler, nameEncoder.encode(ad.getLocalName), ad.getType.getBinding, ad.isNillable)
      }

    val fullSchema = if (withUserData) {
      withFields.name(AVRO_SIMPLE_FEATURE_USERDATA).`type`.array().items().record("userDataItem").fields()
        .name("keyClass").`type`.stringType().noDefault()
        .name("key").`type`.stringType().noDefault()
        .name("valueClass").`type`.stringType().noDefault()
        .name("value").`type`.stringType().noDefault().endRecord().noDefault()
    } else {
      withFields
    }

    fullSchema.endRecord
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
      case c if classOf[Array[Byte]].isAssignableFrom(c)         => baseType.bytesType.noDefault
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
  def createTypeMap(sft: SimpleFeatureType, wkbWriter: WKBWriter, nameEncoder: FieldNameEncoder): Map[String, Binding] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    sft.getAttributeDescriptors.map { ad =>
      val binding = ad.getType.getBinding
      val converter = if (primitiveTypes.contains(binding)) {
        (value: AnyRef) => value
      } else if (classOf[UUID].isAssignableFrom(binding)) {
        (value: AnyRef) => encodeUUID(value.asInstanceOf[UUID])
      } else if (classOf[Date].isAssignableFrom(binding)) {
        (value: AnyRef) => value.asInstanceOf[Date].getTime
      } else if (classOf[Geometry].isAssignableFrom(binding) ) {
        (value: AnyRef) => ByteBuffer.wrap(wkbWriter.write(value.asInstanceOf[Geometry]))
      } else if (ad.isList) {
        (value: AnyRef) => encodeList(value.asInstanceOf[java.util.List[_]], ad.getListType())
      } else if (ad.isMap) {
        (value: AnyRef) => {
          val (keyclass, valueclass) = ad.getMapTypes()
          encodeMap(value.asInstanceOf[java.util.Map[_, _]], keyclass, valueclass)
        }
      } else if (classOf[Array[Byte]].isAssignableFrom(binding)) {
        (value: AnyRef) => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      } else {
        (value: AnyRef) => FastConverter.convert(value, classOf[String])
      }

      (nameEncoder.encode(ad.getLocalName), Binding(ad.getType.getBinding, converter))
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

  def schemaToSft(schema: Schema,
                  sftName: String,
                  geomAttr: Option[String],
                  dateAttr: Option[String]): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(sftName)
    geomAttr.foreach { ga =>
      builder.setDefaultGeometry(ga)
      builder.add(ga, classOf[Geometry])
    }
    dateAttr.foreach(builder.add(_, classOf[Date]))
    schema.getFields.foreach(addSchemaToBuilder(builder, _))
    builder.buildFeatureType()
  }

  def addSchemaToBuilder(builder: SimpleFeatureTypeBuilder,
                         field: Schema.Field,
                         typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema().getType) match {
      case Schema.Type.STRING  => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.BOOLEAN => builder.add(field.name(), classOf[java.lang.Boolean])
      case Schema.Type.INT     => builder.add(field.name(), classOf[java.lang.Integer])
      case Schema.Type.DOUBLE  => builder.add(field.name(), classOf[java.lang.Double])
      case Schema.Type.LONG    => builder.add(field.name(), classOf[java.lang.Long])
      case Schema.Type.FLOAT   => builder.add(field.name(), classOf[java.lang.Float])
      case Schema.Type.BYTES   => logger.error("Avro schema requested BYTES, which is not yet supported") // TODO support
      case Schema.Type.UNION   => field.schema().getTypes.map(_.getType).find(_ != Schema.Type.NULL)
                                       .foreach(t => addSchemaToBuilder(builder, field, Option(t))) // TODO support more union types and log any errors better
      case Schema.Type.MAP     => logger.error("Avro schema requested MAP, which is not yet supported") // TODO support
      case Schema.Type.RECORD  => logger.error("Avro schema requested RECORD, which is not yet supported") // TODO support
      case Schema.Type.ENUM    => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.ARRAY   => logger.error("Avro schema requested ARRAY, which is not yet supported") // TODO support
      case Schema.Type.FIXED   => logger.error("Avro schema requested FIXED, which is not yet supported") // TODO support
      case Schema.Type.NULL    => logger.error("Avro schema requested NULL, which is not yet supported") // TODO support
      case _                   => logger.error(s"Avro schema requested unknown type ${field.schema().getType}")
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
    val totalBytes = getTotalBytes(bytesPerItem, size, list.iterator(), binding.getSimpleName)

    val labelBytes = label.getBytes(StandardCharsets.UTF_8)
    // 4 bytes for list size + 4 bytes for label bytes size + label bytes + item bytes
    val bb = ByteBuffer.allocate(4 + 4 + labelBytes.size + totalBytes)
    // first put the size of the list
    bb.putInt(size)
    // put the type of the list
    AvroSimpleFeatureUtils.putString(bb, label)
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
    val totalKeyBytes   = getTotalBytes(bytesPerKeyItem, size, map.keysIterator, keyLabel)
    val totalValueBytes = getTotalBytes(bytesPerValueItem, size, map.valuesIterator, valueLabel)

    val keyLabelBytes = keyLabel.getBytes(StandardCharsets.UTF_8)
    val valueLabelBytes = valueLabel.getBytes(StandardCharsets.UTF_8)
    // 4 bytes for map size + 8 bytes for label bytes size + label bytes + key bytes + value bytes
    val totalBytes = 4 + 8 + keyLabelBytes.size + valueLabelBytes.size + totalKeyBytes + totalValueBytes
    val bb = ByteBuffer.allocate(totalBytes)
    // first put the size of the map
    bb.putInt(size)
    // put the types of the keys and values
    AvroSimpleFeatureUtils.putString(bb, keyLabel)
    AvroSimpleFeatureUtils.putString(bb, valueLabel)
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
   * @param bytesPerItem
   * @param size
   * @param values
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
   * @param bb
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
   * @param bb
   * @param s
   * @return
   */
  private def putString(bb: ByteBuffer, s: String): ByteBuffer = putBytes(bb, s.getBytes(StandardCharsets.UTF_8))

  /**
    * Writes a byte array to a byte buffer by encoding the length first, then the bytes
    *
    * @param bb
    * @param arr
    * @return
    */
  private def putBytes(bb: ByteBuffer, arr: Array[Byte]): ByteBuffer = bb.putInt(arr.length).put(arr)

  /**
    * Reads a byte array from a byte buffer that has been written using @see putBytes
    *
    * @param bb
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
