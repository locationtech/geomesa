/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.core
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Encoder/decoder for index values. Allows customizable fields to be encoded. Thread-safe.
 *
 * @param sft
 * @param fields
 */
class IndexValueEncoder(val sft: SimpleFeatureType, val fields: Seq[String]) {

  import org.locationtech.geomesa.core.index.IndexValueEncoder._

  fields.foreach(f => require(sft.getDescriptor(f) != null ||
    f == ID_FIELD, s"Encoded field does not exist: $f"))

  // check to see if we need to look for data encoded using the old scheme
  private val needsBackCompatible = fields == getDefaultSchema(sft)

  // map the actual attribute type bindings into well-known equivalents
  private val types = fields.toIndexedSeq // convert to indexed seq so we can look up by index later
      .map(f => if (f == ID_FIELD) classOf[String] else normalizeType(sft.getDescriptor(f).getType.getBinding))

  // compute the functions we need to encode/decode
  // we need to break it out into separate functions in order to determine the size of the byte array,
  // and to avoid allocation of intermediate objects

  // converts the value into something we can encode directly
  private val conversions = types.map(getValueFunctions(_).asInstanceOf[GetValueFunction[Any]])
  // determines the size each value requires to encode
  private val sizings = types.map(getSizeFunctions(_).asInstanceOf[GetSizeFunction[Any]])
  // does the actual encoding into the byte buffer
  private val encodings = types.map(encodeFunctions(_).asInstanceOf[EncodeFunction[Any]])
  // decodes back into the original value
  private val decodings = {
    val decodes = if (needsBackCompatible) backCompatibleDecodeFunctions else decodeFunctions
    types.map(decodes(_).asInstanceOf[DecodeFunction[Any]])
  }

  private val fieldsWithIndex = fields.zipWithIndex

  /**
   * Encodes a simple feature into a byte array. Only the attributes marked for inclusion get encoded.
   *
   * @param sf
   * @return
   */
  def encode(sf: SimpleFeature): Array[Byte] = {
    val valuesWithIndex = fieldsWithIndex.map { case (f, i) =>
      if (f == ID_FIELD) (sf.getID, i) else (sf.getAttribute(f), i)
    }
    var totalSize = 0
    // create partially applied functions we can run over the byte buffer once we know total size
    // calculate total size at the same time
    val partialEncodings = valuesWithIndex.map { case (value, i) =>
      val converted = conversions(i)(value)
      totalSize += sizings(i)(converted)
      encodings(i)(converted, _: ByteBuffer)
    }
    val buf = ByteBuffer.allocate(totalSize)
    partialEncodings.foreach(e => e(buf))
    buf.array()
  }

  val geomField = sft.getGeometryDescriptor.getLocalName
  val dtgField = core.index.getDtgFieldName(sft)

  /**
   * Decodes a byte array into a map of attribute name -> attribute value pairs
   *
   * @param value
   * @return
   */
  def decode(value: Array[Byte]): DecodedIndexValue = {
    val buf = ByteBuffer.wrap(value)
    val values = fieldsWithIndex.map { case (f, i) => f -> decodings(i)(buf) }.toMap
    DecodedIndexValue(id(values), geometry(values), date(values), values)
  }

  // helper methods to extract known fields from the decoded map
  private def id(values: Map[String, Any]) = values(ID_FIELD).asInstanceOf[String]
  private def geometry(values: Map[String, Any]) = values(geomField).asInstanceOf[Geometry]
  private def date(values: Map[String, Any]) = dtgField.flatMap(f => Option(values(f).asInstanceOf[Date]))
}

/**
 * Decoded index value
 *
 * @param id
 * @param geom
 * @param date
 * @param attributes
 */
case class DecodedIndexValue(id: String, geom: Geometry, date: Option[Date], attributes: Map[String, Any])

object IndexValueEncoder {

  val ID_FIELD = "id"

  // even though the encoders are thread safe, this way we don't have to synchronize when retrieving them
  private val cache = new ThreadLocal[scala.collection.mutable.Map[String, IndexValueEncoder]] {
    override def initialValue() = scala.collection.mutable.Map.empty
  }

  // gets a cached instance to avoid the initialization overhead
  // we use sft.toString, which includes the fields and type name, as a unique key
  def apply(sft: SimpleFeatureType) = cache.get.getOrElseUpdate(sft.toString, createNew(sft))

  // gets the default schema, which includes ID, geom and date (if available)
  // order is important here, as it needs to match the old IndexEntry encoding
  def getDefaultSchema(sft: SimpleFeatureType): Seq[String] =
    Seq(ID_FIELD, sft.getGeometryDescriptor.getLocalName) ++ core.index.getDtgFieldName(sft)

  /**
   * Creates a new IndexValueEncoder based on the user data in the simple feature. The
   * IndexValueEncoder does some initialization up-front, so the cached instances should be used.
   *
   * @param sft
   * @return
   */
  private def createNew(sft: SimpleFeatureType) = {
    import scala.collection.JavaConversions._
    val schema = {
      val defaults = getDefaultSchema(sft)
      val descriptors =  sft.getAttributeDescriptors
          .filter(d => Option(d.getUserData.get("stidx").asInstanceOf[Boolean]).getOrElse(false))
          .map(_.getLocalName)
      if (descriptors.isEmpty) {
        defaults
      } else {
        // keep defaults first to maintain back compatibility with old IndexEntry encoding
        defaults ++ descriptors.filterNot(defaults.contains)
      }
    }
    new IndexValueEncoder(sft, schema)
  }

  /**
   * Gets a concrete class we can use as a map key
   *
   * @param clas
   * @return
   */
  private def normalizeType(clas: Class[_]): Class[_] =
    clas match {
      case c if classOf[String].isAssignableFrom(c)              => classOf[String]
      case c if classOf[java.lang.Integer].isAssignableFrom(c)   => classOf[Int]
      case c if classOf[java.lang.Long].isAssignableFrom(c)      => classOf[Long]
      case c if classOf[java.lang.Double].isAssignableFrom(c)    => classOf[Double]
      case c if classOf[java.lang.Float].isAssignableFrom(c)     => classOf[Float]
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)   => classOf[Boolean]
      case c if classOf[UUID].isAssignableFrom(c)                => classOf[UUID]
      case c if classOf[Date].isAssignableFrom(c)                => classOf[Date]
      case c if classOf[Geometry].isAssignableFrom(c)            => classOf[Geometry]
      case _                                                     =>
        throw new IllegalArgumentException(s"Invalid type for index encoding: $clas")
    }

  // functions for encoding/decoding different classes

  private type GetValueFunction[T] = (T) => Any
  private type GetSizeFunction[T]  = (T) => Int
  private type EncodeFunction[T]   = (T, ByteBuffer) => Unit
  private type DecodeFunction[T]   = (ByteBuffer) => T

  private val stringGetValue: GetValueFunction[String] =
    (value: String) => Option(value).map(_.getBytes("UTF-8")).getOrElse(Array.empty[Byte])
  private val stringGetSize: GetSizeFunction[Array[Byte]] =
    (value: Array[Byte]) => value.length + 4
  private val stringEncode: EncodeFunction[Array[Byte]] =
    (value: Array[Byte], buf: ByteBuffer) => buf.putInt(value.length).put(value)
  private val stringDecode: DecodeFunction[String] =
    (buf: ByteBuffer) => {
      val length = buf.getInt
      if (length == 0) {
        null
      } else {
        val array = Array.ofDim[Byte](length)
        buf.get(array)
        new String(array, "UTF-8")
      }
    }

  private val intGetValue: GetValueFunction[Int] = (value: Int) => Option(value).getOrElse(0)
  private val intGetSize: GetSizeFunction[Int] = (_: Int) => 4
  private val intEncode: EncodeFunction[Int] = (value: Int, buf: ByteBuffer) => buf.putInt(value)
  private val intDecode: DecodeFunction[Int] = (buf: ByteBuffer) => buf.getInt

  private val longGetValue: GetValueFunction[Long] = (value: Long) => Option(value).getOrElse(0L)
  private val longGetSize: GetSizeFunction[Long] = (_: Long) => 8
  private val longEncode: EncodeFunction[Long] = (value: Long, buf: ByteBuffer) => buf.putLong(value)
  private val longDecode: DecodeFunction[Long] = (buf: ByteBuffer) => buf.getLong

  private val doubleGetValue: GetValueFunction[Double] = (value: Double) => Option(value).getOrElse(0d)
  private val doubleGetSize: GetSizeFunction[Double] = (value: Double) => 8
  private val doubleEncode: EncodeFunction[Double] =
    (value: Double, buf: ByteBuffer) => buf.putDouble(value)
  private val doubleDecode: DecodeFunction[Double] = (buf: ByteBuffer) => buf.getDouble

  private val floatGetValue: GetValueFunction[Float] = (value: Float) => Option(value).getOrElse(0f)
  private val floatGetSize: GetSizeFunction[Float] = (_: Float) => 4
  private val floatEncode: EncodeFunction[Float] = (value: Float, buf: ByteBuffer) => buf.putFloat(value)
  private val floatDecode: DecodeFunction[Float] = (buf: ByteBuffer) => buf.getFloat

  private val booleanGetValue: GetValueFunction[Boolean] =
    (value: Boolean) => if (Option(value).getOrElse(false)) 1.toByte else 0.toByte
  private val booleanGetSize: GetSizeFunction[Byte] = (_: Byte) => 1
  private val booleanEncode: EncodeFunction[Byte] = (value: Byte, buf: ByteBuffer) => buf.put(value)
  private val booleanDecode: DecodeFunction[Boolean] = (buf: ByteBuffer) => buf.get == 1.toByte

  private val uuidGetValue: GetValueFunction[UUID] = (value: UUID) => Option(value)
  private val uuidGetSize: GetSizeFunction[Option[UUID]] =
    (value: Option[UUID]) => if (value.isDefined) 17 else 1
  private val uuidEncode: EncodeFunction[Option[UUID]] =
    (value: Option[UUID], buf: ByteBuffer) => value match {
      case Some(uuid) =>
        buf.put(1.toByte).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)
      case None => buf.put(0.toByte)
    }
  private val uuidDecode: DecodeFunction[UUID] =
    (buf: ByteBuffer) => if (buf.get == 0.toByte) null else new UUID(buf.getLong, buf.getLong)

  private val dateGetValue: GetValueFunction[Date] = (value: Date) => Option(value).map(_.getTime)
  private val dateGetSize: GetSizeFunction[Option[Long]] =
    (value: Option[Long]) => if (value.isDefined) 9 else 1
  private val dateEncode: EncodeFunction[Option[Long]] =
    (value: Option[Long], buf: ByteBuffer) => value match {
      case Some(long) => buf.put(1.toByte).putLong(long)
      case None => buf.put(0.toByte)
    }
  private val dateDecode: DecodeFunction[Date] =
    (buf: ByteBuffer) => if (buf.get == 0.toByte) null else new Date(buf.getLong)
  private val dateDecodeBackCompatible: DecodeFunction[Date] = (buf: ByteBuffer) =>
    if (buf.remaining() == 8) {
      new Date(buf.getLong) // back compatible test, doesn't include null byte check
    } else if (buf.get == 0.toByte) null else new Date(buf.getLong)

  private val geomGetValue: GetValueFunction[Geometry] = (value: Geometry) => WKBUtils.write(value)
  private val geomGetSize: GetSizeFunction[Array[Byte]] = (value: Array[Byte]) => value.length + 4
  private val geomEncode: EncodeFunction[Array[Byte]] =
    (value: Array[Byte], buf: ByteBuffer) => buf.putInt(value.length).put(value)
  private val geomDecode: DecodeFunction[Geometry] =
    (buf: ByteBuffer) => {
      val length = buf.getInt
      val array = Array.ofDim[Byte](length)
      buf.get(array)
      WKBUtils.read(array)
    }

  private val getValueFunctions = Map[Class[_], GetValueFunction[_]](
    classOf[String]   -> stringGetValue,
    classOf[Int]      -> intGetValue,
    classOf[Long]     -> longGetValue,
    classOf[Double]   -> doubleGetValue,
    classOf[Float]    -> floatGetValue,
    classOf[Boolean]  -> booleanGetValue,
    classOf[UUID]     -> uuidGetValue,
    classOf[Date]     -> dateGetValue,
    classOf[Geometry] -> geomGetValue
  )

  private val getSizeFunctions = Map[Class[_], GetSizeFunction[_]](
    classOf[String]   -> stringGetSize,
    classOf[Int]      -> intGetSize,
    classOf[Long]     -> longGetSize,
    classOf[Double]   -> doubleGetSize,
    classOf[Float]    -> floatGetSize,
    classOf[Boolean]  -> booleanGetSize,
    classOf[UUID]     -> uuidGetSize,
    classOf[Date]     -> dateGetSize,
    classOf[Geometry] -> geomGetSize
  )

  private val encodeFunctions = Map[Class[_], EncodeFunction[_]](
    classOf[String]   -> stringEncode,
    classOf[Int]      -> intEncode,
    classOf[Long]     -> longEncode,
    classOf[Double]   -> doubleEncode,
    classOf[Float]    -> floatEncode,
    classOf[Boolean]  -> booleanEncode,
    classOf[UUID]     -> uuidEncode,
    classOf[Date]     -> dateEncode,
    classOf[Geometry] -> geomEncode
  )

  private val decodeFunctionBase = Map[Class[_], DecodeFunction[_]](
    classOf[String]   -> stringDecode,
    classOf[Int]      -> intDecode,
    classOf[Long]     -> longDecode,
    classOf[Double]   -> doubleDecode,
    classOf[Float]    -> floatDecode,
    classOf[Boolean]  -> booleanDecode,
    classOf[UUID]     -> uuidDecode,
    classOf[Geometry] -> geomDecode
  )

  private val decodeFunctions =
    decodeFunctionBase ++ Map[Class[_], DecodeFunction[_]](classOf[Date] -> dateDecode)

  private val backCompatibleDecodeFunctions =
    decodeFunctionBase ++ Map[Class[_], DecodeFunction[_]](classOf[Date] -> dateDecodeBackCompatible)
}