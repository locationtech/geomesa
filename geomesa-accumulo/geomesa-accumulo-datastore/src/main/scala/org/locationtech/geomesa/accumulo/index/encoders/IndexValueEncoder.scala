/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.encoders

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer, ProjectingKryoFeatureSerializer}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocalCache}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object IndexValueEncoder {

  import scala.collection.JavaConversions._

  private val cache = new SoftThreadLocalCache[String, SimpleFeatureType]()

  @deprecated
  def apply(sft: SimpleFeatureType, transform: SimpleFeatureType): SimpleFeatureSerializer = {
    val key = CacheKeyGenerator.cacheKey(sft)
    val indexSft = cache.getOrElseUpdate(key, getIndexSft(sft))
    if (sft.getSchemaVersion < 4) { // kryo encoding introduced in version 4
      OldIndexValueEncoder(sft, transform)
    } else {
      val encoder = new KryoFeatureSerializer(indexSft)
      val decoder = new ProjectingKryoFeatureDeserializer(indexSft, transform)
      val copyFunction = getCopyFunction(sft, indexSft)
      new IndexValueEncoderImpl(copyFunction, indexSft, encoder, decoder)
    }
  }


  def apply(sft: SimpleFeatureType, includeIds: Boolean = false): SimpleFeatureSerializer = {
    val key = CacheKeyGenerator.cacheKey(sft)
    val indexSft = cache.getOrElseUpdate(key, getIndexSft(sft))
    if (sft.getSchemaVersion < 4) { // kryo encoding introduced in version 4
      OldIndexValueEncoder(sft, indexSft)
    } else if (includeIds) {
      val encoder = new KryoFeatureSerializer(indexSft)
      val copyFunction = getCopyFunction(sft, indexSft)
      new IndexValueEncoderImpl(copyFunction, indexSft, encoder, encoder)
    } else {
      new ProjectingKryoFeatureSerializer(sft, indexSft, SerializationOptions.withoutId)
    }
  }

  /**
   * Gets a feature type compatible with the stored index value
   *
   * @param sft simple feature type
   * @return
   */
  def getIndexSft(sft: SimpleFeatureType) = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setNamespaceURI(null: String)
    builder.setName(sft.getTypeName + "--index")
    builder.setAttributes(getIndexValueAttributes(sft))
    if (sft.getGeometryDescriptor != null) {
      builder.setDefaultGeometry(sft.getGeometryDescriptor.getLocalName)
    }
    builder.setCRS(sft.getCoordinateReferenceSystem)
    val indexSft = builder.buildFeatureType()
    indexSft.getUserData.putAll(sft.getUserData)
    indexSft
  }


  /**
   * Gets a feature type compatible with the stored index value
   *
   * @param sft simple feature type
   * @return
   */
  protected[index] def getCopyFunction(sft: SimpleFeatureType, indexSft: SimpleFeatureType) = {
    val indices = indexSft.getAttributeDescriptors.map(ad => sft.indexOf(ad.getLocalName)).toArray
    (sf: SimpleFeature, copyTo: SimpleFeature) => {
      copyTo.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
      var i = 0
      while (i < indices.length) {
        copyTo.setAttribute(i, sf.getAttribute(indices(i)))
        i += 1
      }
    }
  }

  /**
   * Gets the attributes that are stored in the index value
   *
   * @param sft simple feature type
   * @return
   */
  protected[index] def getIndexValueAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    val geom = sft.getGeometryDescriptor
    val dtg = sft.getDtgField
    val attributes = scala.collection.mutable.Buffer.empty[AttributeDescriptor]
    var i = 0
    while (i < sft.getAttributeCount) {
      val ad = sft.getDescriptor(i)
      if (ad == geom || dtg.exists(_ == ad.getLocalName) || ad.isIndexValue()) {
        attributes.append(ad)
      }
      i += 1
    }
    attributes
  }

  /**
   * Gets the attribute names that are stored in the index value
   *
   * @param sft simple feature type
   * @return
   */
  def getIndexValueFields(sft: SimpleFeatureType): Seq[String] =
    getIndexValueAttributes(sft).map(_.getLocalName)
}

/**
 * Encoder/decoder for index values. Allows customizable fields to be encoded. Not thread-safe.
 *
 * We need a separate encoder/decoder, as the encoder takes a 'full' feature and only writes the 'reduced'
 * parts, while the decoder reads the 'reduced' feature.
 *
 * @param encoder
 * @param decoder
 */
@deprecated
class IndexValueEncoderImpl(copyFeature: (SimpleFeature, SimpleFeature) => Unit,
                            indexSft: SimpleFeatureType,
                            encoder: SimpleFeatureSerializer,
                            decoder: SimpleFeatureSerializer) extends SimpleFeatureSerializer {

  val reusableFeature = new ScalaSimpleFeature("", indexSft)

  override val options: Set[SerializationOption] = Set.empty

  override def serialize(sf: SimpleFeature): Array[Byte] = {
    copyFeature(sf, reusableFeature)
    encoder.serialize(reusableFeature)
  }

  override def deserialize(value: Array[Byte]): SimpleFeature = decoder.deserialize(value)
}


/**
 * Encoder/decoder for index values. Allows customizable fields to be encoded. Thread-safe.
 *
 * @param sft
 * @param fields
 */
@deprecated
class OldIndexValueEncoder(sft: SimpleFeatureType, encodedSft: SimpleFeatureType, val fields: Seq[String])
    extends SimpleFeatureSerializer {

  import OldIndexValueEncoder._

  fields.foreach(f => require(sft.getDescriptor(f) != null ||
    f == ID_FIELD, s"Encoded field does not exist: $f"))

  override val options: Set[SerializationOption] = Set.empty

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
  override def serialize(sf: SimpleFeature): Array[Byte] = {
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
  val dtgField = sft.getDtgField

  /**
   * Decodes a byte array into a map of attribute name -> attribute value pairs
   *
   * @param value
   * @return
   */
  override def deserialize(value: Array[Byte]): SimpleFeature = {
    val buf = ByteBuffer.wrap(value)
    val values = fieldsWithIndex.map { case (f, i) => f -> decodings(i)(buf) }.toMap
    val sf = new ScalaSimpleFeature(values(ID_FIELD).asInstanceOf[String], encodedSft)
    values.foreach { case (key, value) =>
      if (encodedSft.indexOf(key) != -1) {
        sf.setAttribute(key, value.asInstanceOf[AnyRef])
      }
    }
    sf
  }
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

@Deprecated
object OldIndexValueEncoder {

  val ID_FIELD = "id"

  // even though the encoders are thread safe, this way we don't have to synchronize when retrieving them
  private val cache = new ThreadLocal[scala.collection.mutable.Map[String, SimpleFeatureSerializer]] {
    override def initialValue() = scala.collection.mutable.Map.empty
  }

  // gets a cached instance to avoid the initialization overhead
  // we use sft.toString, which includes the fields and type name, as a unique key
  def apply(sft: SimpleFeatureType, transform: SimpleFeatureType) = {
    val key = CacheKeyGenerator.cacheKey(sft) + CacheKeyGenerator.cacheKey(transform)
    cache.get.getOrElseUpdate(key, new OldIndexValueEncoder(sft, transform, getSchema(sft)))
  }

  // gets the default schema, which includes ID, geom and date (if available)
  // order is important here, as it needs to match the old IndexEntry encoding
  def getDefaultSchema(sft: SimpleFeatureType): Seq[String] =
    Seq(ID_FIELD, sft.getGeometryDescriptor.getLocalName) ++ sft.getDtgField

  def getSchema(sft: SimpleFeatureType): Seq[String] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._

    val defaults = getDefaultSchema(sft)
    val descriptors =  sft.getAttributeDescriptors.filter(_.isIndexValue()).map(_.getLocalName)
    if (descriptors.isEmpty) {
      defaults
    } else {
      // keep defaults first to maintain back compatibility with old IndexEntry encoding
      defaults ++ descriptors.filterNot(defaults.contains)
    }
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
