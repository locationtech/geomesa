/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.feature.kryo

import java.util.{Date, List => JList, Map => JMap, UUID}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKBConstants
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature.ScalaSimpleFeature
import org.locationtech.geomesa.feature.serialization.{SimpleFeatureDecodings, DatumReader}
import org.locationtech.geomesa.feature.serialization.kryo.{KryoReader, KryoSimpleFeatureDecodingsCache}
import org.locationtech.geomesa.feature.serialization.kryo.KryoSimpleFeatureDecodingsCache._
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Kryo serialization implementation for simple features. This class shouldn't be used directly -
 * see @KryoFeatureSerializer
 *
 * @param sft
 */
class SimpleFeatureSerializer(sft: SimpleFeatureType, opts: EncodingOptions = EncodingOptions.none)
  extends BaseSimpleFeatureSerializer(sft, opts) {

  import org.locationtech.geomesa.feature.kryo.SimpleFeatureSerializer._

  val encodings = sftEncodings(sft)
  val decodings: SimpleFeatureDecodings[Input] = sftDecodings(sft)

  override def writeAttributes(output: Output, sf: SimpleFeature) = {
    var i = 0
    while (i < encodings.length) {
      encodings(i)(output, sf)
      i += 1
    }
  }

  def readAttributes(input: Input, version: Int): Array[AnyRef] = {
    val values = Array.ofDim[AnyRef](sft.getAttributeCount)
    var i = 0

    val attributeDecodings = decodings.attributeDecodings
    while (i < attributeDecodings.length) {
      values(i) = attributeDecodings(i)(input, version)
      i += 1
    }

    values
  }
}

/**
 * Reads just the id from a serialized simple feature
 */
class FeatureIdSerializer extends Serializer[KryoFeatureId] {

  override def write(kryo: Kryo, output: Output, id: KryoFeatureId): Unit = ???

  override def read(kryo: Kryo, input: Input, typ: Class[KryoFeatureId]): KryoFeatureId = {
    input.readInt(true) // discard version info, not used for ID
    KryoFeatureId(input.readString())
  }
}

/**
 * Kryo serialization implementation for simple features - provides transformation during read and write
 *
 * @param sft
 * @param transform
 */
class TransformingSimpleFeatureSerializer(sft: SimpleFeatureType, transform: SimpleFeatureType, options: EncodingOptions)
    extends BaseSimpleFeatureSerializer(transform, options) {

  import org.locationtech.geomesa.feature.kryo.SimpleFeatureSerializer._

  val (transformEncodings, transformDecodings) = {
    val encodings = sftEncodings(sft)
    val decodings = sftDecodings(sft).attributeDecodings

    val enc = scala.collection.mutable.ArrayBuffer.empty[Encoding]
    val dec = scala.collection.mutable.ArrayBuffer.empty[(DatumReader[Input, AnyRef], Int)]

    var i = 0
    while (i < encodings.length) {
      val index = transform.indexOf(sft.getDescriptor(i).getLocalName)
      if (index != -1) {
        enc.append(encodings(i))
      }
      dec.append((decodings(i), index))
      i += 1
    }
    (enc, dec)
  }

  override def writeAttributes(output: Output, sf: SimpleFeature): Unit = {
    var i = 0
    while (i < transformEncodings.length) {
      transformEncodings(i)(output, sf)
      i += 1
    }
  }

  override def readAttributes(input: Input, version: Int): Array[AnyRef] = {
    val values = Array.ofDim[AnyRef](transform.getAttributeCount)
    var i = 0
    while (i < transformDecodings.length) {
      val (decoding, index) = transformDecodings(i)
      if (index == -1) {
        decoding(input, version) // discard
      } else {
        values(index) = decoding(input, version)
      }
      i += 1
    }
    values
  }
}

abstract class BaseSimpleFeatureSerializer(sft: SimpleFeatureType, opts: EncodingOptions)
  extends Serializer[SimpleFeature] {

  import org.locationtech.geomesa.feature.kryo.SimpleFeatureSerializer.VERSION

  val doWrite: (Kryo, Output, SimpleFeature) => Unit =
    if (opts.withUserData) writeWithUserData else defaultWrite

  val reader: (Kryo, Input, Class[SimpleFeature], Int) => SimpleFeature =
    if (opts.withUserData) readWithUserData else defaultRead

  override def write(kryo: Kryo, output: Output, sf: SimpleFeature) = doWrite(kryo, output, sf)
  override def read(kryo: Kryo, input: Input, typ: Class[SimpleFeature]) = {
    val version = input.readInt(true)
    reader(kryo, input, typ, version)
  }

  def defaultWrite(kryo: Kryo, output: Output, sf: SimpleFeature): Unit = {
    output.writeInt(VERSION, true)
    output.writeString(sf.getID)

    writeAttributes(output, sf)
  }

  def writeWithUserData(kryo: Kryo, output: Output, sf: SimpleFeature) = {
    defaultWrite(kryo, output, sf)

    // TODO move out and use everywhere
    val kw = new org.locationtech.geomesa.feature.serialization.kryo.KryoWriter(output)
    kw.writeGenericMap(sf.getUserData)
  }

  def defaultRead(kryo: Kryo, input: Input, typ: Class[SimpleFeature], version: Int): SimpleFeature = {
    val id = input.readString()

    val values = readAttributes(input, version)

    new ScalaSimpleFeature(id, sft, values)
  }

  def readWithUserData(kryo: Kryo, input: Input, typ: Class[SimpleFeature], serializationVersion: Int): SimpleFeature = {
    val sf = defaultRead(kryo, input, typ, serializationVersion)

    val kr = sftDecodings(sft).datumReaders

    val userData = kr.readGenericMap(input, serializationVersion)
    sf.getUserData.clear()
    sf.getUserData.putAll(userData)
    sf
  }

  def writeAttributes(output: Output, sf: SimpleFeature)
  def readAttributes(input: Input, version: Int): Array[AnyRef]
}

object SimpleFeatureSerializer {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

  val VERSION = 1

  val NULL_BYTE     = 0.asInstanceOf[Byte]
  val NON_NULL_BYTE = 1.asInstanceOf[Byte]

  // [[Encoding]] is attribute specific, it extracts a single attribute from the simple feature
  type Encoding = (Output, SimpleFeature) => Unit

  type AttributeWriter = (Output, AnyRef) => Unit

  // encodings are cached per-thread to avoid synchronization issues
  // we use soft references to allow garbage collection as needed
  private val encodingsCache = new SoftThreadLocalCache[String, Array[Encoding]]()


  import org.locationtech.geomesa.feature.serialization.CacheKeyGenerator._
  /**
   * Gets a seq of functions to encode the attributes of simple feature
   *
   * @param sft
   * @return
   */
  def sftEncodings(sft: SimpleFeatureType): Array[Encoding] =
    encodingsCache.getOrElseUpdate(cacheKeyForSFT(sft), {
      sft.getAttributeDescriptors.zipWithIndex.toArray.map { case (d, i) =>
        val encode = matchEncode(d.getType.getBinding, d.getUserData)
        (out: Output, sf: SimpleFeature) => encode(out, sf.getAttribute(i))
      }
    })

  /**
   * Finds an encoding function based on the input type
   *
   * @param clas
   * @param metadata
   * @return
   */
  def matchEncode(clas: Class[_], metadata: JMap[AnyRef, AnyRef]): AttributeWriter = clas match {

    case c if classOf[String].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => out.writeString(value.asInstanceOf[String])

    case c if classOf[java.lang.Integer].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeInt(value.asInstanceOf[java.lang.Integer]) )

    case c if classOf[java.lang.Long].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeLong(value.asInstanceOf[java.lang.Long]) )

    case c if classOf[java.lang.Double].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeDouble(value.asInstanceOf[java.lang.Double]) )

    case c if classOf[java.lang.Float].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeFloat(value.asInstanceOf[java.lang.Float]) )

    case c if classOf[java.lang.Boolean].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeBoolean(value.asInstanceOf[java.lang.Boolean]) )

    case c if classOf[UUID].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => {
        out.writeLong(value.asInstanceOf[UUID].getMostSignificantBits)
        out.writeLong(value.asInstanceOf[UUID].getLeastSignificantBits)
      })

    case c if classOf[Date].isAssignableFrom(c) =>
      nullableWriter( (out: Output, value: AnyRef) => out.writeLong(value.asInstanceOf[Date].getTime) )

    case c if classOf[Geometry].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => writeGeometry(out, value.asInstanceOf[Geometry])

    case c if classOf[JList[_]].isAssignableFrom(c) =>
      val subtype = metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]
      val subEncoding = matchEncode(subtype, null)

      (out: Output, value: AnyRef) => {
        val list = value.asInstanceOf[JList[Object]]
        if (list == null) {
          out.writeInt(-1): Unit
        } else {
          out.writeInt(list.size())
          list.foreach(subEncoding(out, _))
        }
      }

    case c if classOf[JMap[_, _]].isAssignableFrom(c) =>
      val keyClass      = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
      val valueClass    = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
      val keyEncoding   = matchEncode(keyClass, null)
      val valueEncoding = matchEncode(valueClass, null)

      (out: Output, value: AnyRef) => {
        val map = value.asInstanceOf[JMap[Object, Object]]
        if (map == null) {
          out.writeInt(-1): Unit
        } else {
          out.writeInt(map.size())
          map.entrySet.foreach { e => keyEncoding(out, e.getKey); valueEncoding(out, e.getValue) }
        }
      }
  }

  /**
   * Write a null or not null flag and if not null then the value.
   *
   * @param writeNonNull the [[[AttributeWriter]] handling the non-null case
   * @return a [[AttributeWriter]] capable of handling potentially null values
   */
  def nullableWriter(writeNonNull: AttributeWriter): AttributeWriter = (out: Output, value: AnyRef) => {
    if (value == null) {
      out.writeByte(NULL_BYTE)
    } else {
      out.writeByte(NON_NULL_BYTE)
      writeNonNull(out, value)
    }
  }

  /**
   * Based on the method from geotools WKBWriter. This method is optimized for kryo and simplified from
   * WKBWriter in the following ways:
   *
   * 1. Doesn't save SRID (geomesa didn't use that functionality in WKBWriter)
   * 2. Doesn't handle dimensions > 2
   * 3. Doesn't worry about byte order (handled by kryo)
   * 4. Doesn't use a precision model
   *
   * @param out
   * @param geom
   */
  def writeGeometry(out: Output, geom: Geometry): Unit = nullableWriter( (out, geom) => {
    geom match {
      case g: Point =>
        out.writeInt(WKBConstants.wkbPoint, true)
        writeCoordinate(out, g.getCoordinateSequence.getCoordinate(0))

      case g: LineString =>
        out.writeInt(WKBConstants.wkbLineString, true)
        writeCoordinateSequence(out, g.getCoordinateSequence)

      case g: Polygon => writePolygon(out, g)

      case g: MultiPoint => writeGeometryCollection(out, WKBConstants.wkbMultiPoint, g)

      case g: MultiLineString => writeGeometryCollection(out, WKBConstants.wkbMultiLineString, g)

      case g: MultiPolygon => writeGeometryCollection(out, WKBConstants.wkbMultiPolygon, g)

      case g: GeometryCollection => writeGeometryCollection(out, WKBConstants.wkbGeometryCollection, g)
    }
  })(out, geom)

  def writePolygon(out: Output, g: Polygon): Unit = {
    out.writeInt(WKBConstants.wkbPolygon, true)
    writeCoordinateSequence(out, g.getExteriorRing.getCoordinateSequence)
    out.writeInt(g.getNumInteriorRing, true)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, g.getInteriorRingN(i).getCoordinateSequence)
      i += 1
    }
  }

  def writeGeometryCollection(out: Output, typ: Int, g: GeometryCollection): Unit = {
    out.writeInt(typ, true)
    out.writeInt(g.getNumGeometries, true)
    var i = 0
    while (i < g.getNumGeometries) {
      writeGeometry(out, g.getGeometryN(i))
      i += 1
    }
  }

  def writeCoordinateSequence(out: Output, coords: CoordinateSequence): Unit = {
    out.writeInt(coords.size(), true)
    var i = 0
    while (i < coords.size()) {
      writeCoordinate(out, coords.getCoordinate(i))
      i += 1
    }
  }

  def writeCoordinate(out: Output, coord: Coordinate): Unit = {
    out.writeDouble(coord.getOrdinate(0))
    out.writeDouble(coord.getOrdinate(1))
  }
}