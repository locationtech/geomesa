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
import org.locationtech.geomesa.feature.serialization.avro.AvroWriter
import org.locationtech.geomesa.feature.serialization.kryo.{KryoReader, KryoWriter}
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.text.WKBUtils
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
  val decodings = sftDecodings(sft)

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
    while (i < decodings.length) {
      values(i) = decodings(i)(input, version)
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
    val decodings = sftDecodings(sft)

    val enc = scala.collection.mutable.ArrayBuffer.empty[Encoding]
    val dec = scala.collection.mutable.ArrayBuffer.empty[(Decoding, Int)]

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

  val doRead: (Kryo, Input, Class[SimpleFeature]) => SimpleFeature =
    if (opts.withUserData) readWithUserData else defaultRead

  override def write(kryo: Kryo, output: Output, sf: SimpleFeature) = doWrite(kryo, output, sf)
  override def read(kryo: Kryo, input: Input, typ: Class[SimpleFeature]) = doRead(kryo, input, typ)

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

  def defaultRead(kryo: Kryo, input: Input, typ: Class[SimpleFeature]): SimpleFeature = {
    val version = input.readInt(true)
    val id = input.readString()

    val values = readAttributes(input, version)

    new ScalaSimpleFeature(id, sft, values)
  }

  def readWithUserData(kryo: Kryo, input: Input, typ: Class[SimpleFeature]): SimpleFeature = {
    val sf = defaultRead(kryo, input, typ)

    // TODO move out and use everywhere
    val kr = new KryoReader(input)

    val userData = kr.readGenericMap()
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
  type Decoding = (Input, Int) => AnyRef

  type AttributeWriter = (Output, AnyRef) => Unit
  type AttributeReader[A <: AnyRef] = (Input, Int) => A

  // encodings are cached per-thread to avoid synchronization issues
  // we use soft references to allow garbage collection as needed
  private val encodingsCache = new SoftThreadLocalCache[String, Array[Encoding]]()
  private val decodingsCache = new SoftThreadLocalCache[String, Array[Decoding]]()

  def cacheKeyForSFT(sft: SimpleFeatureType) =
    s"${sft.getName};${sft.getAttributeDescriptors.map(ad => s"${ad.getName.toString}${ad.getType}").mkString(",")}"

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

  /**
   * Gets a sequence of functions to decode the attributes of a simple feature
   *
   * @param sft
   * @return
   */
  def sftDecodings(sft: SimpleFeatureType): Array[Decoding] =
    decodingsCache.getOrElseUpdate(cacheKeyForSFT(sft), {
      sft.getAttributeDescriptors.map { d =>
        matchDecode(d.getType.getBinding, d.getUserData)
      }.toArray
    })

  /**
   * Finds an decoding function based on the input type
   *
   * @param clas
   * @param metadata
   * @return
   */
  def matchDecode(clas: Class[_], metadata: JMap[Object, Object]): AttributeReader[AnyRef] = clas match {

    case c if classOf[String].isAssignableFrom(c) =>
      (in: Input, version: Int) => in.readString()

    case c if classOf[java.lang.Integer].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => Int.box(in.readInt()) )

    case c if classOf[java.lang.Long].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => Long.box(in.readLong()) )

    case c if classOf[java.lang.Double].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => Double.box(in.readDouble()) )

    case c if classOf[java.lang.Float].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => Float.box(in.readFloat()) )

    case c if classOf[java.lang.Boolean].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => Boolean.box(in.readBoolean()) )

    case c if classOf[Date].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) => new Date(in.readLong()) )

    case c if classOf[UUID].isAssignableFrom(c) =>
      nullableReader( (in: Input, version: Int) =>  {
        val mostSignificantBits = in.readLong()
        val leastSignificantBits = in.readLong()
        new UUID(mostSignificantBits, leastSignificantBits)
      })

    case c if classOf[Geometry].isAssignableFrom(c) =>
      val factory = new GeometryFactory()
      val csFactory = factory.getCoordinateSequenceFactory
      (in: Input, version: Int) =>
        if (version == 0) {
          val length = in.readInt(true)
          if (length > 0) {
            val bytes = new Array[Byte](length)
            in.read(bytes)
            WKBUtils.read(bytes)
          } else {
            null
          }
        } else {
          readGeometry(in, factory, csFactory)
        }

    case c if classOf[JList[_]].isAssignableFrom(c) =>
      val subtype = metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]
      val subDecoding = matchDecode(subtype, null)

      (in: Input, version: Int) => {
        val length = in.readInt()
        if (length < 0) {
          null
        } else {
          val list = new java.util.ArrayList[Object](length)
          var i = 0
          while (i < length) {
            list.add(subDecoding(in, version))
            i += 1
          }
          list
        }
      }

    case c if classOf[JMap[_, _]].isAssignableFrom(c) =>
      val keyClass      = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
      val valueClass    = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
      val keyDecoding   = matchDecode(keyClass, null)
      val valueDecoding = matchDecode(valueClass, null)

      (in: Input, version: Int) => {
        val length = in.readInt()
        if (length < 0) {
          null
        } else {
          val map = new java.util.HashMap[Object, Object](length)
          var i = 0
          while (i < length) {
            map.put(keyDecoding(in, version), valueDecoding(in, version))
            i += 1
          }
          map
        }
      }
  }

  /**
   * Read a null or not null flag and if not null then the value.
   *
   * @param readNonNull the [[AttributeReader]] for reading the value in the not null case
   * @return a [[AttributeReader]] capable of reading a potentially null value
   */
  def nullableReader[A <: AnyRef](readNonNull: AttributeReader[A]): AttributeReader[A] = (in: Input, version: Int) =>
    if (in.readByte() == NULL_BYTE) null.asInstanceOf[A] else readNonNull(in, version)


  /**
   * Based on the method from geotools WKBReader.
   *
   * @param in
   * @param factory
   * @param csFactory
   * @return
   */
  def readGeometry(in: Input, factory: GeometryFactory, csFactory: CoordinateSequenceFactory): Geometry =
    nullableReader( (in: Input, version: Int) => {
      in.readInt(true) match {
        case WKBConstants.wkbPoint => factory.createPoint(readCoordinate(in, csFactory))

        case WKBConstants.wkbLineString => factory.createLineString(readCoordinateSequence(in, csFactory))

        case WKBConstants.wkbPolygon => readPolygon(in, factory, csFactory)

        case WKBConstants.wkbMultiPoint =>
          val geoms = readGeometryCollection[Point](in, factory, csFactory)
          factory.createMultiPoint(geoms)

        case WKBConstants.wkbMultiLineString =>
          val geoms = readGeometryCollection[LineString](in, factory, csFactory)
          factory.createMultiLineString(geoms)

        case WKBConstants.wkbMultiPolygon =>
          val geoms = readGeometryCollection[Polygon](in, factory, csFactory)
          factory.createMultiPolygon(geoms)

        case WKBConstants.wkbGeometryCollection =>
          val geoms = readGeometryCollection[Geometry](in, factory, csFactory)
          factory.createGeometryCollection(geoms)
      }
  })(in, 1)

  def readPolygon(in: Input, factory: GeometryFactory, csFactory: CoordinateSequenceFactory): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in, csFactory))
    val numInteriorRings = in.readInt(true)
    if (numInteriorRings == 0) {
      factory.createPolygon(exteriorRing)
    } else {
      val interiorRings = Array.ofDim[LinearRing](numInteriorRings)
      var i = 0
      while (i < numInteriorRings) {
        interiorRings.update(i, factory.createLinearRing(readCoordinateSequence(in, csFactory)))
        i += 1
      }
      factory.createPolygon(exteriorRing, interiorRings)
    }
  }

  def readGeometryCollection[T <: Geometry: ClassTag](in: Input,
                                                      factory: GeometryFactory,
                                                      csFactory: CoordinateSequenceFactory): Array[T] = {
    val numGeoms = in.readInt(true)
    val geoms = Array.ofDim[T](numGeoms)
    var i = 0
    while (i < numGeoms) {
      geoms.update(i, readGeometry(in, factory, csFactory).asInstanceOf[T])
      i += 1
    }
    geoms
  }

  def readCoordinateSequence(in: Input, csFactory: CoordinateSequenceFactory): CoordinateSequence = {
    val numCoords = in.readInt(true)
    val coords = csFactory.create(numCoords, 2)
    var i = 0
    while (i < numCoords) {
      coords.setOrdinate(i, 0, in.readDouble())
      coords.setOrdinate(i, 1, in.readDouble())
      i += 1
    }
    coords
  }

  def readCoordinate(in: Input, csFactory: CoordinateSequenceFactory): CoordinateSequence = {
    val coords = csFactory.create(1, 2)
    coords.setOrdinate(0, 0, in.readDouble())
    coords.setOrdinate(0, 1, in.readDouble())
    coords
  }
}