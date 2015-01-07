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

import java.util.{Date, List => jList, Map => jMap, UUID}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.ref.SoftReference

/**
 * Kryo serialization implementation for simple features. This class shouldn't be used directly -
 * see @KryoFeatureSerializer
 *
 * @param sft
 */
class SimpleFeatureSerializer(sft: SimpleFeatureType) extends Serializer[SimpleFeature] {

  import org.locationtech.geomesa.feature.kryo.SimpleFeatureSerializer._

  val encodings = sftEncodings(sft)
  val decodings = sftDecodings(sft)

  override def write(kryo: Kryo, output: Output, sf: SimpleFeature): Unit = {
    output.writeInt(VERSION, true)
    output.writeString(sf.getID)
    encodings.foreach(encode => encode(output, sf))
  }

  override def read(kryo: Kryo, input: Input, typ: Class[SimpleFeature]): SimpleFeature = {
    input.readInt(true) // discard version info, currently only one version
    val id = input.readString()
    val values = Array.ofDim[AnyRef](sft.getAttributeCount)

    decodings.foreach { case (decode, i) => values(i) = decode(input) }

    new KryoSimpleFeature(id, sft, values)
  }
}

/**
 * Reads just the id from a serialized simple feature
 */
class FeatureIdSerializer extends Serializer[KryoFeatureId] {

  override def write(kryo: Kryo, output: Output, id: KryoFeatureId): Unit = ???

  override def read(kryo: Kryo, input: Input, typ: Class[KryoFeatureId]): KryoFeatureId = {
    input.readInt(true) // discard version info, currently only one version
    KryoFeatureId(input.readString())
  }
}

/**
 * Kryo serialization implementation for simple features - provides transformation during read
 *
 * @param sft
 * @param decodeAs
 */
class TransformingSimpleFeatureSerializer(sft: SimpleFeatureType, decodeAs: SimpleFeatureType)
    extends SimpleFeatureSerializer(sft) {

  val transformDecodings = decodings.map {
    case (decode, i) => (decode, decodeAs.indexOf(sft.getDescriptor(i).getLocalName))
  }

  override def read(kryo: Kryo, input: Input, typ: Class[SimpleFeature]): SimpleFeature = {
    input.readInt(true) // discard version info, currently only one version
    val id = input.readString()
    val values = Array.ofDim[AnyRef](decodeAs.getAttributeCount)

    transformDecodings.foreach { case (decode, i) =>
      if (i == -1) decode(input) else values(i) = decode(input)
    }
    new KryoSimpleFeature(id, decodeAs, values)
  }
}

object SimpleFeatureSerializer {

  import scala.collection.mutable.Map

  val VERSION = 0

  val NULL_BYTE     = 0.asInstanceOf[Byte]
  val NON_NULL_BYTE = 1.asInstanceOf[Byte]

  type Encoding = (Output, SimpleFeature) => Unit
  type Decoding = ((Input) => AnyRef, Int)

  // encodings are cached per-thread to avoid synchronization issues
  // we use soft references to allow garbage collection as needed
  private val encodingsCache = new ThreadLocal[Map[String, SoftReference[Seq[Encoding]]]] {
    override def initialValue = Map.empty[String, SoftReference[Seq[Encoding]]]
  }
  private val decodingsCache = new ThreadLocal[Map[String, SoftReference[Seq[Decoding]]]] {
    override def initialValue = Map.empty[String, SoftReference[Seq[Decoding]]]
  }

  /**
   * Gets a seq of functions to encode the attributes of simple feature
   *
   * @param sft
   * @return
   */
  def sftEncodings(sft: SimpleFeatureType): Seq[Encoding] =
    encodingsCache.get.get(sft.toString).flatMap(_.get) match {
      case Some(functions) => functions
      case None =>
        val encodings = sft.getAttributeDescriptors.zipWithIndex.map { case (d, i) =>
          val encode = matchEncode(d.getType.getBinding, d.getUserData)
          (out: Output, sf: SimpleFeature) => encode(out, sf.getAttribute(i))
        }
        encodingsCache.get.put(sft.toString, new SoftReference(encodings))
        encodings
    }

  /**
   * Finds an encoding function based on the input type
   *
   * @param clas
   * @param metadata
   * @return
   */
  def matchEncode(clas: Class[_], metadata: jMap[AnyRef, AnyRef]): (Output, AnyRef) => Unit = clas match {

    case c if classOf[String].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => out.writeString(value.asInstanceOf[String])

    case c if classOf[java.lang.Integer].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeInt(value.asInstanceOf[java.lang.Integer])
      }

    case c if classOf[java.lang.Long].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeLong(value.asInstanceOf[java.lang.Long])
      }

    case c if classOf[java.lang.Double].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeDouble(value.asInstanceOf[java.lang.Double])
      }

    case c if classOf[java.lang.Float].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeFloat(value.asInstanceOf[java.lang.Float])
      }

    case c if classOf[java.lang.Boolean].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeBoolean(value.asInstanceOf[java.lang.Boolean])
      }

    case c if classOf[UUID].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) =>  if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeLong(value.asInstanceOf[UUID].getMostSignificantBits)
        out.writeLong(value.asInstanceOf[UUID].getLeastSignificantBits)
      }

    case c if classOf[Date].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeByte(NULL_BYTE)
      } else {
        out.writeByte(NON_NULL_BYTE)
        out.writeLong(value.asInstanceOf[Date].getTime)
      }

    case c if classOf[Geometry].isAssignableFrom(c) =>
      (out: Output, value: AnyRef) => if (value == null) {
        out.writeInt(0, true): Unit
      } else {
        val bytes = WKBUtils.write(value.asInstanceOf[Geometry])
        out.writeInt(bytes.length, true)
        out.write(bytes)
      }

    case c if classOf[jList[_]].isAssignableFrom(c) =>
      val subtype = metadata.get("subtype").asInstanceOf[Class[_]]
      val subEncoding = matchEncode(subtype, null)

      (out: Output, value: AnyRef) => {
        val list = value.asInstanceOf[jList[Object]]
        if (list == null) {
          out.writeInt(-1): Unit
        } else {
          out.writeInt(list.size())
          list.foreach(subEncoding(out, _))
        }
      }

    case c if classOf[jMap[_, _]].isAssignableFrom(c) =>
      val keyClass      = metadata.get("keyclass").asInstanceOf[Class[_]]
      val valueClass    = metadata.get("valueclass").asInstanceOf[Class[_]]
      val keyEncoding   = matchEncode(keyClass, null)
      val valueEncoding = matchEncode(valueClass, null)

      (out: Output, value: AnyRef) => {
        val map = value.asInstanceOf[jMap[Object, Object]]
        if (map == null) {
          out.writeInt(-1): Unit
        } else {
          out.writeInt(map.size())
          map.entrySet.foreach { e => keyEncoding(out, e.getKey); valueEncoding(out, e.getValue) }
        }
      }
  }

  /**
   * Gets a sequence of functions to decode the attributes of a simple feature
   *
   * @param sft
   * @return
   */
  def sftDecodings(sft: SimpleFeatureType): Seq[((Input) => AnyRef, Int)] =
    decodingsCache.get.get(sft.toString).flatMap(_.get) match {
      case Some(functions) => functions
      case None =>
        val decodings = sft.getAttributeDescriptors.map { d =>
          matchDecode(d.getType.getBinding, d.getUserData)
        }.zipWithIndex
        decodingsCache.get.put(sft.toString, new SoftReference(decodings))
        decodings
    }

  /**
   * Finds an decoding function based on the input type
   *
   * @param clas
   * @param metadata
   * @return
   */
  def matchDecode(clas: Class[_], metadata: jMap[Object, Object]): (Input) => AnyRef = clas match {

    case c if classOf[String].isAssignableFrom(c) =>
      (in: Input) => in.readString()

    case c if classOf[java.lang.Integer].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else in.readInt().asInstanceOf[AnyRef]

    case c if classOf[java.lang.Long].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else in.readLong().asInstanceOf[AnyRef]

    case c if classOf[java.lang.Double].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else in.readDouble().asInstanceOf[AnyRef]

    case c if classOf[java.lang.Float].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else in.readFloat().asInstanceOf[AnyRef]

    case c if classOf[java.lang.Boolean].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else in.readBoolean().asInstanceOf[AnyRef]

    case c if classOf[Date].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) null else new Date(in.readLong())

    case c if classOf[UUID].isAssignableFrom(c) =>
      (in: Input) => if (in.readByte() == NULL_BYTE) {
        null
      } else {
        val mostSignificantBits = in.readLong()
        val leastSignificantBits = in.readLong()
        new UUID(mostSignificantBits, leastSignificantBits)
      }

    case c if classOf[Geometry].isAssignableFrom(c) =>
      (in: Input) => {
        val length = in.readInt(true)
        if (length > 0) {
          val bytes = new Array[Byte](length)
          in.read(bytes)
          WKBUtils.read(bytes)
        } else {
          null
        }
      }

    case c if classOf[jList[_]].isAssignableFrom(c) =>
      val subtype = metadata.get("subtype").asInstanceOf[Class[_]]
      val subDecoding = matchDecode(subtype, null)

      (in: Input) => {
        val length = in.readInt()
        if (length < 0) {
          null
        } else {
          val list = new java.util.ArrayList[Object](length)
          (0 until length).foreach(_ => list.add(subDecoding(in)))
          list
        }
      }

    case c if classOf[jMap[_, _]].isAssignableFrom(c) =>
      val keyClass      = metadata.get("keyclass").asInstanceOf[Class[_]]
      val valueClass    = metadata.get("valueclass").asInstanceOf[Class[_]]
      val keyDecoding   = matchDecode(keyClass, null)
      val valueDecoding = matchDecode(valueClass, null)

      (in: Input) => {
        val length = in.readInt()
        if (length < 0) {
          null
        } else {
          val map = new java.util.HashMap[Object, Object](length)
          (0 until length).foreach(_ => map.put(keyDecoding(in), valueDecoding(in)))
          map
        }
      }
  }
}