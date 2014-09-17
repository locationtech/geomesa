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
import java.util.{Date, UUID}

import com.google.common.collect.Maps
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBWriter
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.codec.binary.Hex
import org.geotools.util.Converters
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
      case c if classOf[String].isAssignableFrom(c)             => baseType.stringType.noDefault
      case c if classOf[java.lang.Integer].isAssignableFrom(c)  => baseType.intType.noDefault
      case c if classOf[java.lang.Long].isAssignableFrom(c)     => baseType.longType.noDefault
      case c if classOf[java.lang.Double].isAssignableFrom(c)   => baseType.doubleType.noDefault
      case c if classOf[java.lang.Float].isAssignableFrom(c)    => baseType.floatType.noDefault
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)  => baseType.booleanType.noDefault
      case c if classOf[UUID].isAssignableFrom(c)               => baseType.bytesType.noDefault
      case c if classOf[Date].isAssignableFrom(c)               => baseType.longType.noDefault
      case c if classOf[Geometry].isAssignableFrom(c)           => baseType.bytesType.noDefault
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
  def createTypeMap(sft: SimpleFeatureType, wkbWriter: WKBWriter) = {
    sft.getAttributeDescriptors.map { ad =>
      val conv =
        ad.getType.getBinding match {
          case t if primitiveTypes.contains(t) => (v: AnyRef) => v
          case t if classOf[UUID].isAssignableFrom(t) =>
            (v: AnyRef) => {
              val uuid = v.asInstanceOf[UUID]
              val bb = ByteBuffer.allocate(16)
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.flip
              bb
            }

          case t if classOf[Date].isAssignableFrom(t) =>
            (v: AnyRef) => v.asInstanceOf[Date].getTime

          case t if classOf[Geometry].isAssignableFrom(t) =>
            (v: AnyRef) => ByteBuffer.wrap(wkbWriter.write(v.asInstanceOf[Geometry]))

          case _ =>
            (v: AnyRef) =>
              Option(Converters.convert(v, classOf[String])).getOrElse { a: AnyRef => a.toString }
        }

      (encodeAttributeName(ad.getLocalName), Binding(ad.getType.getBinding, conv))
    }.toMap
  }

}
