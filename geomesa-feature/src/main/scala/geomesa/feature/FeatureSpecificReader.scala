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

package geomesa.feature

import java.io.InputStream
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import geomesa.feature.serde.{ASFDeserializer, Version1Deserializer, Version2Deserializer}
import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class FeatureSpecificReader(oldType: SimpleFeatureType, newType: SimpleFeatureType)
  extends DatumReader[AvroSimpleFeature] {

  def this(sft: SimpleFeatureType) = this(sft, sft)

  import geomesa.feature.AvroSimpleFeature._

import scala.collection.JavaConversions._

  var oldSchema = AvroSimpleFeature.generateSchema(oldType)
  val newSchema = AvroSimpleFeature.generateSchema(newType)
  val fieldsDesired = DataUtilities.attributeNames(newType).map(encodeAttributeName)

  def isDataField(f: Schema.Field) =
    !f.name.equals(FEATURE_ID_AVRO_FIELD_NAME) && !f.name.equals(AVRO_SIMPLE_FEATURE_VERSION)

  val dataFields = oldSchema.getFields.filter { isDataField }

  val typeMap: Map[String, Class[_]] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.getType.getBinding }.toMap

  val nillableAttrs: Set[String] = oldType.getAttributeDescriptors.filter(_.isNillable).map {
      ad => encodeAttributeName(ad.getLocalName)
    }.toSet

  def setSchema(schema:Schema) = oldSchema = schema

  def buildFieldReaders(deserializer: ASFDeserializer) =
    oldType.getAttributeDescriptors.map { ad =>
      val name = encodeAttributeName(ad.getLocalName)
      buildSetOrConsume(name, typeMap(name), deserializer) }

  def buildSetOrConsume(name: String, cls: Class[_], deserializer: ASFDeserializer) = {
    val f =
      if (!fieldsDesired.contains(name)) buildConsume(cls, name, deserializer)
      else buildSet(cls, name, deserializer)

    if (nillableAttrs.contains(name))
      (sf: AvroSimpleFeature, in: Decoder) =>
        if (in.readIndex() == 1) in.readNull()
        else f(sf, in)
    else
      f
  }
          
  def buildSet(clazz: Class[_], name: String, deserializer: ASFDeserializer): (AvroSimpleFeature, Decoder) => Unit = {
    val decoded = decodeAttributeName(name)
    clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls)  => deserializer.setString(_, decoded, _)
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls) => deserializer.setInt(_, decoded, _)
      case cls if classOf[java.lang.Long].isAssignableFrom(cls)    => deserializer.setLong(_, decoded, _)
      case cls if classOf[java.lang.Double].isAssignableFrom(cls)  => deserializer.setDouble(_, decoded, _)
      case cls if classOf[java.lang.Float].isAssignableFrom(cls)   => deserializer.setFloat(_, decoded, _)
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls) => deserializer.setBool(_, decoded, _)
      case cls if classOf[UUID].isAssignableFrom(cls)              => deserializer.setUUID(_, decoded, _)
      case cls if classOf[Date].isAssignableFrom(cls)              => deserializer.setDate(_, decoded, _)
      case cls if classOf[Geometry].isAssignableFrom(cls)          => deserializer.setGeometry(_, decoded, _)
    }
  }

  def buildConsume(clazz: Class[_], name: String, deserializer: ASFDeserializer) = {
    val f = deserializer.buildConsumeFunction(clazz)
    (sf: SimpleFeature, in: Decoder) => f(in)
  }

  lazy val v1fieldreaders = buildFieldReaders(Version1Deserializer)
  lazy val v2fieldreaders = buildFieldReaders(Version2Deserializer)

  def read(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    // Read the version first and choose the proper deserializer
    val serializationVersion = in.readInt()
    val deserializer = serializationVersion match {
      case 1 => v1fieldreaders
      case 2 => v2fieldreaders
    }

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id, newType)
    deserializer.foreach { f => f(sf, in) }
    sf
  }

}

object FeatureSpecificReader {

  // use when you want the entire feature back, not a subset
  def apply(sftType: SimpleFeatureType) = new FeatureSpecificReader(sftType, sftType)

  // first field is serialization version, 2nd field is ID of simple feature
  def extractId(is: InputStream): String = {
    val decoder = DecoderFactory.get().binaryDecoder(is, null)
    decoder.readInt()
    decoder.readString()
  }

}
