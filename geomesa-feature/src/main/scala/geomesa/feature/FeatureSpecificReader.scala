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

import geomesa.feature.serde.{ASFDeserializer, Version1Deserializer, Version2Deserializer}
import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.immutable.HashSet


class FeatureSpecificReader(oldType: SimpleFeatureType, newType: SimpleFeatureType)
  extends DatumReader[AvroSimpleFeature] {

  import geomesa.feature.AvroSimpleFeature._

import scala.collection.JavaConversions._

  var oldSchema = AvroSimpleFeature.generateSchema(oldType)
  val newSchema = AvroSimpleFeature.generateSchema(newType)
  val fieldsDesired = new HashSet() ++ DataUtilities.attributeNames(newType)

  def isDataField(f: Schema.Field) =
    !f.name.equals(FEATURE_ID_AVRO_FIELD_NAME) && !f.name.equals(AVRO_SIMPLE_FEATURE_VERSION)

  val dataFields = oldSchema.getFields.filter { isDataField }

  val typeMap: Map[String, Class[_]] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.getType.getBinding }.toMap

  val nullMap: Map[String, Boolean] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.isNillable }.toMap

  def setSchema(schema:Schema) = oldSchema = schema

  def read(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    // Read the version first and choose the proper deserializer
    val serializationVersion = in.readInt()
    val deserializer = serializationVersion match {
      case 1 => Version1Deserializer
      case 2 => Version2Deserializer
    }

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id, newType)
    if(dataFields.size != fieldsDesired.size)
      dataFields.foreach { f =>
        if(checkNull(f.name, in)) in.readNull()
        else setOrConsume(sf, decodeAttributeName(f.name), in, typeMap.get(f.name).get, deserializer)
      }
    else
      dataFields.foreach { f =>
        if(checkNull(f.name, in)) in.readNull()
        else deserializer.set(sf, decodeAttributeName(f.name), in, typeMap.get(f.name).get)
      }
    sf
  }

  // null at index 1 according to schema builder...beware 1.7.5 is avro version...might change with later versions
  // look at the json schema to verify the position that the null is in
  protected def checkNull(field:String, in:Decoder) = nullMap.get(field).get && in.readIndex() == 1

  protected def setOrConsume(sf: AvroSimpleFeature,
                             field: String,
                             in:Decoder,
                             cls: Class[_],
                             deserializer: ASFDeserializer) =
    if (fieldsDesired.contains(field)) {
      deserializer.set(sf,field, in, cls)
    } else {
      deserializer.consume(cls, in)
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
