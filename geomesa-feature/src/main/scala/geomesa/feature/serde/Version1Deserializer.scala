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

package geomesa.feature.serde

import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import geomesa.feature.AvroSimpleFeature
import geomesa.utils.text.WKTUtils
import org.apache.avro.io.Decoder

/**
 * Original AvroSimpleFeature - Geometry is a String using WKT
 *
 * Implemented as an object to be a singleton with static method calls
 * so that we don't create one on each instantiation of a FeatureSpecificReader
 */
object Version1Deserializer extends ASFDeserializer {

  override def setValue(sf: AvroSimpleFeature, field: String, in: Decoder, cls: Class[_]): Unit =
    setValue(sf, field, in, cls, (in: Decoder) => WKTUtils.read(in.readString()))

  override def consumeValue(cls: Class[_], in: Decoder) = consumeValue(cls, in, (in: Decoder) => in.skipString())

  def setValue(sf: AvroSimpleFeature, field: String, in: Decoder, cls: Class[_], geomDecoder: Decoder => Geometry) = {
    val obj = cls match {
      case c if classOf[String].isAssignableFrom(cls)            => in.readString()
      case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt().asInstanceOf[Object]
      case c if classOf[java.lang.Long].isAssignableFrom(cls)    => in.readLong().asInstanceOf[Object]
      case c if classOf[java.lang.Double].isAssignableFrom(cls)  => in.readDouble().asInstanceOf[Object]
      case c if classOf[java.lang.Float].isAssignableFrom(cls)   => in.readFloat().asInstanceOf[Object]
      case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean().asInstanceOf[Object]
      case c if classOf[Date].isAssignableFrom(cls)              => new Date(in.readLong())
      case c if classOf[Geometry].isAssignableFrom(cls)          => geomDecoder(in)

      case c if classOf[UUID].isAssignableFrom(cls) =>
        val bb = in.readBytes(null)
        new UUID(bb.getLong, bb.getLong)
    }
    sf.setAttributeNoConvert(field, obj)
  }

  def consumeValue(cls: Class[_], in: Decoder, skipGeometry: Decoder => Unit) = cls match {
    case c if classOf[java.lang.String].isAssignableFrom(cls)  => in.skipString()
    case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt()
    case c if classOf[java.lang.Long].isAssignableFrom(cls)    => in.readLong()
    case c if classOf[java.lang.Double].isAssignableFrom(cls)  => in.readDouble()
    case c if classOf[java.lang.Float].isAssignableFrom(cls)   => in.readFloat()
    case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean()
    case c if classOf[UUID].isAssignableFrom(cls)              => in.skipBytes()
    case c if classOf[Date].isAssignableFrom(cls)              => in.readLong()
    case c if classOf[Geometry].isAssignableFrom(cls)          => skipGeometry(in)
  }
}
