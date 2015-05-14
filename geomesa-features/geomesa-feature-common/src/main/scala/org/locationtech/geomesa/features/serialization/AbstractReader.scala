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

package org.locationtech.geomesa.features.serialization

import java.util.{Collections => JCollections, Map => JMap, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.serialization.AbstractWriter._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

/** Combines all readers.
  *
  */
trait AbstractReader[Reader]
  extends PrimitiveReader[Reader]
  with NullableReader[Reader]
  with CollectionReader[Reader]
  with GeometryReader[Reader]
  with HintKeyReader[Reader] {

  def readUUID: DatumReader[Reader, UUID] = (in: Reader) =>  {
    val mostSignificantBits = readLong(in)
    val leastSignificantBits = readLong(in)
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  /** A [[DatumReader]] which reads a class name and then an object of that class.  If the class name is a null marker
    * then ``null`` will be returned.
    */
  def readGeneric(version: Version): DatumReader[Reader, AnyRef] = (reader) => {
    val className = readString(reader)

    if (className == NULL_MARKER_STR) {
      null
    } else {
      val clazz = Class.forName(className)
      selectReader(clazz, version).apply(reader)
    }
  }

  /**
   * A [[DatumReader]] for reading a map where the key and values may be any type.  The map may not be null. The reader
   * will call ``readArrayStart(reader)`` and then, for each entry, read up to four items.
   */
  def readGenericMap(version: Version): DatumReader[Reader, JMap[AnyRef, AnyRef]] = (reader) => {
    var toRead = readArrayStart(reader)
    val map = new java.util.HashMap[AnyRef, AnyRef](toRead)

    while (toRead > 0) {
      val key = readGeneric(version).apply(reader)
      val value = readGeneric(version).apply(reader)

      map.put(key, value)
      toRead -= 1
    }

    map
  }

  /**
   * @param cls the [[Class]] of the object to be read
   * @return a [[DatumReader]] capable of reading object of the given ``clazz``
   */
  def selectReader(cls: Class[_], version: Version,
                   metadata: JMap[_ <: AnyRef, _ <: AnyRef] = JCollections.emptyMap(),
                   isNullable: isNullableFn = notNullable): DatumReader[Reader, AnyRef] = {

    val reader: DatumReader[Reader, AnyRef] = {
      if (classOf[java.lang.String].isAssignableFrom(cls)) readString
      else if (classOf[java.lang.Integer].isAssignableFrom(cls)) readInt
      else if (classOf[java.lang.Long].isAssignableFrom(cls)) readLong
      else if (classOf[java.lang.Float].isAssignableFrom(cls)) readFloat
      else if (classOf[java.lang.Double].isAssignableFrom(cls)) readDouble
      else if (classOf[java.lang.Boolean].isAssignableFrom(cls)) readBoolean
      else if (classOf[java.util.Date].isAssignableFrom(cls)) readDate

      else if (classOf[UUID].isAssignableFrom(cls)) readUUID
      else if (classOf[Geometry].isAssignableFrom(cls)) selectGeometryReader(version)
      else if (classOf[Hints.Key].isAssignableFrom(cls)) readHintKey

      else if (classOf[java.util.List[_]].isAssignableFrom(cls)) {
        val elemClass = metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]
        val elemReader = selectReader(elemClass, version, isNullable = isNullable)
        readList(elemReader)
      }

      else if (classOf[java.util.Map[_, _]].isAssignableFrom(cls)) {
        val keyClass = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
        val valueClass = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
        val keyDecoding = selectReader(keyClass, version, isNullable = isNullable)
        val valueDecoding = selectReader(valueClass, version, isNullable = isNullable)
        readMap(keyDecoding, valueDecoding)
      }

      else throw new IllegalArgumentException("Unsupported class: " + cls)
    }.asInstanceOf[DatumReader[Reader, AnyRef]]

    if (isNullable(cls)) {
      readNullable(reader)
    } else {
      reader
    }
  }
}
