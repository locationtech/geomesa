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

package org.locationtech.geomesa.feature.serialization

import java.util.{ArrayList => JArrayList, Collections => JCollections, HashMap => JHashMap, List => JList, Map => JMap, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.feature.serialization.AbstractWriter.NULL_MARKER_STR
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

/** Combines all readers.
  *
  */
trait AbstractReader[Reader]
  extends PrimitiveReader[Reader]
  with NullableReader[Reader]
  with GeometryReader[Reader]
  with HintKeyReader[Reader] {


  def readUUID: DatumReader[Reader, UUID] = (in: Reader, version: Int) =>  {
    val mostSignificantBits = readLong(in, version)
    val leastSignificantBits = readLong(in, version)
    new UUID(mostSignificantBits, leastSignificantBits)
  }

  def readList[E](elementReader: DatumReader[Reader, E]): DatumReader[Reader, JList[E]] = (in, version) => {
    val length = readInt(in, version)
    if (length < 0) {
      null
    } else {
      val list = new java.util.ArrayList[E](length)
      var i = 0
      while (i < length) {
        list.add(elementReader(in, version))
        i += 1
      }
      list
    }
  }

  /**
   * @param keyReader for reading the keys
   * @param valueReader for reading the values
   * @tparam K the type of the keys
   * @tparam V the type of the values
   * @return a [[DatumReader]] for reading a [[Map[K,V]] which may be null
   */
  def readMap[K, V](keyReader: DatumReader[Reader, K], valueReader: DatumReader[Reader, V]): DatumReader[Reader, JMap[K, V]] = (in, version) => {
    val length = readInt(in, version)
    if (length < 0) {
      null
    } else {
      val map = new JHashMap[K, V](length)
      var i = 0
      while (i < length) {
        map.put(keyReader(in, version), valueReader(in, version))
        i += 1
      }
      map
    }
  }

  /** A [[DatumReader]] which reads the a class name and then an object of that class.  If the class name is a null marker
    * then ``null`` will be returned.
    */
  def readGeneric: DatumReader[Reader, AnyRef] = (reader, version) => {
    val className = readString(reader, version)

    if (className == NULL_MARKER_STR) {
      null
    } else {
      val clazz = Class.forName(className)
      selectReader(clazz).apply(reader, version)
    }
  }

  /**
   * A [[DatumReader]] which reads the start of an array and returns the number of elements in the array.
   */
  def readArrayStart: DatumReader[Reader, Int]

  /**
   * A [[DatumReader]] for reading a map where the key and values may be any type.  The map may not be null. The reader
   * will call ``readArrayStart(reader)`` and then, for each entry, read up to four items.
   */
  def readGenericMap: DatumReader[Reader, JMap[AnyRef, AnyRef]] = (reader, version) => {
    var toRead = readArrayStart(reader, version)
    val map = new java.util.HashMap[AnyRef, AnyRef](toRead)

    while (toRead > 0) {
      val key = readGeneric(reader, version)
      val value = readGeneric(reader, version)

      // force boxing by calling 'asInstanceOf[AnyRef]
      map.put(key.asInstanceOf[AnyRef], value.asInstanceOf[AnyRef])

      toRead -= 1
    }

    map
  }

  /**
   * @param clazz the [[Class]] of the object to be read
   * @return a [[DatumReader]] capable of reading object of the given ``clazz``
   */
  def selectReader(clazz: Class[_], metadata: JMap[AnyRef, AnyRef] = JCollections.emptyMap(),
                   isNullable: isNullableFn = notNullable): DatumReader[Reader, AnyRef] = {

    val reader: DatumReader[Reader, AnyRef] = clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls) => readString
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls) => readInt.asInstanceOf[DatumReader[Reader, AnyRef]]
      case cls if classOf[java.lang.Long].isAssignableFrom(cls) => readLong.asInstanceOf[DatumReader[Reader, AnyRef]]
      case cls if classOf[java.lang.Float].isAssignableFrom(cls) => readFloat.asInstanceOf[DatumReader[Reader, AnyRef]]
      case cls if classOf[java.lang.Double].isAssignableFrom(cls) => readDouble.asInstanceOf[DatumReader[Reader, AnyRef]]
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls) => readBoolean.asInstanceOf[DatumReader[Reader, AnyRef]]
      case cls if classOf[java.util.Date].isAssignableFrom(cls) => readDate


      case cls if classOf[UUID].isAssignableFrom(cls) => readUUID

      case cls if classOf[Geometry].isAssignableFrom(cls) => readGeometry

      case cls if classOf[Hints.Key].isAssignableFrom(cls) => readHintKey.asInstanceOf[DatumReader[Reader, AnyRef]]

      case cls if classOf[java.util.List[_]].isAssignableFrom(cls) =>
        val elemType = metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]
        val elemReader = selectReader(elemType, isNullable = isNullable)
        readList(elemReader)

      case cls if classOf[java.util.Map[_, _]].isAssignableFrom(cls) =>
        val keyClass      = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
        val valueClass    = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
        val keyDecoding   = selectReader(keyClass, isNullable = isNullable)
        val valueDecoding = selectReader(valueClass, isNullable = isNullable)
        readMap(keyDecoding, valueDecoding)

      case _ => throw new IllegalArgumentException("Unsupported class: " + clazz)
    }

    if (isNullable(clazz)) {
      readNullable(reader)
    } else {
      reader
    }
  }
}
