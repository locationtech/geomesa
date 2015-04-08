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

import org.geotools.factory.Hints

/** Combines all readers.
  *
  */
trait AbstractReader
  extends PrimitiveReader
  with NullableReader
  with HintKeyReader {

  import AbstractWriter.NULL_MARKER_STR


  /** A [[DatumReader]] which reads the a class name and then an object of that class.  If the class name is a null marker
    * then ``null`` will be returned.
    */
  def readGeneric: DatumReader[_] = () => {
    val className = readString()

    if (className == NULL_MARKER_STR) {
      null
    } else {
      val clazz = Class.forName(className)
      selectReader(clazz).apply()
    }
  }

  /**
   * A [[DatumReader]] which reads the start of an array and returns the number of elements in the array.
   */
  def readArrayStart: DatumReader[Long]

  /**
   * A [[DatumReader]] for reading a map where the key and values may be any type.  The map may not be null. The reader
   * will call ``readArrayStart(reader)`` and then, for each entry, read up to four items.
   */
  def readGenericMap: DatumReader[java.util.Map[AnyRef, AnyRef]] = () => {
    val map = new java.util.HashMap[AnyRef, AnyRef]
    var toRead = readArrayStart()

    while (toRead > 0) {
      val key = readGeneric()
      val value = readGeneric()

      // force boxing by calling 'asInstanceOf[AnyRef]
      map.put(key.asInstanceOf[AnyRef], value.asInstanceOf[AnyRef])

      toRead -= 1
    }

    map
  }

  /**
   * @param clazz the [[Class]] of the object to be read
   * @tparam T the type of the object to be read
   * @return a [[DatumReader]] capable of reading object of the given ``clazz``
   */
  def selectReader[T](clazz: Class[_ <: T]): DatumReader[T] = {
    clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls)    => readString.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls)   => readInt.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.lang.Long].isAssignableFrom(cls)      => readLong.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.lang.Float].isAssignableFrom(cls)     => readFloat.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.lang.Double].isAssignableFrom(cls)    => readDouble.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls)   => readBoolean.asInstanceOf[DatumReader[T]]
      case cls if classOf[java.util.Date].isAssignableFrom(cls)      => readDate.asInstanceOf[DatumReader[T]]

      case cls if classOf[Hints.Key].isAssignableFrom(cls)           => readHintKey.asInstanceOf[DatumReader[T]]

      //      case cls if classOf[UUID].isAssignableFrom(cls)                => ???
      //      case cls if classOf[Date].isAssignableFrom(cls)                => ???
      //      case cls if classOf[Geometry].isAssignableFrom(cls)            => ???
      //      case cls if classOf[java.util.List[_]].isAssignableFrom(cls)   => ???
      //      case cls if classOf[java.util.Map[_, _]].isAssignableFrom(cls) => ???

      case _ => throw new IllegalArgumentException("Unsupported class: " + clazz)
    }
  }

}
