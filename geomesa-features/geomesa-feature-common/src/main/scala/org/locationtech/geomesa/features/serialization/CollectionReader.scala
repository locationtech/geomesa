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

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

/** [[DatumReader]]s for reading [[java.util.Map]]s and [[java.util.List]]s.
 */
trait CollectionReader[Reader] extends PrimitiveReader[Reader] {

  /**
   * @param elementReader will be delegated to for reading the list elements
   * @tparam E the type of the list elements
   * @return a [[DatumReader]] for reading a [[java.util.List]] which may be null
   */
  def readList[E](elementReader: DatumReader[Reader, E]): DatumReader[Reader, JList[E]] = (in) => {
    val length = readInt(in)
    if (length < 0) {
      null
    } else {
      val list = new JArrayList[E](length)
      var i = 0
      while (i < length) {
        list.add(elementReader(in))
        i += 1
      }
      list
    }
  }

  /**
   * @param keyReader will be delegated to for reading the map keys
   * @param valueReader will be delegated to for reading the map values
   * @tparam K the type of the map keys
   * @tparam V the type of the map values
   * @return a [[DatumReader]] for reading a [[java.util.Map]]which may be null
   */
  def readMap[K, V](keyReader: DatumReader[Reader, K], valueReader: DatumReader[Reader, V]): DatumReader[Reader, JMap[K, V]] = (in) => {
    val length = readInt(in)
    if (length < 0) {
      null
    } else {
      val map = new JHashMap[K, V](length)
      var i = 0
      while (i < length) {
        map.put(keyReader(in), valueReader(in))
        i += 1
      }
      map
    }
  }

  /** Reads the start of an array.
    *
    * @return the number of elements in the array.
    */
  val readArrayStart: (Reader) => Int
}
