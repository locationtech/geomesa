/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.features.serialization

import java.util.{List => JList, Map => JMap}

/** [[DatumReader]]s for reading [[java.util.Map]]s and [[java.util.List]]s.
 *
 */
trait CollectionWriter[Writer] extends PrimitiveWriter[Writer] {

  /**
   * @param elementWriter will be delegated to for writing the list elements
   * @tparam E the type of the list elements
   * @return a [[DatumWriter]] for writing a [[java.util.List]] which may be null
   */
  def writeList[E](elementWriter: DatumWriter[Writer, E]): DatumWriter[Writer, JList[E]] = (writer, list) => {
    if (list == null) {
      writeInt(writer, -1)
    } else {
      writeInt(writer, list.size())

      // don't convert to scala
      val iter = list.iterator()
      while (iter.hasNext) {
        elementWriter(writer, iter.next())
      }
    }
  }

  /**
   * @param keyWriter will be delegated to for writing the map keys
   * @param valueWriter will be delegated to for writing the map values
   * @tparam K the type of the map keys
   * @tparam V the type of the map values
   * @return a [[DatumWriter]] for writing a [[java.util.Map]] which may be null
   */
  def writeMap[K, V](keyWriter: DatumWriter[Writer, K], valueWriter: DatumWriter[Writer, V]): DatumWriter[Writer, JMap[K, V]] = (writer, map) => {
    if (map == null) {
      writeInt(writer, -1)
    } else {
      writeInt(writer, map.size())

      // don't convert to scala
      val iter = map.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        keyWriter(writer, entry.getKey)
        valueWriter(writer, entry.getValue)
      }
    }
  }

  /** Writes the start of an array including the ``length``. */
  def writeArrayStart: (Writer, Int) => Unit

  /** Indicates the start of an item in an array or map. */
  def startItem: (Writer) => Unit

  /** Indicates the end of an array. */
  def endArray: (Writer) => Unit
}
