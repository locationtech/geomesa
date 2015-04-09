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

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.factory.Hints

/** [[DatumWriter]] definitions for writing (serializing) components of a [[org.opengis.feature.simple.SimpleFeature]].
  *
  */
trait AbstractWriter
  extends PrimitiveWriter
  with NullableWriter
  with HintKeyWriter
  with Logging {

  import AbstractWriter.NULL_MARKER_STR

  def writeUUID: DatumWriter[UUID] = (uuid) => {
    writeLong(uuid.getMostSignificantBits)
    writeLong(uuid.getLeastSignificantBits)
  }

  /** A [[DatumWriter]] which writes the class name of ``obj`` and then the ``obj``.  If the object is ``null`` then only
    * ``<null>`` will be written
    *
    * @tparam T thpe of t
    */
  def writeGeneric[T]: DatumWriter[T] = (obj) => {
    if (obj == null) {
      writeString(NULL_MARKER_STR)
    } else {
      writeString(obj.getClass.getName)
      selectWriter(obj.getClass.asInstanceOf[Class[T]])(obj)
    }
  }

  /**
   * A [[DatumWriter]] which writes the start of an array.  The value is the length of the array
   */
  def writeArrayStart: DatumWriter[Int]

  /** Call to indicate the start of an item in an array or map. */
  def startItem(): Unit

  /** Call to indicate the end of an array. */
  def endArray(): Unit

  /**
   * A [[DatumWriter]] for writing a map where the key and values may be any type.  The map may not be null. The writer
   * will call ``writeArrayStart(writer, map.size)`` and then, for each entry, call ``startItem`` followed by four
   * writes.  After writing all entries the reader will call ``endArray``.
   */
  def writeGenericMap: DatumWriter[java.util.Map[AnyRef, AnyRef]] = (map) => {

    // may not be able to write all entries - must pre-filter to know correct count
    import collection.JavaConverters.mapAsScalaMapConverter
    val filtered = map.asScala.filter {
      case (key, value) =>
        if (canSerialize(key)) {
          true
        } else {
          logger.warn(s"Can't serialize Map entry ($key,$value).  The map entry will be skipped.")
          false
        }
    }

    writeArrayStart(filtered.size)

    filtered.foreach {
      case (key, value) =>
        startItem()
        writeGeneric(key)
        writeGeneric(value)
    }

    endArray()
  }

  def canSerialize(obj: AnyRef): Boolean = obj match {
    case key: Hints.Key => HintKeySerialization.canSerialize(key)
    case _ => true
  }

  /**
   * @param clazz the [[Class]] of the object to be written
   * @tparam T the type of the object to be written
   * @return a [[DatumWriter]] capable of writing object of the given ``clazz``
   */
  def selectWriter[T](clazz: Class[_ <: T]): DatumWriter[T] = {
    clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls)    => writeString.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls)   => writeInt.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.lang.Long].isAssignableFrom(cls)      => writeLong.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.lang.Float].isAssignableFrom(cls)     => writeFloat.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.lang.Double].isAssignableFrom(cls)    => writeDouble.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls)   => writeBoolean.asInstanceOf[DatumWriter[T]]
      case cls if classOf[java.util.Date].isAssignableFrom(cls)      => writeDate.asInstanceOf[DatumWriter[T]]

      case cls if classOf[Hints.Key].isAssignableFrom(cls)           => writeHintKey.asInstanceOf[DatumWriter[T]]

      //      case cls if classOf[UUID].isAssignableFrom(cls)                => ???
      //      case cls if classOf[Geometry].isAssignableFrom(cls)            => ???
      //      case cls if classOf[java.util.List[_]].isAssignableFrom(cls)   => ???
      //      case cls if classOf[java.util.Map[_, _]].isAssignableFrom(cls) => ???

      case _ => throw new IllegalArgumentException("Unsupported class: " + clazz)
    }
  }
}

object AbstractWriter {
  val NULL_MARKER_STR = "<null>"
}
