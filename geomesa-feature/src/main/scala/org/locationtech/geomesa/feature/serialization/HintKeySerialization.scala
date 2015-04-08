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

import java.awt.RenderingHints

import org.geotools.factory.Hints
import org.locationtech.geomesa.feature.SerializationException

/** Writes a [[Hints.Key]].
  *
  * Created by mmatz on 4/7/15.
  */
trait HintKeyWriter extends PrimitiveWriter {

  /**
   * A [[DatumWriter]] for writing a non-nullable [[Hints.Key]].
   */
  val writeHintKey: DatumWriter[Hints.Key] = (key) => writeString(HintKeySerialization.getIdentity(key))
}

/** Reads a [[Hints.Key]]
  *
  * Created by mmatz on 4/7/15.
  */
trait HintKeyReader extends PrimitiveReader {

  val readHintKey: DatumReader[Hints.Key] = () => HintKeySerialization.getKey(readString())
}

/** Reflection support for [[HintKeyWriter]] and [[HintKeyReader]].
  *
  * This is fragile.  Is there a better way?
  */
object HintKeySerialization {

  // avoid creating a copy of RenderingHints.Key.identityMap - just create an accessible reference
  lazy val keyIdentityMap: java.util.Map[Object, Object] = {

    try {
      val keyClass = classOf[RenderingHints.Key]
      val field = keyClass.getDeclaredField("identitymap")
      field.setAccessible(true)
      field.get(null).asInstanceOf[java.util.Map[Object,Object]]
    } catch {
      case e: Exception =>
        throw new SerializationException("Error accessing identityMap", e)
    }
  }

  def getKey(identity: String): Hints.Key = {
    Option(keyIdentityMap.get(identity))
      .collect{case ref: java.lang.ref.WeakReference[_] => ref}
      .flatMap((ref) => Option(ref.get()))
      .collect{case key: Hints.Key => key}
      .getOrElse(throw  new SerializationException(s"No Key found for identity $identity"))
  }

  def getIdentity(key: Hints.Key): String = {
    try {
      val keyClass = classOf[RenderingHints.Key]
      val method = keyClass.getDeclaredMethod("getIdentity")
      method.setAccessible(true)
      method.invoke(key).asInstanceOf[String]
    } catch {
      case e: Exception =>
        throw new SerializationException("Error accessing getIdentity() method", e)
    }
  }
}