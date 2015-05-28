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

import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationException

/** Writes a [[Hints.Key]]. */
trait HintKeyWriter[Writer] extends PrimitiveWriter[Writer] {

  import HintKeySerialization.keyToId

  /**
   * A [[DatumWriter]] for writing a non-nullable [[Hints.Key]].
   */
  val writeHintKey: DatumWriter[Writer, Hints.Key] = (writer, key) => {
    // exception should not be thrown - AbstractWriter.writeGenericMap will short circuit
    val id = keyToId.getOrElse(key, throw new SerializationException(s"Unknown Key: '$key'"))
    writeString(writer, id)
  }
}

/** Reads a [[Hints.Key]] */
trait HintKeyReader[Reader] extends PrimitiveReader[Reader] {

  import HintKeySerialization.idToKey

  val readHintKey: DatumReader[Reader, Hints.Key] = (reader) => {
    val id = readString(reader)

    // exception should not be thrown - if we wrote it, we should be able to read it!
    idToKey.getOrElse(id, throw new SerializationException(s"Unknown Key ID: '$id'"))
  }
}

/** Maintains Key -> String and String -> Key mappings */
object HintKeySerialization {

  // Add more keys as needed.
  val keyToId: Map[Hints.Key, String] = Map(
    Hints.PROVIDED_FID -> "PROVIDED_FID",
    Hints.USE_PROVIDED_FID -> "USE_PROVIDED_FID"
  )

  val idToKey: Map[String, Hints.Key] = keyToId.map(_.swap)

  def canSerialize(key: Hints.Key): Boolean = keyToId.contains(key)
}