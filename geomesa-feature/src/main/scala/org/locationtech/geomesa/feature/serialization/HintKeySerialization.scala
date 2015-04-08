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
import org.locationtech.geomesa.feature.SerializationException

/** Writes a [[Hints.Key]]. */
trait HintKeyWriter extends PrimitiveWriter {

  /**
   * A [[DatumWriter]] for writing a non-nullable [[Hints.Key]].
   */
  val writeHintKey: DatumWriter[Hints.Key] = (key) => writeString(HintKeySerialization.getId(key))
}

/** Reads a [[Hints.Key]] */
trait HintKeyReader extends PrimitiveReader {

  val readHintKey: DatumReader[Hints.Key] = () => HintKeySerialization.getKey(readString())
}

/** Maintains Key -> String and String -> Key mappings */
object HintKeySerialization {

  // Add more keys as needed.
  val idToKey: Map[String, Hints.Key] = Map(
    "USE_PROVIDED_FID" -> Hints.USE_PROVIDED_FID
  )

  val keyToId: Map[Hints.Key, String] = idToKey.map(_.swap)

  def getKey(identity: String): Hints.Key = {
    idToKey.getOrElse(identity, throw new SerializationException(s"Unknown Key ID: '$identity'"))
  }

  def getId(key: Hints.Key): String = {
    keyToId.getOrElse(key, throw new SerializationException(s"Unknown Key: '$key'"))
  }
}