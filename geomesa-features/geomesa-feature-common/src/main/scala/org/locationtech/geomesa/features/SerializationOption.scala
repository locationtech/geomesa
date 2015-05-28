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

package org.locationtech.geomesa.features

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object SerializationOption extends Enumeration {
  type SerializationOption = Value

  /**
   * If this [[SerializationOption]] is specified then all user data of the simple feature will be
   * serialized and deserialized.
   */
  val WithUserData = Value


  implicit class SerializationOptions(val options: Set[SerializationOption]) extends AnyVal {

    /**
     * @param value the value to search for
     * @return true iff ``this`` contains the given ``value``
     */
    def contains(value: SerializationOption.Value) = options.contains(value)

    /** @return true iff ``this`` contains ``EncodingOption.WITH_USER_DATA`` */
    def withUserData: Boolean = options.contains(SerializationOption.WithUserData)
  }

  object SerializationOptions {

    /**
     * An empty set of encoding options.
     */
    val none: SerializationOptions = Set.empty[SerializationOption]

    /**
     * @return a new [[SerializationOptions]] containing just the ``EncodingOption.WITH_USER_DATA`` option
     */
    def withUserData: SerializationOptions = Set(SerializationOption.WithUserData)
  }
}

