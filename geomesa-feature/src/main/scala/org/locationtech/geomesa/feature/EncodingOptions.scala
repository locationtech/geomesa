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

package org.locationtech.geomesa.feature

import org.locationtech.geomesa.feature.EncodingOption.EncodingOption

/**
  * Created by mmatz on 4/6/15.
  */
object EncodingOptions {

  /**
   * An empty set of encoding options.
   */
  val none: EncodingOptions = Set[EncodingOption]()

  /**
   * @return a new [[EncodingOptions]] containing just the ``EncodingOption.WITH_USER_DATA`` option
   */
  def withUserData: EncodingOptions = Set(EncodingOption.WITH_USER_DATA)

  implicit class EncodingOptions(val options: Set[EncodingOption]) extends AnyVal {

    /**
     * @param value the value to search for
     * @return true iff ``this`` contains the given ``value``
     */
    def contains(value: EncodingOption.Value) = options.contains(value)

    /** @return true iff ``this`` contains ``EncodingOption.WITH_USER_DATA``
     */
    def withUserData: Boolean = options.contains(EncodingOption.WITH_USER_DATA)
  }
}

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object EncodingOption extends Enumeration {
  type EncodingOption = Value

  /**
   * If this [[EncodingOption]] is specified then all user data of the simple feature will be
   * serialized and deserialized.
   */
  val WITH_USER_DATA = Value("withUserData")

  /**
   * If this [[EncodingOption]] is specified then the security marking associated with the simple feature will be
   * serialized and deserialized.
   */
  @deprecated
  val WITH_VISIBILITIES = Value("withVisibilities")
}

