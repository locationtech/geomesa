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
package org.locationtech.geomesa.utils.geotools

import org.geotools.data.Query
import org.geotools.data.store.{ContentFeatureSource, ContentFeatureStore}
import org.locationtech.geomesa.utils.geotools.FR

/** Parent for any trait adding support to a [[ContentFeatureSource]] such as 'retype', 'sort', 'offset',
  * or 'limit'.
  *
  *
  * Any [[ContentFeatureSource]] with a [[ContentFeatureSourceSupport]] trait should call
  * ``addSupport(query, reader)`` in their ``getReaderInternal`` implementation.
  *
  *
  * Sub-traits of [[ContentFeatureSource]] should first delegate to ``super.addSupport(query, reader)`` in
  * their ``addSupport`` implementation so that support can be chained and applied left to right.
  */
trait ContentFeatureSourceSupport {

  /** Adds support to given ``reader``, based on the ``query`` and returns a wrapped [[FR]].  Sub-traits
    * must override.
    */
  def addSupport(query: Query, reader: FR): FR = reader
}

/** Adds support for retyping to a [[ContentFeatureStore]].  This is necessary because the re-typing
  * functionality in [[ContentFeatureStore]] does not preserve user data.
  */
trait ContentFeatureSourceReTypingSupport extends ContentFeatureSourceSupport {
  self: ContentFeatureSource =>

  override val canRetype: Boolean = true

  override def addSupport(query: Query, reader: FR): FR = {
    var result = super.addSupport(query, reader)
    
    // logic copied from ContentFeatureSource.getReader() but uses TypeUpdatingFeatureReader instead
    if (query.getPropertyNames != Query.ALL_NAMES) {
      val target = FeatureUtils.retype(getSchema, query.getPropertyNames)
      if (!(target == reader.getFeatureType)) {
        result = new TypeUpdatingFeatureReader(result, target)
      }
    }

    result
  }
}
