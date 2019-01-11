/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.data.Query
import org.geotools.data.store.{ContentFeatureSource, ContentFeatureStore}

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
