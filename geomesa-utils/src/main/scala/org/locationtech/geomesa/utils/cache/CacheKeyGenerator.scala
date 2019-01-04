/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

object CacheKeyGenerator {

  /**
    * Encodes the simple feature type fo use as a cache key. Note that this encodes all user data, not just
    * geomesa-prefixed ones
    *
    * @param sft simple feature type
    * @return
    */
  def cacheKey(sft: SimpleFeatureType): String =
    s"${sft.getName};${SimpleFeatureTypes.encodeType(sft)}${SimpleFeatureTypes.encodeUserData(sft.getUserData)}"

  /**
    * Restores a simple feature type from a cache key
    *
    * @param key cache key
    * @return
    */
  def restore(key: String): SimpleFeatureType = {
    val i = key.indexOf(';')
    SimpleFeatureTypes.createImmutableType(key.substring(0, i), key.substring(i + 1))
  }
}
