/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import org.opengis.feature.simple.SimpleFeatureType

object CacheKeyGenerator {

  import collection.JavaConversions._

  def cacheKey(sft: SimpleFeatureType): String =
    s"${sft.getName};${sft.getAttributeDescriptors.map(ad => s"${ad.getLocalName}:${ad.getType.getBinding.getSimpleName}").mkString(",")}"
}
