/************************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

object IteratorCache {

  private val sftCache = new SoftThreadLocalCache[(String, String), SimpleFeatureType]()
  private val kryoBufferCache = new SoftThreadLocalCache[(SimpleFeatureType, SerializationOptions), KryoBufferSimpleFeature]()

  def sft(name: String, spec: String): SimpleFeatureType =
    sftCache.getOrElseUpdate((name, spec), SimpleFeatureTypes.createType(name, spec))

  def kryoBufferFeature(sft: SimpleFeatureType, options: Set[SerializationOption]) =
    kryoBufferCache.getOrElseUpdate((sft, options), new KryoFeatureSerializer(sft, options).getReusableFeature)
}
