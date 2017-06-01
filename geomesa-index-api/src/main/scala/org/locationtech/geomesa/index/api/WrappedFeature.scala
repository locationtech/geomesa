/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets

import org.opengis.feature.simple.SimpleFeature

import scala.util.hashing.MurmurHash3

/**
  * Wraps a simple feature for writing. Usually contains cached values that will be written to multiple indices,
  * to e.g. avoid re-serializing a simple feature multiple times
  */
trait WrappedFeature {

  /**
    * Underlying simple feature
    *
    * @return
    */
  def feature: SimpleFeature

  /**
    * Hash of the simple feature ID - can be used for sharding
    */
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))

  /**
    * Feature ID bytes
    */
  lazy val idBytes: Array[Byte] = feature.getID.getBytes(StandardCharsets.UTF_8)
}
