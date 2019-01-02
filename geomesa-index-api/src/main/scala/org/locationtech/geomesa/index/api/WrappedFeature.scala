/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.locationtech.geomesa.utils.index.ByteArrays
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
    * Feature ID bytes
    */
  def idBytes: Array[Byte]

  /**
    * Hash of the simple feature ID - can be used for sharding.
    *
    * Note: we could use the idBytes here, but for back compatibility of deletes we don't want to change it
    */
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}
