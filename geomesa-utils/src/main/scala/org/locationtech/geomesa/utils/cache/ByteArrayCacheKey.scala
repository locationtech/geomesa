/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import com.clearspring.analytics.hash.MurmurHash
import com.typesafe.scalalogging.LazyLogging

/**
  * Cache key for comparing byte arrays
  *
  * @param bytes bytes
  */
class ByteArrayCacheKey(val bytes: Array[Byte]) extends LazyLogging {

  override def hashCode(): Int = MurmurHash.hash(bytes)

  override def equals(obj: Any): Boolean = {
    obj match {
      case b: ByteArrayCacheKey => java.util.Arrays.equals(bytes, b.bytes)
      case _ => false
    }
  }

  // writes out the bytes as hex
  override def toString: String =
    bytes.map(b => f"${(b & 0xff) >>> 4}%01x${b & 0x0f}%01x").mkString("ByteArrayCacheKey[", ":", "]")
}
