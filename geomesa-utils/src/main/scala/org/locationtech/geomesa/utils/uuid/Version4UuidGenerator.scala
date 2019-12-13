/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.security.SecureRandom

import com.google.common.primitives.Longs
import org.locationtech.geomesa.utils.cache.SoftThreadLocal

/**
 *
 * Base trait for generating version 4 UUIDs.
 */
trait Version4UuidGenerator {

  private val byteCache = new SoftThreadLocal[Array[Byte]]

  /**
   * Gets a reusable byte array of length 8. This is a thread-local value, so be careful how it's used.
   */
  def getTempByteArray = byteCache.getOrElseUpdate(Array.ofDim[Byte](8))

  /**
   * Sets the variant number for the UUID. This overwrites the first 2 bits of the 1st byte.
   * Expects an 8 byte array that is the least significant half of the UUID.
   */
  def setVariant(bytes: Array[Byte]): Unit = {
    bytes(0) = (bytes(0) & 0x3f).asInstanceOf[Byte] // clear variant
    bytes(0) = (bytes(0) | 0x80).asInstanceOf[Byte] // set to IETF variant
  }

  /**
   * Sets the version number for the UUID. This overwrites the first 4 bits of the 7th byte.
   * Expects an 8 byte array that is the most significant half of the UUID.
   */
  def setVersion(bytes: Array[Byte]): Unit = {
    bytes(6) = (bytes(6) & 0x0f).asInstanceOf[Byte] // clear version
    bytes(6) = (bytes(6) | 0x40).asInstanceOf[Byte] // set to version 4 (random UUID)
  }
}

/**
 * Creates random least significant bits with the appropriate variant set
 */
trait RandomLsbUuidGenerator extends Version4UuidGenerator {

  private val r = new SecureRandom()

  /**
   * Creates the random part of the uuid.
   */
  def createRandomLsb(): Long = {
    val bytes = getTempByteArray
    // set the random bytes
    r.nextBytes(bytes)
    setVariant(bytes)
    Longs.fromByteArray(bytes)
  }
}