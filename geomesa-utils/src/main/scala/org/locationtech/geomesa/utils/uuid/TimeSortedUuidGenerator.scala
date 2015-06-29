/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.uuid

import java.security.SecureRandom
import java.util.UUID

import org.locationtech.geomesa.utils.cache.SoftThreadLocal

/**
 * UUID generator that creates UUIDs that sort by creation time (useful for accumulo).
 *
 * Uses variant 2 (IETF) and version 4 (for random UUIDs, although it's not totally random).
 * See https://en.wikipedia.org/wiki/Universally_unique_identifier#Variants_and_versions
 *
 * Using a version 1 (time based) UUID doesn't ensure uniqueness when running in different processes on
 * the same machine (at least not without some complicated distributed locking), as MAC address
 * (or IP address) is the unique factor.
 */
object TimeSortedUuidGenerator {

  private val r = new SecureRandom()
  private val byteCache = new SoftThreadLocal[Array[Byte]]

  /**
   * Creates a UUID where the first 16 bytes are based on the current time and the second 16 bytes are
   * based on a random number. This should provide uniqueness along with sorting by date.
   *
   * Doesn't support negative time values.
   */
  def createUuid(time: Long = System.currentTimeMillis()): UUID = {
    // get a reusable byte array
    val bytes = byteCache.getOrElseUpdate(Array.ofDim[Byte](8))
    val mostSigBits = timeBytes(time, bytes)
    val leastSigBits = randomBytes(bytes)
    new UUID(mostSigBits, leastSigBits)
  }

  /**
   * Creates the time based part of the uuid.
   */
  private def timeBytes(time: Long, array: Array[Byte]): Long = {
    // write the time in a sorted fashion
    // we drop the 4 most significant bits as we need 4 bits extra for the version
    // this shouldn't matter as we use sys time, so we don't need to worry about negative numbers
    array(0) = (time >> 52).asInstanceOf[Byte]
    array(1) = (time >> 44).asInstanceOf[Byte]
    array(2) = (time >> 36).asInstanceOf[Byte]
    array(3) = (time >> 28).asInstanceOf[Byte]
    array(4) = (time >> 20).asInstanceOf[Byte]
    array(5) = (time >> 12).asInstanceOf[Byte]
    array(6) = ((time >> 8) & 0x0f).asInstanceOf[Byte]  // the 0x0f clears the version bits
    array(7) = time.asInstanceOf[Byte]

    // set the version number for the UUID
    array(6) = (array(6) | 0x40).asInstanceOf[Byte] // set to version 4 (designates a random uuid)
    bytesToLong(array)
  }

  /**
   * Creates the random part of the uuid.
   */
  private def randomBytes(array: Array[Byte]): Long = {
    // set the random bytes
    r.nextBytes(array)
    // set the variant number for the UUID
    array(0) = (array(0) & 0x3f).asInstanceOf[Byte] // clear variant
    array(0) = (array(0) | 0x80).asInstanceOf[Byte] // set to IETF variant
    bytesToLong(array)
  }

  /**
   * Converts 8 bytes to a long
   */
  private def bytesToLong(bytes: Array[Byte]): Long = {
    var bits = 0L
    var i = 0
    while (i < 8) {
      bits = (bits << 8) | (bytes(i) & 0xff)
      i += 1
    }
    bits
  }
}
