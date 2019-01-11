/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.security.SecureRandom
import java.util.UUID

import com.google.common.primitives.Longs
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

/**
 * Creates feature id based on current system time.
 */
class IngestTimeFeatureIdGenerator extends FeatureIdGenerator {
  override def createId(sft: SimpleFeatureType, sf: SimpleFeature) =
    TimeSortedUuidGenerator.createUuid().toString
}

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
object TimeSortedUuidGenerator extends RandomLsbUuidGenerator {

  /**
   * Creates a UUID where the first 8 bytes are based on the current time and the second 8 bytes are
   * based on a random number. This should provide uniqueness along with sorting by date.
   *
   * Doesn't support negative time values.
   */
  def createUuid(time: Long = System.currentTimeMillis()): UUID = {
    val mostSigBits = timeBytes(time)
    val leastSigBits = createRandomLsb()
    new UUID(mostSigBits, leastSigBits)
  }

  /**
   * Creates the time based part of the uuid.
   */
  private def timeBytes(time: Long): Long = {
    val array = getTempByteArray

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
    setVersion(array)

    Longs.fromByteArray(array)
  }
}
