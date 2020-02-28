/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import com.esotericsoftware.kryo.io.{Input, Output}
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.concurrent.duration.Duration

package object kryo {

  val SerializerCacheExpiry: Duration = SystemProperty("geomesa.serializer.cache.expiry", "1 hour").toDuration.get

  /**
    * Metadata for serialized simple features
    *
    * @param input kryo input
    * @param count number of attributes serialized in this feature (may be less than the current sft)
    * @param size size of each offset - either 2 or 4 bytes
    * @param offset attribute positions are stored relative to this offset into the serialized bytes
    * @param nulls null bit set
    */
  case class Metadata(input: Input, count: Int, size: Int, offset: Int, nulls: IntBitSet) {

    /**
      * Position the input to read an attribute
      *
      * @param i attribute to read
      * @return the relative position being set
      */
    def setPosition(i: Int): Int = {
      input.setPosition(offset + i * size)
      val pos = if (size == 2) { input.readShortUnsigned() } else { input.readInt() }
      input.setPosition(offset + pos)
      pos
    }

    /**
      * Position the input to read the feature ID
      *
      * @return the relative position being set
      */
    def setIdPosition(): Int = {
      val pos = size * (count + 1) + (IntBitSet.size(count) * 4)
      input.setPosition(offset + pos)
      pos
    }

    /**
      * Position the input to read the user data
      *
      * @return the relative position being set
      */
    def setUserDataPosition(): Int = setPosition(count)
  }

  object Metadata {

    /**
      * Read the metadata from a kryo input. The input should be positioned at the start of the serialized
      * simple feature, just after reading the 'version' byte
      *
      * @param input input
      * @return
      */
    def apply(input: Input): Metadata = {
      val count = input.readShortUnsigned()
      val size = input.readByte()
      val offset = input.position()
      // read our null mask
      input.setPosition(offset + size * (count + 1))
      val nulls = IntBitSet.deserialize(input, count)
      Metadata(input, count, size, offset, nulls)
    }

    /**
      * Write metadata to the output. After this call, the output will be positioned to write the feature ID
      *
      * @param output output
      * @param count number of serialized attributes
      * @param size size of each offset (2 or 4 bytes)
      * @return the relative offset used to track attribute offsets
      */
    def write(output: Output, count: Int, size: Int): Int = {
      output.writeShort(count) // track the number of attributes
      output.write(size) // size of each offset
      val offset = output.position()
      output.setPosition(offset + (size * (count + 1)) + (IntBitSet.size(count) * 4))
      offset
    }
  }
}
