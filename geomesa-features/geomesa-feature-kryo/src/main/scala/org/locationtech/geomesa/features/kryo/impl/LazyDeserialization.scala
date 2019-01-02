/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import java.io.InputStream

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.ScalaSimpleFeature.{LazyImmutableSimpleFeature, LazyMutableSimpleFeature}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.SimpleFeature

object LazyDeserialization {

  /**
    * Creates mutable features, lazily evaluated
    */
  trait MutableLazyDeserialization extends LazyDeserialization {
    override protected def createFeature(id: String,
                                         offsets: Array[Int],
                                         userDataOffset: Int,
                                         input: Input): SimpleFeature = {
      new LazyMutableSimpleFeature(deserializeSft, id, readAttribute(_, offsets, input),
        readUserData(userDataOffset, input))
    }
  }

  /**
    * Creates immutable features, lazily evaluated
    */
  trait ImmutableLazyDeserialization extends LazyDeserialization {
    override protected def createFeature(id: String,
                                         offsets: Array[Int],
                                         userDataOffset: Int,
                                         input: Input): SimpleFeature = {
      new LazyImmutableSimpleFeature(deserializeSft, id, readAttribute(_, offsets, input),
        readUserData(userDataOffset, input))
    }
  }
}

/**
  * Wraps the input but defers deserialization until an attribute is required
  */
trait LazyDeserialization extends KryoFeatureDeserialization {

  protected def createFeature(id: String, offsets: Array[Int], userDataOffset: Int, input: Input): SimpleFeature

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize("", bytes, 0, bytes.length)

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature =
    deserialize(id, bytes, 0, bytes.length)

  // TODO read into a byte array so we can lazily evaluate it
  // user data is tricky here as we don't know the length...
  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError
  override def deserialize(id: String, in: InputStream): SimpleFeature = throw new NotImplementedError

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    deserialize("", bytes, offset, length)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val input = new Input(bytes, offset, offset + length)
    if (input.readInt(true) != KryoFeatureSerializer.VERSION) {
      throw new IllegalArgumentException("Can't process features serialized with wrong version")
    }
    // read the start of the offsets, then the feature id
    val offsets = Array.ofDim[Int](readers.length)
    val offsetStarts = offset + input.readInt()
    val finalId = if (withoutId) { id } else { input.readString() }
    // now read our offsets
    input.setPosition(offsetStarts) // set to offsets start
    var i = 0
    while (i < offsets.length && input.position < input.limit) {
      offsets(i) = offset + input.readInt(true)
      i += 1
    }
    if (i < offsets.length) {
      // attributes have been added to the sft since this feature was serialized
      do { offsets(i) = -1; i += 1 } while (i < offsets.length)
    }
    val userDataOffset = input.position()

    createFeature(finalId, offsets, userDataOffset, input)
  }


  protected def readAttribute(index: Int, offsets: Array[Int], input: Input): AnyRef = {
    val offset = offsets(index)
    if (offset == -1) { null } else {
      input.setPosition(offset)
      readers(index)(input)
    }
  }

  protected def readUserData(offset: Int, input: Input): java.util.Map[AnyRef, AnyRef] = {
    input.setPosition(offset)
    readUserData(input, skipOffsets = false)
  }
}