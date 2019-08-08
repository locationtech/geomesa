/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo
package impl

import java.io.InputStream

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.ScalaSimpleFeature.{LazyImmutableSimpleFeature, LazyMutableSimpleFeature}
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.opengis.feature.simple.SimpleFeature

object LazyDeserialization {

  /**
    * Creates mutable features, lazily evaluated
    */
  trait MutableLazyDeserialization extends LazyDeserialization {
    override protected def createFeature(
        id: String,
        readAttribute: Int => AnyRef,
        readUserData: () => java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new LazyMutableSimpleFeature(out, id, readAttribute, readUserData)
    }
  }

  /**
    * Creates immutable features, lazily evaluated
    */
  trait ImmutableLazyDeserialization extends LazyDeserialization {
    override protected def createFeature(
        id: String,
        readAttribute: Int => AnyRef,
        readUserData: () => java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new LazyImmutableSimpleFeature(out, id, readAttribute, readUserData)
    }
  }
}

/**
  * Wraps the input but defers deserialization until an attribute is required
  */
trait LazyDeserialization extends KryoFeatureDeserialization {

  override def deserialize(bytes: Array[Byte]): SimpleFeature =
    readFeature("", new Input(bytes, 0, bytes.length))

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature =
    readFeature(id, new Input(bytes, 0, bytes.length))

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    readFeature("", new Input(bytes, offset, length))

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    readFeature(id, new Input(bytes, offset, length))

  // TODO read into a byte array so we can lazily evaluate it
  // user data is tricky here as we don't know the length...
  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError
  override def deserialize(id: String, in: InputStream): SimpleFeature = throw new NotImplementedError

  protected def createFeature(
      id: String,
      readAttribute: Int => AnyRef,
      readUserData: () => java.util.Map[AnyRef, AnyRef]): SimpleFeature

  private def readFeature(id: String, input: Input): SimpleFeature = {
    input.readByte() match {
      case KryoFeatureSerializer.Version  => readFeatureV3(id, input)
      case KryoFeatureSerializer.Version2 => readFeatureV2(id, input)
      case b => throw new IllegalArgumentException(s"Can't process features serialized with version: $b")
    }
  }

  private def readFeatureV3(id: String, input: Input): SimpleFeature = {
    val count = input.readShort()
    val offset = input.position()

    // read our null mask
    input.setPosition(offset + (2 * count) + 2)
    val nulls = IntBitSet.deserialize(input, count)

    // we should now be positioned to read the feature id
    val finalId = if (withoutId) { id } else { input.readString() }
    val toAttribute: Int => AnyRef = readAttributeV3(input, offset, count, nulls)
    val toUserData: () => java.util.Map[AnyRef, AnyRef] = readUserDataV3(input, offset, count)
    createFeature(finalId, toAttribute, toUserData)
  }

  private def readAttributeV3(input: Input, offset: Int, count: Int, nulls: IntBitSet)(i: Int): AnyRef = {
    if (i >= count || nulls.contains(i)) { null } else {
      // read the offset and go to the position for reading
      input.setPosition(offset + (2 * i))
      input.setPosition(offset + input.readShort())
      readers(i).apply(input)
    }
  }

  private def readUserDataV3(input: Input, offset: Int, count: Int)(): java.util.Map[AnyRef, AnyRef] = {
    if (withoutUserData) { new java.util.HashMap[AnyRef, AnyRef](1) } else {
      // read the offset and go to the position for reading
      input.setPosition(offset + (2 * count))
      input.setPosition(offset + input.readShort())
      KryoUserDataSerialization.deserialize(input)
    }
  }

  private def readFeatureV2(id: String, input: Input): SimpleFeature = {
    val offset = input.position() - 1 // we've already read our version byte
    // read the start of the offsets, then the feature id
    val offsets = Array.ofDim[Int](readersV2.length)
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

    val toAttribute: Int => AnyRef = readAttributeV2(offsets, input)
    val toUserData: () => java.util.Map[AnyRef, AnyRef]  = readUserDataV2(userDataOffset, input)

    createFeature(finalId, toAttribute, toUserData)
  }

  protected def readAttributeV2(offsets: Array[Int], input: Input)(index: Int): AnyRef = {
    val offset = offsets(index)
    if (offset == -1) { null } else {
      input.setPosition(offset)
      readersV2(index)(input)
    }
  }

  protected def readUserDataV2(offset: Int, input: Input)(): java.util.Map[AnyRef, AnyRef] = {
    if (withoutUserData) { new java.util.HashMap[AnyRef, AnyRef] } else {
      input.setPosition(offset)
      KryoUserDataSerialization.deserialize(input)
    }
  }
}
