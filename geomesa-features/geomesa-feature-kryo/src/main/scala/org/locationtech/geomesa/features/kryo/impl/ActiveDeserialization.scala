/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo
package impl

import java.io.InputStream

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature.ImmutableSimpleFeature
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.getInput
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object ActiveDeserialization {

  /**
    * Creates mutable features
    */
  trait MutableActiveDeserialization extends ActiveDeserialization {
    override protected def createFeature(
        id: String,
        attributes: Array[AnyRef],
        userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new ScalaSimpleFeature(out, id, attributes, userData)
    }
  }

  /**
    * Creates immutable features
    */
  trait ImmutableActiveDeserialization extends ActiveDeserialization {
    override protected def createFeature(
        id: String,
        attributes: Array[AnyRef],
        userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new ImmutableSimpleFeature(out, id, attributes, userData)
    }
  }
}

/**
  * Fully deserializes the simple feature before returning
  */
trait ActiveDeserialization extends KryoFeatureDeserialization {

  override def deserialize(bytes: Array[Byte]): SimpleFeature = readFeature("", getInput(bytes, 0, bytes.length))

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature =
    readFeature(id, getInput(bytes, 0, bytes.length))

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    readFeature("", getInput(bytes, offset, length))

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    readFeature(id, getInput(bytes, offset, length))

  override def deserialize(in: InputStream): SimpleFeature = readFeature("", getInput(in))

  override def deserialize(id: String, in: InputStream): SimpleFeature = readFeature(id, getInput(in))

  protected def createFeature(
      id: String,
      attributes: Array[AnyRef],
      userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature

  private def readFeature(id: String, input: Input): SimpleFeature = {
    input.readByte() match {
      case KryoFeatureSerializer.Version3 => readFeatureV3(id, input)
      case KryoFeatureSerializer.Version2 => readFeatureV2(id, input)
      case b => throw new IllegalArgumentException(s"Can't process features serialized with version: $b")
    }
  }

  private def readFeatureV3(id: String, input: Input): SimpleFeature = {
    val metadata = Metadata(input) // read count, size, nulls, etc

    // we should now be positioned to read the feature id
    val finalId = if (withoutId) { id } else { input.readString() }

    // read our attributes
    val attributes = Array.ofDim[AnyRef](out.getAttributeCount) // note: may not match the serialized count
    var i = 0
    while (i < metadata.count) {
      if (!metadata.nulls.contains(i)) {
        attributes(i) = readers(i).apply(input)
      }
      i += 1
    }

    val userData = if (withoutUserData) { new java.util.HashMap[AnyRef, AnyRef](1) } else {
      KryoUserDataSerialization.deserialize(input)
    }

    createFeature(finalId, attributes, userData)
  }

  private def readFeatureV2(id: String, input: Input): SimpleFeature = {
    val offset = input.position() - 1 // we've already read our version byte
    // read the start of the offsets - we'll stop reading when we hit this
    val limit = offset + input.readInt()
    val finalId = if (withoutId) { id } else { input.readString() }
    val attributes = Array.ofDim[AnyRef](readersV2.length)
    var i = 0
    while (i < readersV2.length && input.position < limit) {
      attributes(i) = readersV2(i)(input)
      i += 1
    }

    val userData = if (withoutUserData) { new java.util.HashMap[AnyRef, AnyRef] } else {
      // skip offset data
      try {
        i = 0
        while (i < readersV2.length) {
          input.readInt(true)
          i += 1
        }
        KryoUserDataSerialization.deserialize(input)
      } catch {
        case NonFatal(e) =>
          logger.error("Error reading serialized kryo user data:", e)
          new java.util.HashMap[AnyRef, AnyRef]()
      }
    }

    createFeature(finalId, attributes, userData)
  }
}
