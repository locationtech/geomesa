/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import java.io.InputStream

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature.ImmutableSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.getInput
import org.opengis.feature.simple.SimpleFeature

object ActiveDeserialization {

  /**
    * Creates mutable features
    */
  trait MutableActiveDeserialization extends ActiveDeserialization {
    override protected def createFeature(id: String,
                                         attributes: Array[AnyRef],
                                         userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new ScalaSimpleFeature(deserializeSft, id, attributes, userData)
    }
  }

  /**
    * Creates immutable features
    */
  trait ImmutableActiveDeserialization extends ActiveDeserialization {
    override protected def createFeature(id: String,
                                         attributes: Array[AnyRef],
                                         userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature = {
      new ImmutableSimpleFeature(deserializeSft, id, attributes, userData)
    }
  }
}

/**
  * Fully deserializes the simple feature before returning
  */
trait ActiveDeserialization extends KryoFeatureDeserialization {

  protected def createFeature(id: String,
                              attributes: Array[AnyRef],
                              userData: java.util.Map[AnyRef, AnyRef]): SimpleFeature

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize(bytes, 0, bytes.length)

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    readFeature(getInput(bytes, offset, length))

  override def deserialize(in: InputStream): SimpleFeature = readFeature(getInput(in))

  private def readFeature(input: Input): SimpleFeature = {
    val offset = input.position()
    if (input.readInt(true) != KryoFeatureSerializer.VERSION) {
      throw new IllegalArgumentException("Can't process features serialized with an older version")
    }

    // read the start of the offsets - we'll stop reading when we hit this
    val limit = offset + input.readInt()
    val id = readId(input)
    val attributes = Array.ofDim[AnyRef](readers.length)
    var i = 0
    while (i < readers.length && input.position < limit) {
      attributes(i) = readers(i)(input)
      i += 1
    }
    val userData = readUserData(input, skipOffsets = true)
    createFeature(id, attributes, userData)
  }
}
