/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.{ComplexWriterImpl, NullableMapWriter}
import org.locationtech.geomesa.arrow.feature.ArrowAttributeWriter
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class SimpleFeatureVector(sft: SimpleFeatureType, allocator: BufferAllocator) {

  import scala.collection.JavaConversions._

  private val vector = new NullableMapVector("features", allocator, null, null)
  vector.allocateNew()

  private val writer = new Writer(this)
  private val reader = new Reader(this)

  def getVector: NullableMapVector = vector
  def getWriter: Writer = writer
  def getReader: Reader = reader

  class Writer(vector: SimpleFeatureVector) {

    private val arrowWriter = new NullableMapWriter(vector.vector)
    private val idWriter = ArrowAttributeWriter("id", Seq(ObjectType.STRING), classOf[String], arrowWriter, allocator)
    private val attributeWriters = sft.getAttributeDescriptors.map(ad => ArrowAttributeWriter(ad, arrowWriter, allocator)).toArray

    def write(i: Int, feature: SimpleFeature): Unit = {
      arrowWriter.setPosition(i)
      arrowWriter.start()
      idWriter.apply(feature.getID)
      var j = 0
      while (j < attributeWriters.length) {
        attributeWriters(j).apply(feature.getAttribute(j))
        j += 1
      }
      arrowWriter.end()
    }

    def setValueCount(count: Int): Unit = arrowWriter.setValueCount(count)
  }

  class Reader(vector: SimpleFeatureVector) {

  }
}

object SimpleFeatureVector {
  def apply(sft: SimpleFeatureType, allocator: BufferAllocator): SimpleFeatureVector = {
    val vector: NullableMapVector = null
    val writer: NullableMapWriter = null
  }
}