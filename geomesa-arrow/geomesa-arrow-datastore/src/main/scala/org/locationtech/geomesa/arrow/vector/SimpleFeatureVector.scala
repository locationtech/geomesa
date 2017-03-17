/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.NullableMapWriter
import org.locationtech.geomesa.arrow.feature.{ArrowAttributeReader, ArrowAttributeWriter}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class SimpleFeatureVector(val sft: SimpleFeatureType, allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  // TODO user data

  val vector = new NullableMapVector("features", allocator, null, null)
  vector.allocateNew()

  // note: writer creates the map child vectors based on the sft, and should be instantiated before the reader
  val writer = new Writer(this)
  val reader = new Reader(this)

  override def close(): Unit = {
    vector.close()
    writer.arrowWriter.close()
  }

  class Writer(vector: SimpleFeatureVector) {
    private [SimpleFeatureVector] val arrowWriter = new NullableMapWriter(vector.vector)
    private val idWriter = ArrowAttributeWriter("id", Seq(ObjectType.STRING), classOf[String], arrowWriter, allocator)
    private [SimpleFeatureVector] val attributeWriters = sft.getAttributeDescriptors.map { ad =>
      ArrowAttributeWriter(ad, arrowWriter, allocator)
    }.toArray

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
    private val idReader = ArrowAttributeReader("id", Seq(ObjectType.STRING), classOf[String], vector.vector)
    private val attributeReaders = sft.getAttributeDescriptors.map { ad =>
      ArrowAttributeReader(ad, vector.vector)
    }.toArray

    def read(i: Int): SimpleFeature = {
      val id = idReader(i).asInstanceOf[String]
      val attributes = attributeReaders.map(_.apply(i))
      new ScalaSimpleFeature(id, vector.sft, attributes)
    }

    def getValueCount: Int = vector.vector.getAccessor.getValueCount
  }
}
