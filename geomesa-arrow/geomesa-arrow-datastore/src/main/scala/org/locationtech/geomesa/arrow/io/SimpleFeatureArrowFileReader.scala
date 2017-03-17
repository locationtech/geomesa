/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, FileInputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.file.ArrowFileReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.opengis.feature.simple.SimpleFeature

class SimpleFeatureArrowFileReader(is: FileInputStream, allocator: BufferAllocator) extends Closeable {

  private val reader = new ArrowFileReader(is.getChannel, allocator)
  private val root = reader.getVectorSchemaRoot
  require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[NullableMapVector], "Invalid file")
  private val vector = SimpleFeatureVector.wrap(root.getFieldVectors.get(0).asInstanceOf[NullableMapVector], allocator)

  def read(decodeDictionaries: Boolean = true): Iterator[SimpleFeature] = {
    new Iterator[SimpleFeature] {
      private var done = false
      private var index = 0

      override def hasNext: Boolean = {
        if (done) {
          false
        } else if (index < root.getRowCount) {
          true
        } else {
          index = 0
          reader.loadNextBatch()
          if (root.getRowCount == 0) {
            done = true
            false
          } else {
            true
          }
        }
      }

      override def next(): SimpleFeature = {
        val sf = vector.reader.get(index)
        index += 1
        sf
      }
    }
  }

  override def close(): Unit = {
    reader.close()
    vector.close()
  }
}
