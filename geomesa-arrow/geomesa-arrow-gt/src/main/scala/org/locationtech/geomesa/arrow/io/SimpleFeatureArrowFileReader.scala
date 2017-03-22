/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, InputStream}
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.NullableVarCharVector
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.stream.ArrowStreamReader
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

class SimpleFeatureArrowFileReader(is: InputStream, decodeDictionaries: Boolean = true)(implicit allocator: BufferAllocator)
    extends Closeable {

  private val reader = new ArrowStreamReader(is, allocator)
  reader.loadNextBatch() // load the first batch so we get any dictionaries
  private val root = reader.getVectorSchemaRoot
  require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[NullableMapVector], "Invalid file")
  private val underlying = root.getFieldVectors.get(0).asInstanceOf[NullableMapVector]

  val dictionaries: Map[String, ArrowDictionary] = if (!decodeDictionaries) { Map.empty } else {
    import scala.collection.JavaConversions._
    underlying.getField.getChildren.flatMap { field =>
      Option(field.getDictionary).map { encoding =>
        val accessor = reader.lookup(encoding.getId).getVector.asInstanceOf[NullableVarCharVector].getAccessor
        val values = ArrayBuffer.empty[String]
        var i = 0
        while (i < accessor.getValueCount) {
          values.append(new String(accessor.get(i), StandardCharsets.UTF_8))
          i += 1
        }
        field.getName -> new ArrowDictionary(values, encoding.getId)
      }
    }.toMap
  }

  private val vector = SimpleFeatureVector.wrap(underlying, dictionaries)

  def getSchema: SimpleFeatureType = vector.sft

  def read(): Iterator[SimpleFeature] = {
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
