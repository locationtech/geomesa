/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, InputStream}
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.NullableVarCharVector
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.stream.ArrowStreamReader
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * For reading simple features from an arrow file written by SimpleFeatureArrowFileWriter.
  *
  * Expects arrow streaming format (no footer).
  *
  * @param is input stream
  * @param allocator buffer allocator
  */
class SimpleFeatureArrowFileReader(is: InputStream, filter: Filter = Filter.INCLUDE)
                                  (implicit allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  private val reader = new ArrowStreamReader(is, allocator)
  reader.loadNextBatch() // load the first batch so we get any dictionaries
  private val root = reader.getVectorSchemaRoot
  require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[NullableMapVector], "Invalid file")
  private val underlying = root.getFieldVectors.get(0).asInstanceOf[NullableMapVector]

  // load any dictionaries into memory
  val dictionaries: Map[String, ArrowDictionary] = underlying.getField.getChildren.flatMap { field =>
    Option(field.getDictionary).map { encoding =>
      val accessor = reader.lookup(encoding.getId).getVector.asInstanceOf[NullableVarCharVector].getAccessor
      val values = ArrayBuffer.empty[String]
      var i = 0
      while (i < accessor.getValueCount) {
        values.append(new String(accessor.get(i), StandardCharsets.UTF_8))
        i += 1
      }
      field.getName -> new ArrowDictionary(values, encoding.getId)
    }.toSeq
  }.toMap

  private val vector = SimpleFeatureVector.wrap(underlying, dictionaries)

  val sft = vector.sft

  // iterator of simple features read from the input stream
  // note that features are lazily backed by the underlying arrow vectors,
  // so may not be valid once another batch is loaded
  val features = new Iterator[SimpleFeature] {
    private var done = false
    private var batch: Iterator[SimpleFeature] = filterBatch().iterator

    override def hasNext: Boolean = {
      if (done) {
        false
      } else if (batch.hasNext) {
        true
      } else {
        reader.loadNextBatch()
        if (root.getRowCount == 0) {
          done = true
          false
        } else {
          batch = filterBatch().iterator
          hasNext
        }
      }
    }

    override def next(): SimpleFeature = batch.next()
  }

  /**
    * Evaluate the filter against each vector in the current batch.
    * This should optimize memory reads for the filtered attributes by checking them all up front
    *
    * @return
    */
  private def filterBatch(): Seq[SimpleFeature] = {
    if (filter == Filter.INCLUDE) {
      (0 until root.getRowCount).map(vector.reader.get)
    } else {
      (0 until root.getRowCount).flatMap { i =>
        val sf = vector.reader.get(i)
        if (filter.evaluate(sf)) {
          Seq(sf)
        } else {
          Seq.empty
        }
      }
    }
  }

  override def close(): Unit = {
    reader.close()
    vector.close()
  }
}
