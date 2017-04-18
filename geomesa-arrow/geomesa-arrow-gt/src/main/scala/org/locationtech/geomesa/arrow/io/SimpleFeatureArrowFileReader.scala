/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, InputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.stream.ArrowStreamReader
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeReader, ArrowDictionary, GeometryFields, SimpleFeatureVector}
import org.locationtech.geomesa.features.arrow.ArrowSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureSpecParser
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * For reading simple features from an arrow file written by SimpleFeatureArrowFileWriter.
  *
  * Expects arrow streaming format (no footer). Can handle multiple 'files' in a single input stream
  *
  * @param is input stream
  * @param filter ecql filter for features to return
  * @param allocator buffer allocator
  */
class SimpleFeatureArrowFileReader(is: InputStream, filter: Filter = Filter.INCLUDE)
                                  (implicit allocator: BufferAllocator) extends Closeable {

  private var reader = new SingleFileReader()
  // we track all the readers and close at the end to avoid closing the input stream prematurely
  private val readers = ArrayBuffer(reader)

  // note: may change as features are read, if multiple logical 'files' in the input stream
  def sft: SimpleFeatureType = reader.sft
  // note: may change as features are read, if multiple logical 'files' in the input stream
  def dictionaries: Map[String, ArrowDictionary] = reader.dictionaries

  lazy val features = new Iterator[SimpleFeature] {
    private var done = false
    private var batch: Iterator[SimpleFeature] = reader.features

    override def hasNext: Boolean = {
      if (done) {
        false
      } else if (batch.hasNext) {
        true
      } else if (is.available() > 0) {
        reader = new SingleFileReader()
        readers.append(reader)
        batch = reader.features
        hasNext
      } else {
        done = true
        false
      }
    }

    override def next(): SimpleFeature = batch.next()
  }

  override def close(): Unit = readers.foreach(_.close())

  /**
    * Reads a single logical arrow 'file'
    */
  private class SingleFileReader extends Closeable {

    import scala.collection.JavaConversions._

    private val reader = new ArrowStreamReader(is, allocator)
    private var firstBatchLoaded = false
    private val root = reader.getVectorSchemaRoot
    require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[NullableMapVector], "Invalid file")
    private val underlying = root.getFieldVectors.get(0).asInstanceOf[NullableMapVector]

    // load any dictionaries into memory
    val dictionaries: Map[String, ArrowDictionary] = underlying.getField.getChildren.flatMap { field =>
      Option(field.getDictionary).map { encoding =>
        if (!firstBatchLoaded) {
          reader.loadNextBatch() // load the first batch so we get any dictionaries
          firstBatchLoaded = true
        }
        val vector = reader.lookup(encoding.getId).getVector
        val spec = SimpleFeatureSpecParser.parseAttribute(field.getName)
        val (objectType, bindings) = ObjectType.selectType(spec.clazz, spec.options)
        val isSingle = GeometryFields.precisionFromField(field) == FloatingPointPrecision.SINGLE
        val precision = if (isSingle) { GeometryPrecision.Float } else { GeometryPrecision.Double }
        val attributeReader = ArrowAttributeReader(bindings.+:(objectType), spec.clazz, vector, None, precision)

        val values = ArrayBuffer.empty[AnyRef]
        var i = 0
        while (i < vector.getAccessor.getValueCount) {
          values.append(attributeReader.apply(i))
          i += 1
        }
        field.getName -> new ArrowDictionary(values, encoding)
      }.toSeq
    }.toMap

    private val vector = SimpleFeatureVector.wrap(underlying, dictionaries)

    val sft: SimpleFeatureType = vector.sft

    // iterator of simple features read from the input stream
    lazy val features: Iterator[SimpleFeature] = new Iterator[SimpleFeature] {
      private var done = false
      private var batch: Iterator[ArrowSimpleFeature] = filterBatch()

      override def hasNext: Boolean = {
        if (done) {
          false
        } else if (batch.hasNext) {
          true
        } else if (reader.loadNextBatch()) {
          batch = filterBatch()
          hasNext
        } else {
          done = true
          false
        }
      }

      override def next(): SimpleFeature = {
        val n = batch.next()
        n.load() // load values into memory so that they don't get lost when the next batch is loaded
        n
      }
    }

    /**
      * Evaluate the filter against each vector in the current batch.
      * This should optimize memory reads for the filtered attributes by checking them all up front
      *
      * @return
      */
    private def filterBatch(): Iterator[ArrowSimpleFeature] = {
      if (!firstBatchLoaded) {
        reader.loadNextBatch()
        firstBatchLoaded = true
      }
      val all = Iterator.range(0, root.getRowCount).map(vector.reader.get)
      if (filter == Filter.INCLUDE) { all } else {
        all.filter(filter.evaluate)
      }
    }

    override def close(): Unit = {
      reader.close()
      vector.close()
    }
  }
}