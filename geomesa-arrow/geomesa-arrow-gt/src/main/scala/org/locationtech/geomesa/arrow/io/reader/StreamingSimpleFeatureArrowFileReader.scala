/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.reader

import java.io.{Closeable, InputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader.{SkipIndicator, VectorToIterator, loadDictionaries}
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * Streams features from an input stream - note that a feature may not be valid after a call to `.next()`,
  * as the underlying data may be reclaimed
  *
  * @param is creates a new input stream
  * @param allocator buffer allocator
  */
class StreamingSimpleFeatureArrowFileReader(is: () => InputStream)(implicit allocator: BufferAllocator)
    extends SimpleFeatureArrowFileReader  {

  private var initialized = false

  lazy val (sft, sort) = {
    var sft: SimpleFeatureType = null
    var sort: Option[(String, Boolean)] = None
    WithClose(new ArrowStreamReader(is(), allocator)) { reader =>
      val root = reader.getVectorSchemaRoot
      require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
      val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      sft = SimpleFeatureVector.getFeatureType(underlying)._1
      sort = SimpleFeatureArrowIO.getSortFromMetadata(reader.getVectorSchemaRoot.getSchema.getCustomMetadata)
    }
    (sft, sort)
  }

  override lazy val dictionaries: Map[String, ArrowDictionary] = {
    import scala.collection.JavaConversions._

    initialized = true
    WithClose(is()) { is =>
      val reader = new ArrowStreamReader(is, allocator)
      WithClose(reader.getVectorSchemaRoot) { root =>
        require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
        val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
        reader.loadNextBatch() // load the first batch so we get any dictionaries
        val encoding = SimpleFeatureVector.getFeatureType(underlying)._2
        // load any dictionaries into memory
        loadDictionaries(underlying.getField.getChildren, reader, encoding)
      }
    }
  }

  override def vectors: Seq[SimpleFeatureVector] = throw new NotImplementedError()

  override def features(filter: Filter): Iterator[ArrowSimpleFeature] with Closeable = {
    val stream = is()
    val skip = new SkipIndicator
    val nextBatch = SimpleFeatureArrowFileReader.features(sft, filter, skip, sort, dictionaries)

    // we track all the readers and close at the end to avoid closing the input stream prematurely
    val readers = ArrayBuffer.empty[StreamingSingleFileReader]
    // reader for current logical 'file'
    var reader: StreamingSingleFileReader = null

    new Iterator[ArrowSimpleFeature] with Closeable {
      private var done = false
      private var batch: Iterator[ArrowSimpleFeature] = Iterator.empty

      override def hasNext: Boolean = {
        if (done) {
          false
        } else if (batch.hasNext) {
          true
        } else if (!skip.skip && stream.available() > 0) {
          // new logical file
          reader = new StreamingSingleFileReader(stream)
          readers.append(reader)
          batch = reader.features(nextBatch, skip)
          hasNext
        } else {
          done = true
          false
        }
      }

      override def next(): ArrowSimpleFeature = batch.next()

      override def close(): Unit = readers.foreach(_.close())
    }
  }

  override def close(): Unit = {
    if (initialized) {
      dictionaries.foreach(_._2.close())
    }
  }
}

/**
  * Reads a single logical arrow 'file' from the stream, which may contain multiple record batches
  */
private class StreamingSingleFileReader(is: InputStream)(implicit allocator: BufferAllocator) extends Closeable {

  import SimpleFeatureArrowFileReader.loadDictionaries

  import scala.collection.JavaConversions._

  private val reader = new ArrowStreamReader(is, allocator)
  private val root = reader.getVectorSchemaRoot
  require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
  private val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
  private var done = !reader.loadNextBatch() // load the first batch so we get any dictionaries

  val (sft, encoding) = SimpleFeatureVector.getFeatureType(underlying)

  // load any dictionaries into memory
  val dictionaries: Map[String, ArrowDictionary] = loadDictionaries(underlying.getField.getChildren, reader, encoding)
  private val vector = new SimpleFeatureVector(sft, underlying, dictionaries, encoding)

  def metadata: java.util.Map[String, String] = reader.getVectorSchemaRoot.getSchema.getCustomMetadata

  // iterator of simple features read from the input stream
  def features(nextBatch: VectorToIterator, skip: SkipIndicator): Iterator[ArrowSimpleFeature] = {
    if (done) { Iterator.empty } else {
      new Iterator[ArrowSimpleFeature] {
        private var batch: Iterator[ArrowSimpleFeature] = nextBatch(vector)
        override def hasNext: Boolean = {
          if (done) {
            false
          } else if (batch.hasNext) {
            true
          } else if (!skip.skip && reader.loadNextBatch()) {
            batch = nextBatch(vector)
            hasNext
          } else {
            done = true
            false
          }
        }

        override def next(): ArrowSimpleFeature = batch.next()
      }
    }
  }

  override def close(): Unit = {
    reader.close()
    vector.close()
  }
}
