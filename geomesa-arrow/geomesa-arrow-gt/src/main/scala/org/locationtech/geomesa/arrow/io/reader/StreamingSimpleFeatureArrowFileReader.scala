/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io
package reader

import java.io.{Closeable, InputStream}
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedDeque

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader.{SkipIndicator, VectorToIterator, loadDictionaries}
import org.locationtech.geomesa.arrow.io.reader.StreamingSimpleFeatureArrowFileReader.StreamingSingleFileReader
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * Streams features from an input stream - note that a feature may not be valid after a call to `.next()`,
  * as the underlying data may be reclaimed
  *
  * @param is creates a new input stream
  */
class StreamingSimpleFeatureArrowFileReader(is: () => InputStream) extends SimpleFeatureArrowFileReader  {

  import scala.collection.JavaConverters._

  private val allocator = ArrowAllocator("streaming-file-reader")

  private val opened = new ConcurrentLinkedDeque[AutoCloseable](Collections.singletonList(allocator))

  lazy val (sft, sort) = {
    var sft: SimpleFeatureType = null
    var sort: Option[(String, Boolean)] = None
    WithClose(new ArrowStreamReader(is(), allocator)) { reader =>
      val root = reader.getVectorSchemaRoot
      require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
      val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      sft = SimpleFeatureVector.getFeatureType(underlying)._1
      sort = getSortFromMetadata(root.getSchema.getCustomMetadata)
    }
    (sft, sort)
  }

  override lazy val dictionaries: Map[String, ArrowDictionary] = {
    val dicts = WithClose(is()) { is =>
      val reader = new ArrowStreamReader(is, allocator)
      opened.addFirst(reader)
      val root = reader.getVectorSchemaRoot
      require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
      val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      reader.loadNextBatch() // load the first batch so we get any dictionaries
      val encoding = SimpleFeatureVector.getFeatureType(underlying)._2
      // load any dictionaries into memory
      loadDictionaries(underlying.getField.getChildren.asScala, reader, encoding)
    }
    dicts.values.foreach(opened.addFirst)
    dicts
  }

  override def vectors: Seq[SimpleFeatureVector] = throw new NotImplementedError()

  override def features(filter: Filter): CloseableIterator[ArrowSimpleFeature] = {
    val stream = is()
    val skip = new SkipIndicator
    val nextBatch = SimpleFeatureArrowFileReader.features(sft, filter, skip, sort, dictionaries)

    // we track all the readers and close at the end to avoid closing the input stream prematurely
    val readers = ArrayBuffer.empty[StreamingSingleFileReader]
    // reader for current logical 'file'
    var reader: StreamingSingleFileReader = null

    new CloseableIterator[ArrowSimpleFeature] {
      private var done = false
      private var batch: Iterator[ArrowSimpleFeature] = Iterator.empty

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (done) {
          false
        } else if (batch.hasNext) {
          true
        } else if (!skip.skip && stream.available() > 0) {
          // new logical file
          reader = new StreamingSingleFileReader(stream, allocator)
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

  override def close(): Unit = CloseWithLogging.raise(opened.asScala)
}

object StreamingSimpleFeatureArrowFileReader {

  import scala.collection.JavaConverters._

  /**
   * Reads a single logical arrow 'file' from the stream, which may contain multiple record batches
   */
  private class StreamingSingleFileReader(is: InputStream, allocator: BufferAllocator) extends Closeable {

    private val reader = new ArrowStreamReader(is, allocator)
    private val root = reader.getVectorSchemaRoot
    require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
    private val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
    private var done = !reader.loadNextBatch() // load the first batch so we get any dictionaries

    val (sft, encoding) = SimpleFeatureVector.getFeatureType(underlying)

    // load any dictionaries into memory
    val dictionaries: Map[String, ArrowDictionary] =
      SimpleFeatureArrowFileReader.loadDictionaries(underlying.getField.getChildren.asScala, reader, encoding)
    private val vector = new SimpleFeatureVector(sft, underlying, dictionaries, encoding, None)

    def metadata: java.util.Map[String, String] = reader.getVectorSchemaRoot.getSchema.getCustomMetadata

    // iterator of simple features read from the input stream
    def features(nextBatch: VectorToIterator, skip: SkipIndicator): Iterator[ArrowSimpleFeature] = {
      if (done) { Iterator.empty } else {
        new Iterator[ArrowSimpleFeature] {
          private var batch: Iterator[ArrowSimpleFeature] = nextBatch(vector)

          @scala.annotation.tailrec
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

    override def close(): Unit = CloseWithLogging.raise(dictionaries.values ++ Seq(reader, vector))
  }
}
