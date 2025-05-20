/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io
package reader

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader.SkipIndicator
import org.locationtech.geomesa.arrow.io.reader.StreamingSimpleFeatureArrowFileReader.{StreamingFeatureIterator, StreamingSingleFileReader}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.{Closeable, InputStream}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Streams features from an input stream - note that a feature may not be valid after a call to `.next()`,
  * as the underlying data may be reclaimed
  *
  * @param is input stream
  */
class StreamingSimpleFeatureArrowFileReader(is: InputStream) extends SimpleFeatureArrowFileReader  {

  require(is.available() > 0, "Empty input stream")

  private val allocator = ArrowAllocator("streaming-file-reader")

  // reader for the first logical 'file'
  private val reader = Try(new StreamingSingleFileReader(is, allocator))

  override def sft: SimpleFeatureType = reader.get.sft
  override def dictionaries: Map[String, ArrowDictionary] = reader.get.dictionaries

  override def vectors: Seq[SimpleFeatureVector] = throw new NotImplementedError()

  override def features(filter: Filter): CloseableIterator[ArrowSimpleFeature] = {
    // if the read failed, throw an exception up front
    val reader = this.reader match {
      case Success(r) => r
      case Failure(e) => throw e
    }
    new StreamingFeatureIterator(is, reader, filter, allocator)
  }

  override def close(): Unit = CloseWithLogging.raise(reader.toOption.toSeq ++ Seq(allocator))
}

object StreamingSimpleFeatureArrowFileReader {

  import scala.collection.JavaConverters._

  /**
   * Creates an iterator from an arrow file input stream
   *
   * @param is input stream
   * @param starter first file reader, created from the input stream, for dictionaries and metadata
   * @param filter filter for feature reading
   * @param allocator allocator
   */
  private class StreamingFeatureIterator(is: InputStream, starter: StreamingSingleFileReader, filter: Filter, allocator: BufferAllocator)
      extends CloseableIterator[ArrowSimpleFeature] {

    // note: we track all the readers and close at the end to avoid closing the input stream prematurely
    private val opened = ArrayBuffer.empty[AutoCloseable]
    private val skip = new SkipIndicator

    private var done = false
    private var batch: Iterator[ArrowSimpleFeature] = starter.features(filter, skip)

    @tailrec
    override final def hasNext: Boolean = {
      if (done) {
        false
      } else if (batch.hasNext) {
        true
      } else if (!skip.skip && is.available() > 0) {
        // new logical file
        val nextReader = new StreamingSingleFileReader(is, allocator)
        opened += nextReader
        batch = nextReader.features(filter, skip)
        hasNext
      } else {
        done = true
        false
      }
    }

    override def next(): ArrowSimpleFeature = batch.next()

    override def close(): Unit = CloseWithLogging.raise(opened)
  }

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
      SimpleFeatureArrowFileReader.loadDictionaries(underlying.getField.getChildren.asScala.toSeq, reader, encoding)
    private val vector = new SimpleFeatureVector(sft, underlying, dictionaries, encoding, None)

    private def metadata: java.util.Map[String, String] = reader.getVectorSchemaRoot.getSchema.getCustomMetadata

    // iterator of simple features read from the input stream
    def features(filter: Filter, skip: SkipIndicator): Iterator[ArrowSimpleFeature] = {
      if (done) { Iterator.empty } else {
        val nextBatch = SimpleFeatureArrowFileReader.features(sft, filter, skip, getSortFromMetadata(metadata), dictionaries)

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
