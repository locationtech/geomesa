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
import java.nio.channels.{Channels, ReadableByteChannel}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader.{SkipIndicator, VectorToIterator}
import org.locationtech.geomesa.arrow.io.reader.CachingSimpleFeatureArrowFileReader.CachingSingleFileReader
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

class CachingSimpleFeatureArrowFileReader(is: InputStream) extends SimpleFeatureArrowFileReader  {

  private val allocator = ArrowAllocator("caching-file-reader")
  private val opened = ArrayBuffer.empty[CachingSingleFileReader]
  private val readers = createReaders()
  private lazy val sort = getSortFromMetadata(readers.head.metadata)

  override lazy val sft: SimpleFeatureType = readers.head.sft

  override lazy val dictionaries: Map[String, ArrowDictionary] = readers.head.dictionaries

  override lazy val vectors: Seq[SimpleFeatureVector] = readers.flatMap(_.vectors)

  override def features(filter: Filter): CloseableIterator[ArrowSimpleFeature] = {
    val skip = new SkipIndicator
    val nextBatch = SimpleFeatureArrowFileReader.features(sft, filter, skip, sort, dictionaries)
    new CloseableIterator[ArrowSimpleFeature] {
      // short-circuit if skip is toggled
      private val iter = readers.iterator.takeWhile(_ => !skip.skip).flatMap(_.features(nextBatch, skip))
      override def hasNext: Boolean = iter.hasNext
      override def next(): ArrowSimpleFeature = iter.next
      override def close(): Unit = {}
    }
  }

  override def close(): Unit = CloseWithLogging.raise(opened ++ Seq(allocator, is))

  // lazy stream to only read as much as is requested
  private def createReaders(): Stream[CachingSingleFileReader] = {
    if (is.available() > 0) {
      val reader = new CachingSingleFileReader(Channels.newChannel(is), allocator)
      opened.append(reader)
      reader #:: createReaders()
    } else {
      Stream.empty
    }
  }
}

object CachingSimpleFeatureArrowFileReader {

  import scala.collection.JavaConverters._

  /**
   * Reads a single logical arrow 'file' from the stream, which may contain multiple record batches
   */
  private class CachingSingleFileReader(is: ReadableByteChannel, allocator: BufferAllocator) extends Closeable {

    import SimpleFeatureArrowFileReader.loadDictionaries

    private val reader = new ArrowStreamReader(is, allocator)

    private val opened = ArrayBuffer.empty[SimpleFeatureVector]

    val vectors: Stream[SimpleFeatureVector] = {
      val hasMore = reader.loadNextBatch() // load dictionaries and the first batch
      val root = reader.getVectorSchemaRoot
      require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
      val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      val (_, encoding) = SimpleFeatureVector.getFeatureType(underlying)

      // load any dictionaries into memory
      val dictionaries = loadDictionaries(underlying.getField.getChildren.asScala, reader, encoding)

      // lazily evaluate batches as we need them
      def createStream(current: SimpleFeatureVector): Stream[SimpleFeatureVector] = {
        readIsolatedBatch(current, is, allocator) match {
          case None       => current #:: Stream.empty
          case Some(next) => opened.append(next); current #:: createStream(next)
        }
      }

      val head = SimpleFeatureVector.wrap(underlying, dictionaries)
      opened.append(head)
      if (hasMore) {
        createStream(head)
      } else {
        head #:: Stream.empty
      }
    }

    def sft: SimpleFeatureType = vectors.head.sft
    def dictionaries: Map[String, ArrowDictionary] = vectors.head.dictionaries
    def metadata: java.util.Map[String, String] = reader.getVectorSchemaRoot.getSchema.getCustomMetadata

    // iterator of simple features read from the input stream
    def features(nextBatch: VectorToIterator, skip: SkipIndicator): Iterator[ArrowSimpleFeature] with Closeable = {
      new Iterator[ArrowSimpleFeature] with Closeable {
        private var batch: Iterator[ArrowSimpleFeature] = Iterator.empty
        private val batches = vectors.iterator

        @scala.annotation.tailrec
        override def hasNext(): Boolean = {
          if (batch.hasNext) {
            true
          } else if (batches.hasNext) {
            if (skip.skip) {
              // make sure we read the rest of the record batches so that our input stream is at the end of a 'file'
              batches.foreach(_ => Unit)
              false
            } else {
              batch = nextBatch(batches.next)
              hasNext()
            }
          } else {
            false
          }
        }

        override def next(): ArrowSimpleFeature = batch.next()

        override def close(): Unit = {}
      }
    }

    override def close(): Unit = {
      reader.close()
      opened.foreach(_.close())
    }
  }

  // read a batch into a new simple feature vector, so that the batch still is accessible after reading the next one
  private def readIsolatedBatch(
      original: SimpleFeatureVector,
      is: ReadableByteChannel,
      allocator: BufferAllocator): Option[SimpleFeatureVector] = {
    WithClose(MessageSerializer.deserializeMessageBatch(new ReadChannel(is), allocator)) {
      case null => None

      case b: ArrowRecordBatch =>
        val fields = Seq(original.underlying.getField)
        val vectors = fields.map(_.createVector(allocator))
        val root = new VectorSchemaRoot(fields.asJava, vectors.asJava, 0)
        new VectorLoader(root).load(b)
        Some(SimpleFeatureVector.clone(original, vectors.head.asInstanceOf[StructVector]))

      case b => throw new IllegalArgumentException(s"Expected record batch but got $b")
    }
  }
}

