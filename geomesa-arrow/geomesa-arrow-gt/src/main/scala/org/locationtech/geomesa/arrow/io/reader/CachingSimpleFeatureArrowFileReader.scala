/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.reader

import java.io.{Closeable, InputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader.{SkipIndicator, VectorToIterator}
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

class CachingSimpleFeatureArrowFileReader(is: InputStream)(implicit allocator: BufferAllocator)
    extends SimpleFeatureArrowFileReader  {

  private val opened = ArrayBuffer.empty[CachingSingleFileReader]
  private val readers = createReaders()
  private val sort = SimpleFeatureArrowIO.getSortFromMetadata(readers.head.metadata)

  override val sft: SimpleFeatureType = readers.head.sft

  override val dictionaries: Map[String, ArrowDictionary] = readers.head.dictionaries

  override val vectors: Seq[SimpleFeatureVector] = readers.flatMap(_.vectors)

  override def features(filter: Filter): Iterator[ArrowSimpleFeature] with Closeable = {
    val skip = new SkipIndicator
    val nextBatch = SimpleFeatureArrowFileReader.features(sft, filter, skip, sort, dictionaries)
    new Iterator[ArrowSimpleFeature] with Closeable {
      // short-circuit if skip is toggled
      private val iter = readers.iterator.takeWhile(_ => !skip.skip).flatMap(_.features(nextBatch, skip))
      override def hasNext: Boolean = iter.hasNext
      override def next(): ArrowSimpleFeature = iter.next
      override def close(): Unit = {}
    }
  }

  override def close(): Unit = {
    opened.foreach(_.close())
    is.close()
  }

  // lazy stream to only read as much as is requested
  private def createReaders(): Stream[CachingSingleFileReader] = {
    if (is.available() > 0) {
      val reader = new CachingSingleFileReader(Channels.newChannel(is))
      opened.append(reader)
      reader #:: createReaders()
    } else {
      is.close()
      Stream.empty
    }
  }
}

/**
  * Reads a single logical arrow 'file' from the stream, which may contain multiple record batches
  */
private class CachingSingleFileReader(is: ReadableByteChannel)(implicit allocator: BufferAllocator) extends Closeable {

  import SimpleFeatureArrowFileReader.loadDictionaries

  import scala.collection.JavaConversions._

  private val reader = new ArrowStreamReader(is, allocator)

  private val opened = ArrayBuffer.empty[SimpleFeatureVector]

  val vectors: Stream[SimpleFeatureVector] = {
    val hasMore = reader.loadNextBatch() // load dictionaries and the first batch
    val root = reader.getVectorSchemaRoot
    require(root.getFieldVectors.size() == 1 && root.getFieldVectors.get(0).isInstanceOf[StructVector], "Invalid file")
    val underlying = root.getFieldVectors.get(0).asInstanceOf[StructVector]
    val (sft, encoding) = SimpleFeatureVector.getFeatureType(underlying)

    // load any dictionaries into memory
    val dictionaries = loadDictionaries(underlying.getField.getChildren, reader, encoding)

    // lazily evaluate batches as we need them
    def createStream(current: SimpleFeatureVector): Stream[SimpleFeatureVector] = {
      CachingSingleFileReader.readIsolatedBatch(current, is) match {
        case None       => current #:: Stream.empty
        case Some(next) => opened.append(next); current #:: createStream(next)
      }
    }

    val head = new SimpleFeatureVector(sft, underlying, dictionaries, encoding)
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

      override def hasNext: Boolean = {
        if (batch.hasNext) {
          true
        } else if (batches.hasNext) {
          if (skip.skip) {
            // make sure we read the rest of the record batches so that our input stream is at the end of a 'file'
            batches.foreach(_ => Unit)
            false
          } else {
            batch = nextBatch(batches.next)
            hasNext
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

private object CachingSingleFileReader {

  // read a batch into a new simple feature vector, so that the batch still is accessible after reading the next one
  private def readIsolatedBatch(original: SimpleFeatureVector, is: ReadableByteChannel)
                               (implicit allocator: BufferAllocator): Option[SimpleFeatureVector] = {
    import scala.collection.JavaConversions._

    WithClose(MessageSerializer.deserializeMessageBatch(new ReadChannel(is), allocator)) {
      case null => None

      case b: ArrowRecordBatch =>
        val fields = Seq(original.underlying.getField)
        val vectors = fields.map(_.createVector(allocator))
        val root = new VectorSchemaRoot(fields, vectors, 0)
        new VectorLoader(root).load(b)
        Some(SimpleFeatureVector.clone(original, vectors.head.asInstanceOf[StructVector]))

      case b => throw new IllegalArgumentException(s"Expected record batch but got $b")
    }
  }
}
