/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.ByteArrayOutputStream
import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.Schema
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.math.Ordering

object SimpleFeatureArrowIO extends LazyLogging {

  object Metadata {
    val SortField = "sort-field"
    val SortOrder = "sort-order"
  }

  private val ordering = new Ordering[(AnyRef, Int, Int)] {
    override def compare(x: (AnyRef, Int, Int), y: (AnyRef, Int, Int)): Int =
      SimpleFeatureOrdering.nullCompare(x._1.asInstanceOf[Comparable[Any]], y._1)
  }

  /**
    * Reduce function for concatenating separate arrow files
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding simple feature encoding
    * @param sort sort
    * @param files full logical arrow files encoded in arrow streaming format
    * @return
    */
  def reduceFiles(sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  sort: Option[(String, Boolean)])
                 (files: CloseableIterator[Array[Byte]])
                 (implicit allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    // ensure we return something
    if (files.hasNext) { files } else {
      // files is empty but this will pass it through to be closed
      createFile(sft, createEmptyDictionaries(dictionaryFields), encoding, sort)(files)
    }
  }

  /**
    * Reduce function for batches with a common dictionary
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding simple feature encoding
    * @param sort sort
    * @param batchSize batch size
    * @param batches record batches
    * @return
    */
  def reduceBatches(sft: SimpleFeatureType,
                    dictionaries: Map[String, ArrowDictionary],
                    encoding: SimpleFeatureEncoding,
                    sort: Option[(String, Boolean)],
                    batchSize: Int)
                   (batches: CloseableIterator[Array[Byte]])
                   (implicit allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    val sorted = sort match {
      case None => batches
      case Some((field, reverse)) => sortBatches(sft, dictionaries, encoding, field, reverse, batchSize, batches)
    }
    createFile(sft, dictionaries, encoding, sort)(sorted)
  }

  /**
    * Sorts record batches. Batches are assumed to be already sorted.
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding options
    * @param sortBy attribute to sort by, assumed to be comparable
    * @param reverse sort reversed or not
    * @param batchSize batch size
    * @param iter iterator of batches
    * @return iterator of sorted batches
    */
  def sortBatches(sft: SimpleFeatureType,
                  dictionaries: Map[String, ArrowDictionary],
                  encoding: SimpleFeatureEncoding,
                  sortBy: String,
                  reverse: Boolean,
                  batchSize: Int,
                  iter: CloseableIterator[Array[Byte]])
                 (implicit allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    val batches = iter.toSeq
    if (batches.lengthCompare(2) < 0) {
      CloseableIterator(batches.iterator, iter.close())
    } else {
      // gets the attribute we're sorting by from the i-th feature in the vector
      val getSortAttribute: (SimpleFeatureVector, Int) => AnyRef = {
        val sortByIndex = sft.indexOf(sortBy)
        if (dictionaries.contains(sortBy)) {
          // since we've sorted the dictionaries, we can just compare the encoded index values
          (vector, i) => vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i)
        } else {
          (vector, i) => vector.reader.readers(sortByIndex).apply(i)
        }
      }

      val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

      val inputs: Array[(SimpleFeatureVector, (Int, Int) => Unit)] = batches.map { bytes =>
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val field = result.underlying.getField
        val loader = RecordBatchLoader(field)
        val vector = SimpleFeatureVector.wrap(loader.vector.asInstanceOf[StructVector], dictionaries)
        loader.load(bytes)
        val transfers: Seq[(Int, Int) => Unit] = {
          val fromVectors = vector.underlying.getChildrenFromFields
          val toVectors = result.underlying.getChildrenFromFields
          val builder = Seq.newBuilder[(Int, Int) => Unit]
          builder.sizeHint(fromVectors.size())
          var i = 0
          while (i < fromVectors.size()) {
            val fromVector = fromVectors.get(i)
            val toVector = toVectors.get(i)
            if (SimpleFeatureVector.isGeometryVector(fromVector)) {
              // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
              val from = GeometryFields.wrap(fromVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              val to = GeometryFields.wrap(toVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              builder += { (fromIndex: Int, toIndex: Int) => from.transfer(fromIndex, toIndex, to) }
            } else {
              val transfer = fromVector.makeTransferPair(toVector)
              builder += { (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex) }
            }
            i += 1
          }
          builder.result()
        }
        val transfer: (Int, Int) => Unit = (from, to) => transfers.foreach(_.apply(from, to))
        (vector, transfer)
      }.toArray

      // we do a merge sort of each batch
      // sorted queue of [(current batch value, current index in that batch, number of the batch)]
      val queue = {
        // populate with the first element from each batch
        // note: need to flip ordering here as highest sorted values come off the queue first
        val order = if (reverse) { ordering } else { ordering.reverse }
        val heads = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](order)
        var i = 0
        while (i < inputs.length) {
          val vector = inputs(i)._1
          if (vector.reader.getValueCount > 0) {
            heads.+=((getSortAttribute(vector, 0), 0, i))
          } else {
            vector.close()
          }
          i += 1
        }
        heads
      }

      val unloader = new RecordBatchUnloader(result)

      // gets the next record batch to write - returns null if no further records
      def nextBatch(): Array[Byte] = {
        if (queue.isEmpty) { null } else {
          result.clear()
          var resultIndex = 0
          // copy the next sorted value and then queue and sort the next element out of the batch we copied from
          while (queue.nonEmpty && resultIndex < batchSize) {
            val (_, i, batch) = queue.dequeue()
            val (vector, transfer) = inputs(batch)
            transfer.apply(i, resultIndex)
            result.underlying.setIndexDefined(resultIndex)
            resultIndex += 1
            val nextBatchIndex = i + 1
            if (vector.reader.getValueCount > nextBatchIndex) {
              val value = getSortAttribute(vector, nextBatchIndex)
              queue.+=((value, nextBatchIndex, batch))
            } else {
              vector.close()
            }
          }
          unloader.unload(resultIndex)
        }
      }

      val output = new Iterator[Array[Byte]] {
        private var batch: Array[Byte] = _

        override def hasNext: Boolean = {
          if (batch == null) {
            batch = nextBatch()
          }
          batch != null
        }

        override def next(): Array[Byte] = {
          val res = batch
          batch = null
          res
        }
      }

      def closeAll(): Unit = {
        CloseWithLogging(result)
        CloseWithLogging(iter)
        inputs.foreach(i => CloseWithLogging(i._1))
      }

      CloseableIterator(output, closeAll())
    }
  }

  /**
    * Creates an arrow streaming format file
    *
    * @param vector simple feature vector, used for arrow metadata and dictionaries
    * @param sort sort
    * @param body record batches
    * @return
    */
  def createFile(vector: SimpleFeatureVector,
                 sort: Option[(String, Boolean)])
                (body: CloseableIterator[Array[Byte]])
                (implicit allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    // header with schema and dictionaries
    val headerBytes = new ByteArrayOutputStream
    // make sure to copy bytes before closing so we get just the header metadata
    val writer = SimpleFeatureArrowFileWriter(vector, headerBytes, sort)
    writer.start()

    val header = Iterator.single(headerBytes.toByteArray)
    // per arrow streaming format footer is the encoded int '0'
    val footer = Iterator.single(Array[Byte](0, 0, 0, 0))
    CloseableIterator(header ++ body ++ footer, { CloseWithLogging(body); CloseWithLogging(writer) })
  }

  /**
    * Creates an arrow streaming format file
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding simple feature encoding
    * @param sort sort
    * @param body record batches
    * @return
    */
  def createFile(sft: SimpleFeatureType,
                 dictionaries: Map[String, ArrowDictionary],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)])
                (body: CloseableIterator[Array[Byte]])
                (implicit allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    createFile(SimpleFeatureVector.create(sft, dictionaries, encoding, 0), sort)(body)
  }

  /**
    * Checks schema metadata for sort fields
    *
    * @param metadata schema metadata
    * @return (sort field, reverse sorted or not)
    */
  def getSortFromMetadata(metadata: java.util.Map[String, String]): Option[(String, Boolean)] = {
    Option(metadata.get(Metadata.SortField)).map { field =>
      val reverse = Option(metadata.get(Metadata.SortOrder)).exists {
        case "descending" => true
        case _ => false
      }
      (field, reverse)
    }
  }

  /**
    * Creates metadata for sort fields
    *
    * @param field sort field
    * @param reverse reverse sorted or not
    * @return metadata map
    */
  def getSortAsMetadata(field: String, reverse: Boolean): java.util.Map[String, String] = {
    import scala.collection.JavaConversions._
    // note: reverse == descending
    Map(Metadata.SortField -> field, Metadata.SortOrder -> (if (reverse) { "descending" } else { "ascending" }))
  }

  /**
    * Creates a vector schema root for the given vector
    *
    * @param vector vector
    * @param metadata field metadata
    * @return
    */
  def createRoot(vector: FieldVector, metadata: java.util.Map[String, String] = null): VectorSchemaRoot = {
    val schema = new Schema(Collections.singletonList(vector.getField), metadata)
    new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getValueCount)
  }

  /**
    * Create empty dictionaries, for when there are no results
    *
    * @param fields dictionary fields
    * @return
    */
  private def createEmptyDictionaries(fields: Seq[String]): Map[String, ArrowDictionary] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike
    fields.mapWithIndex { case (name, i) => name -> ArrowDictionary.create(i, Array.empty[AnyRef]) }.toMap
  }
}
