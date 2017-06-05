/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import org.apache.arrow.vector.complex.NullableMapVector
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, ArrowDictionaryReader, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.math.Ordering

object SimpleFeatureArrowIO {

  import org.locationtech.geomesa.arrow.allocator

  object Metadata {
    val SortField = "sort-field"
    val SortOrder = "sort-order"
  }

  private val ordering = new Ordering[(Comparable[Any], Int, Int)] {
    override def compare(x: (Comparable[Any], Int, Int), y: (Comparable[Any], Int, Int)): Int =
      x._1.compareTo(y._1)
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
                  iter: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val batches = iter.toSeq
    if (batches.length < 2) {
      CloseableIterator(batches.iterator, iter.close())
    } else {
      // gets the attribute we're sorting by from the i-th feature in the vector
      val getSortAttribute: (SimpleFeatureVector, Int) => Comparable[Any] = {
        val sortByIndex = sft.indexOf(sortBy)
        if (dictionaries.contains(sortBy)) {
          // since we've sorted the dictionaries, we can just compare the encoded index values
          (vector, i) =>
            vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i).asInstanceOf[Comparable[Any]]
        } else {
          (vector, i) => vector.reader.readers(sortByIndex).apply(i).asInstanceOf[Comparable[Any]]
        }
      }

      val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

      val inputs = batches.map { bytes =>
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val field = result.underlying.getField
        val loader = new RecordBatchLoader(field)
        val vector = SimpleFeatureVector.wrap(loader.vector.asInstanceOf[NullableMapVector], dictionaries)
        loader.load(bytes)
        val transfer = vector.underlying.makeTransferPair(result.underlying)
        (vector, transfer)
      }.toArray

      // we do a merge sort of each batch
      // sorted queue of [(current batch value, current index in that batch, number of the batch)]
      val queue = {
        // populate with the first element from each batch
        // note: need to flip ordering here as highest sorted values come off the queue first
        val order = if (reverse) { ordering } else { ordering.reverse }
        val heads = scala.collection.mutable.PriorityQueue.empty[(Comparable[Any], Int, Int)](order)
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
            transfer.copyValueSafe(i, resultIndex)
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
}
