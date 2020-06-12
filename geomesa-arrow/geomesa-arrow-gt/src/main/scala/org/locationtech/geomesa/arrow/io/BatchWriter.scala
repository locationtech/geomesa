/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import org.apache.arrow.vector.ipc.message.IpcOption
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.math.Ordering

object BatchWriter {

  private val ordering = new Ordering[(AnyRef, Int, Int)] {
    override def compare(x: (AnyRef, Int, Int), y: (AnyRef, Int, Int)): Int =
      SimpleFeatureOrdering.nullCompare(x._1.asInstanceOf[Comparable[Any]], y._1)
  }

  /**
   * Reduce function for batches with a common dictionary
   *
   * @param sft simple feature type
   * @param dictionaries dictionaries
   * @param encoding simple feature encoding
   * @param sort sort metadata, if defined each batch is assumed to be sorted
   * @param sorted whether the batches are already globally sorted
   * @param batchSize batch size
   * @param batches record batches
   * @return
   */
  def reduce(
      sft: SimpleFeatureType,
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      sorted: Boolean,
      batchSize: Int,
      batches: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val (iter, firstBatchHasHeader) = if (sorted || sort.isEmpty) { (batches, false) } else {
      val Some((f, r)) = sort
      (new BatchSortingIterator(sft, dictionaries, encoding, ipcOpts, f, r, batchSize, batches), true)
    }
    createFileFromBatches(sft, dictionaries, encoding, ipcOpts, sort, iter, firstBatchHasHeader)
  }

  /**
   * Globally sorts batches. Each batch is expected to already be locally sorted.
   *
   * By default, the first batch will include the arrow file header metadata.
   *
   * @param sft simple feature type
   * @param dictionaries dictionaries
   * @param encoding encoding
   * @param sortBy sort by field
   * @param reverse sort is reversed?
   * @param batchSize number of records per batch
   * @param batches locally sorted batches
   * @param writeHeader if true, the first batch will include the arrow file header
   */
  private [io] class BatchSortingIterator(
      sft: SimpleFeatureType,
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sortBy: String,
      reverse: Boolean,
      batchSize: Int,
      batches: CloseableIterator[Array[Byte]],
      private var writeHeader: Boolean = true
    ) extends CloseableIterator[Array[Byte]] {

    private val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val unloader = new RecordBatchUnloader(result, ipcOpts)

    private var batch: Array[Byte] = _

    // gets the attribute we're sorting by from the i-th feature in the vector
    val getSortAttribute: (SimpleFeatureVector, Int) => AnyRef = {
      val sortByIndex = sft.indexOf(sortBy)
      if (dictionaries.contains(sortBy)) {
        // since we've sorted the dictionaries, we can just compare the encoded index values
        (vector, i) => vector.reader.feature.getReader(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i)
      } else {
        (vector, i) => vector.reader.feature.getReader(sortByIndex).apply(i)
      }
    }

    // this is lazy to allow the query plan to be instantiated without pulling back all the batches first
    private lazy val inputs: Array[(SimpleFeatureVector, (Int, Int) => Unit)] = {
      val builder = Array.newBuilder[(SimpleFeatureVector, (Int, Int) => Unit)]
      while (batches.hasNext) {
        val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
        RecordBatchLoader.load(vector.underlying, batches.next)

        val transfers: Seq[(Int, Int) => Unit] = {
          val fromVectors = vector.underlying.getChildrenFromFields
          val toVectors = result.underlying.getChildrenFromFields
          val builder = Seq.newBuilder[(Int, Int) => Unit]
          builder.sizeHint(fromVectors.size())
          var i = 0
          while (i < fromVectors.size()) {
            builder += createTransferPair(fromVectors.get(i), toVectors.get(i))
            i += 1
          }
          builder.result()
        }
        val transfer: (Int, Int) => Unit = (from, to) => transfers.foreach(_.apply(from, to))
        builder += vector -> transfer
      }
      builder.result
    }

    // we do a merge sort of each batch
    // sorted queue of [(current batch value, current index in that batch, number of the batch)]
    private lazy val queue = {
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
          CloseWithLogging(vector)
        }
        i += 1
      }
      heads
    }

    // gets the next record batch to write - returns null if no further records
    private def nextBatch(): Array[Byte] = {
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
            CloseWithLogging(vector)
          }
        }
        if (writeHeader) {
          writeHeader = false
          writeHeaderAndFirstBatch(result, dictionaries, ipcOpts, Some(sortBy -> reverse), resultIndex)
        } else {
          unloader.unload(resultIndex)
        }
      }
    }

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

    override def close(): Unit = {
      CloseWithLogging(result, batches)
      inputs.foreach { case (vector, _) => CloseWithLogging(vector) }
    }
  }
}
