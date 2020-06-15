/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayOutputStream, Closeable, OutputStream}
import java.nio.channels.Channels
import java.util.PriorityQueue
import java.util.concurrent.ThreadLocalRandom

import com.google.common.collect.HashBiMap
import com.typesafe.scalalogging.StrictLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.apache.arrow.vector.util.TransferPair
import org.apache.arrow.vector.{FieldVector, IntVector}
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.ArrowAttributeReader.ArrowDateReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.util.control.NonFatal

/**
  * Builds up dictionaries and write record batches. Dictionaries are encoded as deltas
  * to minimize redundant messages.
  *
  * @param sft simple feature type
  * @param dictionaryFields dictionary fields
  * @param encoding simple feature encoding
  * @param sort sort
  * @param initialCapacity initial allocation size, will expand if needed
  */
class DeltaWriter(
    val sft: SimpleFeatureType,
    dictionaryFields: Seq[String],
    encoding: SimpleFeatureEncoding,
    ipcOpts: IpcOption,
    sort: Option[(String, Boolean)],
    initialCapacity: Int
  ) extends Closeable with StrictLogging {

  import DeltaWriter._

  import scala.collection.JavaConverters._

  private val allocator = ArrowAllocator("delta-writer")

  // threading key that we use to group results in the reduce phase
  private var threadingKey: Long = math.abs(ThreadLocalRandom.current().nextLong)
  logger.trace(s"$threadingKey created")

  private val result = new ByteArrayOutputStream

  private val vector = StructVector.empty(sft.getTypeName, allocator)

  private val ordering = sort.map { case (field, reverse) =>
    val o = SimpleFeatureOrdering(sft.indexOf(field))
    if (reverse) { o.reverse } else { o }
  }

  private val idWriter = ArrowAttributeWriter.id(sft, encoding, vector)

  private val writers = sft.getAttributeDescriptors.asScala.map { descriptor =>
    val name = descriptor.getLocalName
    val bindings = ObjectType.selectType(descriptor)
    val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))

    if (dictionaryFields.contains(name)) {
      val dictMetadata = Map(SimpleFeatureVector.DescriptorKey -> s"$name:Integer")
      val attribute = ArrowAttributeWriter(name, Seq(ObjectType.INT), None, dictMetadata, encoding, VectorFactory(vector))
      val dictionary = {
        val attribute = ArrowAttributeWriter(name, bindings, None, metadata, encoding, VectorFactory(allocator))
        val writer = new BatchWriter(attribute.vector, ipcOpts)
        attribute.vector.setInitialCapacity(initialCapacity)
        attribute.vector.allocateNew()
        Some(DictionaryWriter(sft.indexOf(name), attribute, writer, scala.collection.mutable.Map.empty))
      }
      FieldWriter(name, sft.indexOf(name), attribute, dictionary)
    } else {
      val attribute = ArrowAttributeWriter(name, bindings, None, metadata, encoding, VectorFactory(vector))
      FieldWriter(name, sft.indexOf(name), attribute, None)
    }
  }

  // writer per-dictionary
  private val dictionaryWriters = dictionaryFields.map(f => writers.find(_.name == f).get.dictionary.get)

  // single writer to write out all vectors at once (not including dictionaries)
  private val writer = new BatchWriter(vector, ipcOpts)

  // set capacity after all child vectors have been created by the writers, then allocate
  vector.setInitialCapacity(initialCapacity)
  vector.allocateNew()

  /**
    * Clear any existing dictionary values
    */
  def reset(): Unit = {
    val last = threadingKey
    threadingKey = math.abs(ThreadLocalRandom.current().nextLong)
    logger.trace(s"$last resetting to $threadingKey")
    writers.foreach(writer => writer.dictionary.foreach(_.values.clear()))
  }

  /**
    * Writes out a record batch. Format is:
    *
    * 8 bytes long - threading key
    * (foreach dictionaryField) -> {
    *   4 byte int - length of dictionary batch
    *   anyref vector batch with dictionary delta values
    * }
    * 4 byte int - length of record batch
    * record batch (may be dictionary encodings)
    *
    * Note: will sort the feature array in place if sorting is defined
    *
    * @param features features to write
    * @param count number of features to write, starting from array index 0
    * @return serialized record batch
    */
  def encode(features: Array[SimpleFeature], count: Int): Array[Byte] = {

    result.reset()
    result.write(ByteArrays.toBytes(threadingKey))

    ordering.foreach(java.util.Arrays.sort(features, 0, count, _))

    // write out the dictionaries

    // come up with the delta of new dictionary values
    val delta = new java.util.TreeSet[AnyRef](dictionaryOrdering)

    dictionaryWriters.foreach { dictionary =>
      var i = 0
      while (i < count) {
        val value = features(i).getAttribute(dictionary.index)
        if (!dictionary.values.contains(value)) {
          delta.add(value)
        }
        i += 1
      }
      val size = dictionary.values.size
      i = 0
      // update the dictionary mappings, and write the new values to the vector
      delta.asScala.foreach { n =>
        dictionary.values.put(n, i + size)
        dictionary.attribute.apply(i, n)
        i += 1
      }
      // write out the dictionary batch
      dictionary.attribute.setValueCount(i)
      logger.trace(s"$threadingKey writing dictionary delta with $i values")
      dictionary.writer.writeBatch(i, result)
      delta.clear()
    }

    // set feature ids in the vector
    if (encoding.fids.isDefined) {
      var i = 0
      while (i < count) {
        idWriter.apply(i, features(i))
        i += 1
      }
      idWriter.setValueCount(count)
    }

    // set attributes in the vector
    writers.foreach { writer =>
      val getAttribute: Int => AnyRef = writer.dictionary match {
        case None =>             i => features(i).getAttribute(writer.index)
        case Some(dictionary) => i => dictionary.values(features(i).getAttribute(writer.index)) // dictionary encoded value
      }
      var i = 0
      while (i < count) {
        writer.attribute.apply(i, getAttribute(i))
        i += 1
      }
      writer.attribute.setValueCount(count)
    }

    logger.trace(s"$threadingKey writing batch with $count values")

    // write out the vector batch
    writer.writeBatch(count, result)

    result.toByteArray
  }

  /**
    * Close the writer
    */
  override def close(): Unit = {
    CloseWithLogging(writer) // also closes `vector`
    dictionaryWriters.foreach(w => CloseWithLogging(w.writer)) // also closes dictionary vectors
    CloseWithLogging(allocator)
  }
}

object DeltaWriter extends StrictLogging {

  import scala.collection.JavaConverters._

  // empty provider
  private val provider = new MapDictionaryProvider()

  private val dictionaryOrdering: Ordering[AnyRef] = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  /**
   * Reduce function for delta records created by DeltaWriter
   *
   * @param sft simple feature type
   * @param dictionaryFields dictionary fields
   * @param encoding simple feature encoding
   * @param sort sort metadata, if defined each delta is assumed to be sorted
   * @param sorted whether features are already globally sorted or not
   * @param batchSize batch size
   * @param deltas output from `DeltaWriter.encode`
   * @return single arrow streaming file, with potentially multiple record batches
   */
  def reduce(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      sorted: Boolean,
      batchSize: Int,
      deltas: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    new ReducingIterator(sft, dictionaryFields, encoding, ipcOpts, sort, sorted, batchSize, deltas)
  }

  /**
   * Merge without sorting
   *
   * @param sft simple feature type
   * @param dictionaryFields dictionary fields
   * @param encoding simple feature encoding
   * @param mergedDictionaries merged dictionaries and batch mappings
   * @param sort sort metadata for file headers
   * @param batchSize record batch size
   * @param threadedBatches record batches, grouped by threading key
   * @return
   */
  private def reduceNoSort(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      mergedDictionaries: MergedDictionaries,
      sort: Option[(String, Boolean)],
      batchSize: Int,
      threadedBatches: Array[Array[Array[Byte]]]): CloseableIterator[Array[Byte]] = {

    val iter: CloseableIterator[Array[Byte]] = new CloseableIterator[Array[Byte]] {
      private var writeHeader = true
      private val toLoad = SimpleFeatureVector.create(sft, mergedDictionaries.dictionaries, encoding, batchSize)
      private val result = SimpleFeatureVector.create(sft, mergedDictionaries.dictionaries, encoding, batchSize)
      logger.trace(s"merge unsorted deltas - read schema ${result.underlying.getField}")
      private val loader = new RecordBatchLoader(toLoad.underlying)
      private val unloader = new RecordBatchUnloader(result, ipcOpts)

      private val transfers: Seq[(String, (Int, Int, java.util.Map[Integer, Integer]) => Unit)] = {
        toLoad.underlying.getChildrenFromFields.asScala.map { fromVector =>
          val name = fromVector.getField.getName
          val toVector = result.underlying.getChild(name)
          val transfer: (Int, Int, java.util.Map[Integer, Integer]) => Unit =
            if (fromVector.getField.getDictionary != null) {
              val from = fromVector.asInstanceOf[IntVector]
              val to = toVector.asInstanceOf[IntVector]
              (fromIndex: Int, toIndex: Int, mapping: java.util.Map[Integer, Integer]) => {
                val n = from.getObject(fromIndex)
                if (n == null) {
                  to.setNull(toIndex)
                } else {
                  to.setSafe(toIndex, mapping.get(n))
                }
              }
            } else if (sft.indexOf(name) != -1 &&
                classOf[Geometry].isAssignableFrom(sft.getDescriptor(name).getType.getBinding)) {
              // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
              val from = GeometryFields.wrap(fromVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              val to = GeometryFields.wrap(toVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              (fromIndex: Int, toIndex: Int, _: java.util.Map[Integer, Integer]) => {
                from.transfer(fromIndex, toIndex, to)
              }
            } else {
              val pair = fromVector.makeTransferPair(toVector)
              (fromIndex: Int, toIndex: Int, _: java.util.Map[Integer, Integer]) => {
                pair.copyValueSafe(fromIndex, toIndex)
              }
            }
          (name, transfer)
        }
      }

      private val threadIterator = threadedBatches.iterator
      private var threadIndex = -1
      private var batches: Iterator[Array[Byte]] = Iterator.empty
      private var mappings: Map[String, java.util.Map[Integer, Integer]] = _
      private var count = 0 // records read in current batch

      override def hasNext: Boolean = count < toLoad.reader.getValueCount || loadNextBatch()

      override def next(): Array[Byte] = {
        var total = 0
        while (total < batchSize && hasNext) {
          // read the rest of the current vector, up to the batch size
          val toRead = math.min(batchSize - total, toLoad.reader.getValueCount - count)
          transfers.foreach { case (name, transfer) =>
            val mapping = mappings.get(name).orNull
            logger.trace(s"dictionary mappings for $name: $mapping")
            var i = 0
            while (i < toRead) {
              transfer(i + count, i + total, mapping)
              i += 1
            }
          }
          count += toRead
          total += toRead
        }

        if (writeHeader) {
          // write the header in the first result, which includes the metadata and dictionaries
          writeHeader = false
          writeHeaderAndFirstBatch(result, mergedDictionaries.dictionaries, ipcOpts, sort, total)
        } else {
          unloader.unload(total)
        }
      }

      override def close(): Unit = CloseWithLogging.raise(Seq(toLoad, result, mergedDictionaries))

      /**
        * Read the next batch
        *
        * @return true if there was a batch to load, false if we've read all batches
        */
      @tailrec
      private def loadNextBatch(): Boolean = {
        if (batches.hasNext) {
          val batch = batches.next
          // skip the dictionary batches
          var offset = 8 // initial threading key offset
          dictionaryFields.foreach { _ =>
            offset += ByteArrays.readInt(batch, offset) + 4
          }
          val messageLength = ByteArrays.readInt(batch, offset)
          offset += 4 // skip over message length bytes
          // load the record batch
          loader.load(batch, offset, messageLength)
          if (toLoad.reader.getValueCount > 0) {
            count = 0 // reset count for this batch
            true
          } else {
            loadNextBatch()
          }
        } else if (threadIterator.hasNext) {
          threadIndex += 1
          // set the mappings for this thread
          mappings = mergedDictionaries.mappings.map { case (f, m) => (f, m(threadIndex)) }
          batches = threadIterator.next.iterator
          loadNextBatch()
        } else {
          false
        }
      }
    }

    createFileFromBatches(sft, mergedDictionaries.dictionaries, encoding, ipcOpts, None, iter, firstBatchHasHeader = true)
  }

  /**
    * Merge with sorting. Each batch is assumed to be already sorted
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding simple feature encoding
    * @param mergedDictionaries merged dictionaries and batch mappings
    * @param sortBy sort field
    * @param reverse reverse sort or not
    * @param batchSize record batch size
    * @param threadedBatches record batches, grouped by threading key, internally sorted
    * @return
    */
  private def reduceWithSort(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      mergedDictionaries: MergedDictionaries,
      sortBy: String,
      reverse: Boolean,
      batchSize: Int,
      threadedBatches: Array[Array[Array[Byte]]]): CloseableIterator[Array[Byte]] = {

    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichArray

    val dictionaries = mergedDictionaries.dictionaries

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val unloader = new RecordBatchUnloader(result, ipcOpts)

    logger.trace(s"merging sorted deltas - read schema: ${result.underlying.getField}")

    // we do a merge sort of each batch
    // queue sorted by current value in each batch
    val queue = {
      val ordering = if (reverse) { BatchMergerOrdering.reverse } else { BatchMergerOrdering }
      new PriorityQueue[BatchMerger[Any]](ordering)
    }

    // track our open vectors to close later
    val cleanup = ArrayBuffer.empty[SimpleFeatureVector]
    cleanup.sizeHint(threadedBatches.foldLeft(0)((sum, a) => sum + a.length))

    threadedBatches.foreachIndex { case (batches, batchIndex) =>
      val mappings = mergedDictionaries.mappings.map { case (f, m) => (f, m(batchIndex)) }
      logger.trace(s"loading ${batches.length} batch(es) from a single thread")

      batches.foreach { batch =>
        val toLoad = SimpleFeatureVector.create(sft, dictionaries, encoding)
        val loader = new RecordBatchLoader(toLoad.underlying)
        cleanup += toLoad

        // skip the dictionary batches
        var offset = 8
        dictionaryFields.foreach { _ =>
          offset += ByteArrays.readInt(batch, offset) + 4
        }
        val messageLength = ByteArrays.readInt(batch, offset)
        offset += 4 // skip the length bytes
        // load the record batch
        loader.load(batch, offset, messageLength)
        if (toLoad.reader.getValueCount > 0) {
          val transfers: Seq[(Int, Int) => Unit] = toLoad.underlying.getChildrenFromFields.asScala.map { fromVector =>
            val name = fromVector.getField.getName
            val toVector = result.underlying.getChild(name)
            if (fromVector.getField.getDictionary != null) {
              val mapping = mappings(name)
              val to = toVector.asInstanceOf[IntVector]
              (fromIndex: Int, toIndex: Int) => {
                val n = fromVector.getObject(fromIndex).asInstanceOf[Integer]
                if (n == null) {
                  to.setNull(toIndex)
                } else {
                  to.setSafe(toIndex, mapping.get(n))
                }
              }
            } else if (sft.indexOf(name) != -1 &&
                classOf[Geometry].isAssignableFrom(sft.getDescriptor(name).getType.getBinding)) {
              // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
              val from = GeometryFields.wrap(fromVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              val to = GeometryFields.wrap(toVector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              (fromIndex: Int, toIndex: Int) => from.transfer(fromIndex, toIndex, to)
            } else {
              val transfer = fromVector.makeTransferPair(toVector)
              (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
            }
          }
          val mapVector = toLoad.underlying
          val dict = dictionaries.get(sortBy)
          val merger = ArrowAttributeReader(sft.getDescriptor(sortBy), mapVector.getChild(sortBy), dict, encoding) match {
            case r: ArrowDictionaryReader => new DictionaryBatchMerger(toLoad, transfers, r, mappings.get(sortBy).orNull)
            case r: ArrowDateReader => new DateBatchMerger(toLoad, transfers, r)
            case r => new AttributeBatchMerger(toLoad, transfers, r)
          }
          queue.add(merger.asInstanceOf[BatchMerger[Any]])
        }
      }
    }

    var writtenHeader = false

    // gets the next record batch to write - returns null if no further records
    def nextBatch(): Array[Byte] = {
      if (queue.isEmpty) { null } else {
        result.clear()
        var resultIndex = 0
        // copy the next sorted value and then queue and sort the next element out of the batch we copied from
        do {
          val next = queue.remove()
          if (next.transfer(resultIndex)) {
            queue.add(next)
          }
          result.underlying.setIndexDefined(resultIndex)
          resultIndex += 1
        } while (!queue.isEmpty && resultIndex < batchSize)

        if (writtenHeader) {
          unloader.unload(resultIndex)
        } else {
          // write the header in the first result, which includes the metadata and dictionaries
          writtenHeader = true
          writeHeaderAndFirstBatch(result, dictionaries, ipcOpts, Some(sortBy -> reverse), resultIndex)
        }
      }
    }

    val merged: CloseableIterator[Array[Byte]] = new CloseableIterator[Array[Byte]] {

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

      override def close(): Unit = {
        CloseWithLogging(result)
        CloseWithLogging(cleanup)
        CloseWithLogging(mergedDictionaries)
      }
    }

    createFileFromBatches(sft, dictionaries, encoding, ipcOpts, Some(sortBy -> reverse), merged, firstBatchHasHeader = true)
  }

  /**
    * Merge delta dictionary batches
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param deltas Seq of threaded dictionary deltas
    * @return
    */
  private def mergeDictionaries(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      deltas: Array[Array[Array[Byte]]],
      encoding: SimpleFeatureEncoding): MergedDictionaries = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichArray

    val allocator = ArrowAllocator("merge-dictionaries")

    if (dictionaryFields.isEmpty) {
      return MergedDictionaries(Map.empty, Map.empty, allocator)
    }

    // calculate our vector bindings/metadata once up front
    val vectorMetadata = dictionaryFields.toArray.map { name =>
      val descriptor = sft.getDescriptor(name)
      val bindings = ObjectType.selectType(descriptor)
      val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
      val factory = VectorFactory(allocator)
      (name, bindings, metadata, factory)
    }

    // create a vector for each dictionary field
    def createNewVectors: Array[ArrowAttributeReader] = {
      vectorMetadata.map { case (name, bindings, metadata, factory) =>
        // use the writer to create the appropriate child vector
        val vector = ArrowAttributeWriter(name, bindings, None, metadata, encoding, factory).vector
        ArrowAttributeReader(bindings, vector, None, encoding)
      }
    }

    // final results
    val results = createNewVectors

    // re-used queue, gets emptied after each dictionary field
    // batch state is tracked in the DictionaryMerger instances
    val queue = new PriorityQueue[DictionaryMerger](Ordering.ordered[DictionaryMerger])

    // merge each threaded delta vector into a single dictionary for that thread
    var batch = -1
    val allMerges: Array[DictionaryMerger] = deltas.map { deltas =>
      // deltas are threaded batches containing partial dictionary vectors
      batch += 1

      // per-dictionary vectors for our final merged results for this threaded batch
      val dictionaries = createNewVectors

      // tracks the offset for each dictionary, based on the deltas that came before it
      val offsets = Array.fill(dictionaries.length)(0)

      // the delta vectors, each sorted internally
      val toMerge: Array[DictionaryMerger] = deltas.map { bytes =>
        val vectors = createNewVectors // per-dictionary vectors from this batch

        var i = 0
        var offset = 8 // start after threading key
        while (i < dictionaries.length) {
          val length = ByteArrays.readInt(bytes, offset)
          offset += 4 // increment past length
          if (length > 0) {
            new RecordBatchLoader(vectors(i).vector).load(bytes, offset, length)
            offset += length
          }
          i += 1
        }
        logger.trace(s"dictionary deltas: ${vectors.map(v => Seq.tabulate(v.getValueCount)(v.apply).mkString(",")).mkString(";")}")

        // copy the current dictionary offsets to account for previous batches
        val off = Array.ofDim[Int](offsets.length)
        System.arraycopy(offsets, 0, off, 0, offsets.length)

        val transfers = vectors.mapWithIndex { case (v, j) =>
          offsets(j) += v.getValueCount // note: side-effect in map - update our offsets for the next batch
          v.vector.makeTransferPair(dictionaries(j).vector)
        }

        new DictionaryMerger(vectors, transfers, off, null, -1) // we don't care about the batch number here
      }

      val transfers = Array.ofDim[TransferPair](dictionaries.length)
      val mappings = Array.fill(dictionaries.length)(HashBiMap.create[Integer, Integer]())

      var i = 0 // dictionary field index
      while (i < dictionaries.length) {
        // set initial values in the sorting queue
        toMerge.foreach { merger =>
          if (merger.setCurrent(i)) {
            queue.add(merger)
          }
        }

        var count = 0
        while (!queue.isEmpty) {
          val merger = queue.remove()
          merger.transfer(count)
          mappings(i).put(merger.offset, count)
          if (merger.advance()) {
            queue.add(merger)
          }
          count += 1
        }
        dictionaries(i).vector.setValueCount(count)
        transfers(i) = dictionaries(i).vector.makeTransferPair(results(i).vector)
        i += 1
      }

      new DictionaryMerger(dictionaries, transfers, Array.empty, mappings, batch)
    }

    // now merge the separate threads together

    // final mappings - we build up a new map as otherwise we'd get key/value overlaps
    // dictionary(batch(mapping))
    val mappings = Array.fill(results.length)(Array.fill(allMerges.length)(new java.util.HashMap[Integer, Integer]()))

    results.foreachIndex { case (result, i) =>
      allMerges.foreach { merger =>
        if (merger.setCurrent(i)) {
          queue.add(merger)
        }
      }

      var count = 0
      while (!queue.isEmpty) {
        val merger = queue.remove()
        // check for duplicates
        if (count == 0 || result.apply(count - 1) != merger.value) {
          merger.transfer(count)
          count += 1
        }
        // update the dictionary mapping from the per-thread to the global dictionary
        logger.trace(s"remap ${merger.value} ${merger.batch} ${merger.mappings(i)} ${merger.index} -> ${count - 1}")
        val remap = merger.remap
        if (remap != null) {
          mappings(i)(merger.batch).put(remap, count - 1)
        }
        if (merger.advance()) {
          queue.add(merger)
        }
      }
      result.vector.setValueCount(count)
    }

    // convert from indexed arrays to dictionary-field-keyed maps
    val dictionaryBuilder = Map.newBuilder[String, ArrowDictionary]
    dictionaryBuilder.sizeHint(dictionaryFields.length)
    val mappingsBuilder = Map.newBuilder[String, Array[java.util.Map[Integer, Integer]]]
    mappingsBuilder.sizeHint(dictionaryFields.length)

    vectorMetadata.foreachIndex { case ((name, bindings, _, _), i) =>
      logger.trace("merged dictionary: " + Seq.tabulate(results(i).getValueCount)(results(i).apply).mkString(","))
      val enc = new DictionaryEncoding(i, true, new ArrowType.Int(32, true))
      dictionaryBuilder += name -> ArrowDictionary.create(enc, results(i).vector, bindings, encoding)
      mappingsBuilder += name -> mappings(i).asInstanceOf[Array[java.util.Map[Integer, Integer]]]
    }

    val dictionaryMap = dictionaryBuilder.result()
    val mappingsMap = mappingsBuilder.result()

    logger.trace(s"batch dictionary mappings: ${mappingsMap.mapValues(_.mkString(",")).mkString(";")}")
    MergedDictionaries(dictionaryMap, mappingsMap, allocator)
  }

  // holder for merged dictionaries and mappings from written values to merged values
  private case class MergedDictionaries(
      dictionaries: Map[String, ArrowDictionary],
      mappings: Map[String, Array[java.util.Map[Integer, Integer]]],
      allocator: BufferAllocator
    ) extends Closeable {
    override def close(): Unit = {
      dictionaries.foreach { case (_, d) => CloseWithLogging(d) }
      CloseWithLogging(allocator)
    }
  }

  private case class FieldWriter(
      name: String,
      index: Int,
      attribute: ArrowAttributeWriter,
      dictionary: Option[DictionaryWriter] = None
    )

  private case class DictionaryWriter(
      index: Int,
      attribute: ArrowAttributeWriter,
      writer: BatchWriter,
      values: scala.collection.mutable.Map[AnyRef, Integer]
    )

  private object BatchMergerOrdering extends Ordering[BatchMerger[Any]] {
    override def compare(x: BatchMerger[Any], y: BatchMerger[Any]): Int = x.compare(y)
  }

  /**
   * Tracks sorted merging of delta record batches
   *
   * @param vector vector for this batch
   * @param transfers transfer functions to the result batch
   * @tparam T type param
   */
  private abstract class BatchMerger[T](
      vector: SimpleFeatureVector,
      transfers: Seq[(Int, Int) => Unit]
    ) extends Ordered[T] {

    protected var index: Int = 0

    def transfer(to: Int): Boolean = {
      transfers.foreach(_.apply(index, to))
      index += 1
      if (vector.reader.getValueCount > index) {
        load()
        true
      } else {
        false
      }
    }

    protected def load(): Unit
  }

  /**
   * Batch merger for dictionary-encoded values
   *
   * @param vector vector for this batch
   * @param transfers transfer functions to the result batch
   * @param sort vector holding the values being sorted on
   * @param dictionaryMappings mappings from the batch to the global dictionary
   */
  private class DictionaryBatchMerger(
      vector: SimpleFeatureVector,
      transfers: Seq[(Int, Int) => Unit],
      sort: ArrowDictionaryReader,
      dictionaryMappings: java.util.Map[Integer, Integer]
    ) extends BatchMerger[DictionaryBatchMerger](vector, transfers) {

    private var value: Int = dictionaryMappings.get(sort.getEncoded(0))

    override protected def load(): Unit = {
      // since we've sorted the dictionaries, we can just compare the encoded index values
      value = dictionaryMappings.get(sort.getEncoded(index))
    }

    override def compare(that: DictionaryBatchMerger): Int = java.lang.Integer.compare(value, that.value)
  }

  /**
   * Merger for date values. We can avoid allocating a Date object and just compare the millisecond timestamp
   *
   * @param vector vector for this batch
   * @param transfers transfer functions to the result batch
   * @param sort vector holding the values being sorted on
   */
  private class DateBatchMerger(
      vector: SimpleFeatureVector,
      transfers: Seq[(Int, Int) => Unit],
      sort: ArrowDateReader
    ) extends BatchMerger[DateBatchMerger](vector, transfers) {

    private var value: Long = sort.getTime(0)

    override protected def load(): Unit = value = sort.getTime(index)

    override def compare(that: DateBatchMerger): Int = java.lang.Long.compare(value, that.value)
  }

  /**
   * Generic batch merger for non-specialized attribute types
   *
   * @param vector vector for this batch
   * @param transfers transfer functions to the result batch
   * @param sort vector holding the values being sorted on
   */
  private class AttributeBatchMerger(
      vector: SimpleFeatureVector,
      transfers: Seq[(Int, Int) => Unit],
      sort: ArrowAttributeReader
    ) extends BatchMerger[AttributeBatchMerger](vector, transfers) {

    private var value: Comparable[Any] = sort.apply(0).asInstanceOf[Comparable[Any]]

    override protected def load(): Unit = value = sort.apply(index).asInstanceOf[Comparable[Any]]

    override def compare(that: AttributeBatchMerger): Int = SimpleFeatureOrdering.nullCompare(value, that.value)
  }

  /**
   * Dictionary merger for tracking threaded delta batches. Each member variable is an array, with
   * one entry per dictionary field
   *
   * @param readers attribute readers for the dictionary values
   * @param transfers transfers for the dictionary vectors
   * @param offsets dictionary offsets based on the number of threaded delta batches
   * @param mappings mappings from the local threaded batch dictionary to the global dictionary
   * @param batch the batch number
   */
  class DictionaryMerger(
      readers: Array[ArrowAttributeReader],
      transfers: Array[TransferPair],
      offsets: Array[Int],
      val mappings: Array[HashBiMap[Integer, Integer]],
      val batch: Int
    ) extends Ordered[DictionaryMerger] {

    private var current: Int = 0
    private var _index: Int = 0
    private var _value: Comparable[Any] = _

    /**
     * The read position of the current dictionary
     *
     * @return
     */
    def index: Int = _index

    /**
     * The current dictionary value
     *
     * @return
     */
    def value: Comparable[Any] = _value

    /**
     * The global offset of the current dictionary, based on the batch threading and the current read position
     *
     * @return
     */
    def offset: Int = offsets(current) + _index

    /**
     * Set the current dictionary to operate on, and reads the first value
     *
     * @param i dictionary index
     * @return true if the dictionary has any values to read
     */
    def setCurrent(i: Int): Boolean = {
      current = i
      _index = -1
      advance()
    }

    /**
     * Transfer the current dictionary/value to a new vector
     *
     * @param to destination index to transfer to
     */
    def transfer(to: Int): Unit = transfers(current).copyValueSafe(_index, to)

    /**
     * Get the reverse global mapping for the current dictionary and value
     *
     * @return
     */
    def remap: Integer = mappings(current).inverse().get(_index)

    /**
     * Read the next value from the current dictionary. Closes the current dictionary if there are no more values.
     *
     * @return true if there are more values
     */
    def advance(): Boolean = {
      _index += 1
      if (readers(current).getValueCount > _index) {
        _value = readers(current).apply(_index).asInstanceOf[Comparable[Any]]
        true
      } else {
        _value = null
        CloseWithLogging(readers(current).vector)
        false
      }
    }

    override def compare(that: DictionaryMerger): Int = SimpleFeatureOrdering.nullCompare(_value, that._value)
  }

  /**
    * Writes out a 4-byte int with the batch length, then a single batch
    *
    * @param vector vector
    */
  private class BatchWriter(vector: FieldVector, ipcOpts: IpcOption) extends Closeable {

    private val root = createRoot(vector)
    private val os = new ByteArrayOutputStream()
    private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os), ipcOpts)
    writer.start() // start the writer - we'll discard the metadata later, as we only care about the record batches

    logger.trace(s"write schema: ${vector.getField}")

    def writeBatch(count: Int, to: OutputStream): Unit = {
      os.reset()
      if (count < 1) {
        logger.trace("writing 0 bytes")
        to.write(ByteArrays.toBytes(0))
      } else {
        vector.setValueCount(count)
        root.setRowCount(count)
        writer.writeBatch()
        logger.trace(s"writing ${os.size} bytes")
        to.write(ByteArrays.toBytes(os.size()))
        os.writeTo(to)
      }
    }

    override def close(): Unit = {
      CloseWithLogging(writer)
      CloseWithLogging(root) // also closes the vector
    }
  }

  private class ReducingIterator(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      sorted: Boolean,
      batchSize: Int,
      deltas: CloseableIterator[Array[Byte]]
    ) extends CloseableIterator[Array[Byte]] {

    private lazy val reduced = {
      try {
        val grouped = scala.collection.mutable.Map.empty[Long, scala.collection.mutable.ArrayBuilder[Array[Byte]]]
        while (deltas.hasNext) {
          val delta = deltas.next
          grouped.getOrElseUpdate(ByteArrays.readLong(delta), Array.newBuilder) += delta
        }
        val threaded = Array.ofDim[Array[Array[Byte]]](grouped.size)
        var i = 0
        grouped.foreach { case (_, builder) => threaded(i) = builder.result; i += 1 }
        logger.trace(s"merging delta batches from ${threaded.length} thread(s)")
        val dictionaries = mergeDictionaries(sft, dictionaryFields, threaded, encoding)
        if (sorted || sort.isEmpty) {
          reduceNoSort(sft, dictionaryFields, encoding, ipcOpts, dictionaries, sort, batchSize, threaded)
        } else {
          val Some((s, r)) = sort
          reduceWithSort(sft, dictionaryFields, encoding, ipcOpts, dictionaries, s, r, batchSize, threaded)
        }
      } catch {
        case NonFatal(e) =>
          // if we get an error, re-throw it on next()
          new CloseableIterator[Array[Byte]] {
            override def hasNext: Boolean = true
            override def next(): Array[Byte] = throw e
            override def close(): Unit = {}
          }
      }
    }

    override def hasNext: Boolean = reduced.hasNext

    override def next(): Array[Byte] = reduced.next()

    override def close(): Unit = CloseWithLogging(deltas, reduced)
  }
}
