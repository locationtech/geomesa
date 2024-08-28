/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import com.typesafe.scalalogging.StrictLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.apache.arrow.vector.util.TransferPair
import org.apache.arrow.vector.{FieldVector, IntVector}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.jts.{GeometryFields, GeometryVector}
import org.locationtech.geomesa.arrow.vector.ArrowAttributeReader.{ArrowDateReader, ArrowDictionaryReader, ArrowListDictionaryReader}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.ArrowDictionaryArray
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{AttributeOrdering, ObjectType, SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.jts.geom.Geometry

import java.io.{ByteArrayOutputStream, Closeable, OutputStream}
import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom
import java.util.{Collections, PriorityQueue}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
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
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val allocator = ArrowAllocator(s"delta-writer:${sft.getTypeName}")

  // threading key that we use to group results in the reduce phase
  private var threadingKey: Long = math.abs(ThreadLocalRandom.current().nextLong)
  logger.trace(s"$threadingKey created")

  private val result = new ByteArrayOutputStream

  private val vector = StructVector.empty(sft.getTypeName, allocator)

  private val ordering = sort.map { case (field, reverse) => SimpleFeatureOrdering(sft, field, reverse) }

  private val idWriter = ArrowAttributeWriter.id(sft, encoding, vector)

  private val writers = sft.getAttributeDescriptors.asScala.map { descriptor =>
    val name = descriptor.getLocalName
    val isList = descriptor.isList
    val bindings = ObjectType.selectType(descriptor)
    val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
    if (dictionaryFields.contains(name)) {
      // for list types, we encode the list items, not the whole list
      val attribute = {
        val desc = if (isList) { s"$name:List[Integer]" } else { s"$name:Integer" }
        val encodedMetadata = Map(SimpleFeatureVector.DescriptorKey -> desc)
        val encodedBindings = if (isList) { Seq(ObjectType.LIST, ObjectType.INT) } else { Seq(ObjectType.INT) }
        ArrowAttributeWriter(name, encodedBindings, None, encodedMetadata, encoding, VectorFactory(vector))
      }
      // for list types, get the list item binding (which is the tail of the bindings)
      val dictBindings = if (isList) { bindings.tail } else { bindings }
      val ordering = AttributeOrdering(dictBindings)
      val dict = ArrowAttributeWriter(name, dictBindings, None, metadata, encoding, VectorFactory(allocator))
      dict.vector.setInitialCapacity(initialCapacity)
      dict.vector.allocateNew()
      if (isList) {
        new DictionaryListFieldWriter(sft.indexOf(name), attribute, dict, ordering, ipcOpts)
      } else {
        new DictionaryFieldWriter(sft.indexOf(name), attribute, dict, ordering, ipcOpts)
      }
    } else {
      val attribute = ArrowAttributeWriter(name, bindings, None, metadata, encoding, VectorFactory(vector))
      new AttributeFieldWriter(sft.indexOf(name), attribute)
    }
  }

  // writer per-dictionary - keep in dictionary field order
  private val dictionaryWriters = dictionaryFields.flatMap { field =>
    writers.collectFirst { case d: DictionaryFieldWriter if d.attribute.name == field => d }
  }

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
    dictionaryWriters.foreach(_.resetDictionary())
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

    dictionaryWriters.foreach { dictionary =>
      var i = 0
      while (i < count) {
        dictionary.addDictionaryValue(features(i))
        i += 1
      }
      val delta = dictionary.writeDictionaryDelta(result)
      logger.trace(s"$threadingKey writing dictionary delta with $delta values")
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
      var i = 0
      while (i < count) {
        writer.write(i, features(i))
        i += 1
      }
      writer.setValueCount(count)
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
    CloseWithLogging(dictionaryWriters) // also closes dictionary vectors
    CloseWithLogging(allocator)
  }
}

object DeltaWriter extends StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  // empty provider
  private val Provider = new MapDictionaryProvider()

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
    reduce(sft, dictionaryFields, encoding, ipcOpts, sort, sorted, batchSize, process = true, deltas)
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
   * @param process process the output into a valid arrow file (which usually requires reading the entire input stream)
   * @return single arrow streaming file, with potentially multiple record batches, or raw delta batches
   */
  def reduce(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      sorted: Boolean,
      batchSize: Int,
      process: Boolean,
      deltas: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    if (process) {
      new ReducingIterator(sft, dictionaryFields, encoding, ipcOpts, sort, sorted, batchSize, deltas)
    } else {
      new RawIterator(sft, dictionaryFields, encoding, ipcOpts, sort, deltas)
    }
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
            if (mergedDictionaries.dictionaries.contains(name)) {
              (fromVector, toVector) match {
                case (from: IntVector, to: IntVector) =>
                  (fromIndex: Int, toIndex: Int, mapping: java.util.Map[Integer, Integer]) => {
                    val n = from.getObject(fromIndex)
                    if (n == null) {
                      to.setNull(toIndex)
                    } else {
                      to.setSafe(toIndex, mapping.get(n))
                    }
                  }

                case (from: ListVector, to: ListVector) =>
                  val innerTo = to.getDataVector.asInstanceOf[IntVector]
                  (fromIndex: Int, toIndex: Int, mapping: java.util.Map[Integer, Integer]) => {
                    // note: should not be nulls as they are converted to empty list
                    val list = from.getObject(fromIndex).asInstanceOf[java.util.List[Integer]]
                    if (to.getLastSet >= toIndex) {
                      to.setLastSet(toIndex - 1)
                    }
                    val start = to.startNewValue(toIndex)
                    var offset = 0
                    while (offset < list.size()) {
                      // note: nulls get encoded in the dictionary
                      innerTo.setSafe(start + offset, mapping.get(list.get(offset)))
                      offset += 1
                    }
                    to.endValue(toIndex, offset)
                  }

                case _ =>
                  throw new IllegalStateException(s"Encountered unexpected dictionary vector: $fromVector")
              }
            } else if (sft.indexOf(name) != -1 &&
                classOf[Geometry].isAssignableFrom(sft.getDescriptor(name).getType.getBinding)) {
              // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
              val binding = sft.getDescriptor(name).getType.getBinding
              val from = GeometryFields.wrap(fromVector, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              val to = GeometryFields.wrap(toVector, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
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
        }.toSeq
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
        var i = 0
        while (i < count) {
          result.underlying.setIndexDefined(i)
          i += 1
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
            if (dictionaries.contains(name)) {
              val mapping = mappings(name)
              (fromVector, toVector) match {
                case (from: IntVector, to: IntVector) =>
                  (fromIndex: Int, toIndex: Int) => {
                    val n = from.getObject(fromIndex)
                    if (n == null) {
                      to.setNull(toIndex)
                    } else {
                      to.setSafe(toIndex, mapping.get(n))
                    }
                  }

                case (from: ListVector, to: ListVector) =>
                  val innerTo = to.getDataVector.asInstanceOf[IntVector]
                  (fromIndex: Int, toIndex: Int) => {
                    // note: should not be nulls as they are converted to empty list
                    val list = from.getObject(fromIndex).asInstanceOf[java.util.List[Integer]]
                    if (to.getLastSet >= toIndex) {
                      to.setLastSet(toIndex - 1)
                    }
                    val start = to.startNewValue(toIndex)
                    var offset = 0
                    while (offset < list.size()) {
                      // note: nulls get encoded in the dictionary
                      innerTo.setSafe(start + offset, mapping.get(list.get(offset)))
                      offset += 1
                    }
                    to.endValue(toIndex, offset)
                  }

                case _ =>
                  throw new IllegalStateException(s"Encountered unexpected dictionary vector: $fromVector")
              }
            } else if (sft.indexOf(name) != -1 &&
                classOf[Geometry].isAssignableFrom(sft.getDescriptor(name).getType.getBinding)) {
              // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
              val binding = sft.getDescriptor(name).getType.getBinding
              val from = GeometryFields.wrap(fromVector, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              val to = GeometryFields.wrap(toVector, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
              (fromIndex: Int, toIndex: Int) => from.transfer(fromIndex, toIndex, to)
            } else {
              val transfer = fromVector.makeTransferPair(toVector)
              (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
            }
          }.toSeq
          val mapVector = toLoad.underlying
          val dict = dictionaries.get(sortBy)
          val descriptor = sft.getDescriptor(sortBy)
          val merger = ArrowAttributeReader(descriptor, mapVector.getChild(sortBy), dict, encoding) match {
            case r: ArrowDictionaryReader => new DictionaryBatchMerger(toLoad, transfers, r, mappings.get(sortBy).orNull)
            case r: ArrowListDictionaryReader => new DictionaryListBatchMerger(toLoad, transfers, r, mappings.get(sortBy).orNull)
            case r: ArrowDateReader => new DateBatchMerger(toLoad, transfers, r)
            case r => new AttributeBatchMerger(toLoad, transfers, r, AttributeOrdering(descriptor))
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
        while ({
          {
            val next = queue.remove()
            if (next.transfer(resultIndex)) {
              queue.add(next)
            }
            result.underlying.setIndexDefined(resultIndex)
            resultIndex += 1
          }; !queue.isEmpty && resultIndex < batchSize
        })()

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

    val allocator = ArrowAllocator(s"merge-dictionaries:${sft.getTypeName}")

    if (dictionaryFields.isEmpty) {
      return MergedDictionaries(Map.empty, Map.empty, allocator)
    }

    // calculate our vector bindings/metadata once up front
    val vectorMetadata = dictionaryFields.toArray.map { name =>
      val descriptor = sft.getDescriptor(name)
      val bindings = {
        val base = ObjectType.selectType(descriptor)
        // for list types, get the list item binding (which is the tail of the bindings)
        if (descriptor.isList) { base.tail } else { base }
      }
      val ordering = AttributeOrdering(bindings)
      val desc = if (descriptor.isList) { s"$name:${descriptor.getListType().getSimpleName}" } else {
        SimpleFeatureTypes.encodeDescriptor(sft, descriptor)
      }
      val metadata = Map(SimpleFeatureVector.DescriptorKey -> desc)
      val factory = VectorFactory(allocator)
      (name, bindings, ordering, metadata, factory)
    }

    // create a vector for each dictionary field
    def createNewVectors: Array[ArrowAttributeReader] = {
      vectorMetadata.map { case (name, bindings, _, metadata, factory) =>
        // use the writer to create the appropriate child vector
        val vector = ArrowAttributeWriter(name, bindings, None, metadata, encoding, factory).vector
        ArrowAttributeReader(bindings, vector, None, encoding)
      }
    }

    val orderings = vectorMetadata.map(_._3)

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

        new DictionaryMerger(vectors, transfers, off, orderings, null, -1) // we don't care about the batch number here
      }

      val transfers = Array.ofDim[TransferPair](dictionaries.length)
      val mappings = Array.fill(dictionaries.length)(new java.util.HashMap[Integer, Integer]())

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
          mappings(i).put(count, merger.offset)
          if (merger.advance()) {
            queue.add(merger)
          }
          count += 1
        }
        dictionaries(i).vector.setValueCount(count)
        transfers(i) = dictionaries(i).vector.makeTransferPair(results(i).vector)
        i += 1
      }

      new DictionaryMerger(dictionaries, transfers, Array.empty, orderings, mappings, batch)
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

    vectorMetadata.foreachIndex { case ((name, bindings, _, _, _), i) =>
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

  private sealed abstract class FieldWriter(val attribute: ArrowAttributeWriter) {
    def write(i: Int, f: SimpleFeature): Unit
    def setValueCount(i: Int): Unit = attribute.setValueCount(i)
  }

  private class AttributeFieldWriter(index: Int, attribute: ArrowAttributeWriter) extends FieldWriter(attribute) {
    override def write(i: Int, f: SimpleFeature): Unit = attribute.apply(i, f.getAttribute(index))
  }

  private class DictionaryFieldWriter(
      index: Int,
      attribute: ArrowAttributeWriter,
      dict: ArrowAttributeWriter,
      ordering: Ordering[AnyRef],
      ipcOpts: IpcOption
    ) extends FieldWriter(attribute) with Closeable {

    protected val values = scala.collection.mutable.Map.empty[AnyRef, Integer]
    protected val delta  = new java.util.TreeSet[AnyRef](ordering)
    protected val writer = new BatchWriter(dict.vector, ipcOpts)

    override def write(i: Int, f: SimpleFeature): Unit = attribute.apply(i, values(f.getAttribute(index)))

    def addDictionaryValue(f: SimpleFeature): Unit = {
      val value = f.getAttribute(index)
      if (!values.contains(value)) {
        delta.add(value)
      }
    }

    def writeDictionaryDelta(out: ByteArrayOutputStream): Int = {
      val size = values.size
      var i = 0
      // update the dictionary mappings, and write the new values to the vector
      delta.asScala.foreach { n =>
        values.put(n, i + size)
        dict.apply(i, n)
        i += 1
      }
      // write out the dictionary batch
      dict.setValueCount(i)
      writer.writeBatch(i, out)
      delta.clear()
      i
    }

    def resetDictionary(): Unit = values.clear()

    override def close(): Unit = writer.close()
  }

  private class DictionaryListFieldWriter(
      index: Int,
      attribute: ArrowAttributeWriter,
      dict: ArrowAttributeWriter,
      ordering: Ordering[AnyRef],
      ipcOpts: IpcOption
    ) extends DictionaryFieldWriter(index, attribute, dict, ordering, ipcOpts) {

    override def write(i: Int, f: SimpleFeature): Unit = {
      val list = f.getAttribute(index).asInstanceOf[java.util.List[AnyRef]]
      val encoded = if (list == null) { Collections.emptyList() } else {
        val e = new java.util.ArrayList[Integer](list.size)
        list.asScala.foreach(v => e.add(values(v)))
        e
      }
      attribute.apply(i, encoded)
    }

    override def addDictionaryValue(f: SimpleFeature): Unit = {
      val value = f.getAttribute(index).asInstanceOf[java.util.List[AnyRef]]
      if (value != null) {
        value.asScala.foreach { v =>
          if (!values.contains(v)) {
            delta.add(v)
          }
        }
      }
    }
  }

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
   * Batch merger for dictionary-encoded values
   *
   * @param vector vector for this batch
   * @param transfers transfer functions to the result batch
   * @param sort vector holding the values being sorted on
   * @param dictionaryMappings mappings from the batch to the global dictionary
   */
  private class DictionaryListBatchMerger(
      vector: SimpleFeatureVector,
      transfers: Seq[(Int, Int) => Unit],
      sort: ArrowListDictionaryReader,
      dictionaryMappings: java.util.Map[Integer, Integer]
    ) extends BatchMerger[DictionaryListBatchMerger](vector, transfers) {

    private var value: java.util.List[Integer] = {
      val list = sort.getEncoded(0)
      var i = 0
      while (i < list.size) {
        list.set(i, dictionaryMappings.get(list.get(i)))
        i += 1
      }
      list
    }

    override protected def load(): Unit = {
      // since we've sorted the dictionaries, we can just compare the encoded index values
      value = sort.getEncoded(index)
      var i = 0
      while (i < value.size) {
        value.set(i, dictionaryMappings.get(value.get(i)))
        i += 1
      }
    }

    override def compare(that: DictionaryListBatchMerger): Int =
      AttributeOrdering.IntListOrdering.compare(value, that.value)
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
      sort: ArrowAttributeReader,
      ordering: Ordering[AnyRef]
    ) extends BatchMerger[AttributeBatchMerger](vector, transfers) {

    private var value: AnyRef = sort.apply(0)

    override protected def load(): Unit = value = sort.apply(index)

    override def compare(that: AttributeBatchMerger): Int = ordering.compare(value, that.value)
  }

  /**
   * Dictionary merger for tracking threaded delta batches. Each member variable is an array, with
   * one entry per dictionary field
   *
   * @param readers attribute readers for the dictionary values
   * @param transfers transfers for the dictionary vectors
   * @param offsets dictionary offsets based on the number of threaded delta batches
   * @param orderings orderings for sorting the dictionary values
   * @param mappings mappings from the local threaded batch dictionary to the global dictionary
   * @param batch the batch number
   */
  private class DictionaryMerger(
      readers: Array[ArrowAttributeReader],
      transfers: Array[TransferPair],
      offsets: Array[Int],
      orderings: Array[Ordering[AnyRef]],
      val mappings: Array[java.util.HashMap[Integer, Integer]],
      val batch: Int
    ) extends Ordered[DictionaryMerger] {

    private var current: Int = 0
    private var _index: Int = 0
    private var _value: AnyRef = _

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
    def value: AnyRef = _value

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
    def remap: Integer = mappings(current).get(_index)

    /**
     * Read the next value from the current dictionary. Closes the current dictionary if there are no more values.
     *
     * @return true if there are more values
     */
    def advance(): Boolean = {
      _index += 1
      if (readers(current).getValueCount > _index) {
        _value = readers(current).apply(_index)
        true
      } else {
        _value = null
        CloseWithLogging(readers(current).vector)
        false
      }
    }

    override def compare(that: DictionaryMerger): Int = orderings(current).compare(_value, that._value)
  }

  /**
    * Writes out a 4-byte int with the batch length, then a single batch
    *
    * @param vector vector
    */
  private class BatchWriter(vector: FieldVector, ipcOpts: IpcOption) extends Closeable {

    private val root = createRoot(vector)
    private val os = new ByteArrayOutputStream()
    private val writer = new ArrowStreamWriter(root, Provider, Channels.newChannel(os), ipcOpts)
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

  private class RawIterator(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      deltas: CloseableIterator[Array[Byte]]
    ) extends CloseableIterator[Array[Byte]] {

    private lazy val reduced = {
      try {
        // write out an empty batch so that we get the header and dictionaries
        var i = -1L
        val dictionaries = dictionaryFields.map { name =>
          val descriptor = sft.getDescriptor(name)
          val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
          // delta dicts are encoded as int32s since we don't know the size up front
          val enc = new DictionaryEncoding({i += 1; i}, true, new ArrowType.Int(32, true))
          name -> new ArrowDictionaryArray(sft.getTypeName, enc, Array.empty, 0, binding.asInstanceOf[Class[AnyRef]])
        }.toMap
        val header = WithClose(SimpleFeatureVector.create(sft, dictionaries, encoding, 0)) { vector =>
          writeHeaderAndFirstBatch(vector, dictionaries, ipcOpts, sort, 0)
        }
        CloseWithLogging(dictionaries.values)
        val length = Array.ofDim[Byte](4)
        ByteArrays.writeInt(header.length, length)
        Iterator(length, header) ++ deltas
      } catch {
        case NonFatal(e) =>
          // if we get an error, re-throw it on next()
          new Iterator[Array[Byte]] {
            override def hasNext: Boolean = true
            override def next(): Array[Byte] = throw e
          }
      }
    }

    override def hasNext: Boolean = reduced.hasNext

    override def next(): Array[Byte] = reduced.next()

    override def close(): Unit = CloseWithLogging(deltas)
  }
}
