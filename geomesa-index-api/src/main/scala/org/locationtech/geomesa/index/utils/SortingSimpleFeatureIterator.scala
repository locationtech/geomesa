/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{SerializationOption, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseQuietly, Sizable, WithClose}

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, Future}
import scala.util.control.NonFatal
import scala.util.{Random, Try}

/**
  * In memory sorting of simple features, with optional spill to disk
  *
  * @param features unsorted feature iterator
  * @param sortBy attributes to sort by, in the form: (name, reverse).
  *               for sort by feature id (e.g. natural sort), use an empty string for name
  */
class SortingSimpleFeatureIterator(features: CloseableIterator[SimpleFeature], sortBy: Seq[(String, Boolean)])
    extends CloseableIterator[SimpleFeature] {

  import SortingSimpleFeatureIterator.{sortInMemory, sortWithSpillover}

  private val closed = new AtomicBoolean(false)

  private lazy val sorted: CloseableIterator[SimpleFeature] = {
    try {
      if (closed.get || !features.hasNext) { CloseableIterator.empty } else {
        val head = features.next()
        if (!features.hasNext) { CloseableIterator.single(head) } else {
          val ordering = SimpleFeatureOrdering(head.getFeatureType, sortBy)
          QueryProperties.SortMemoryThreshold.toBytes match {
            case None => sortInMemory(head, features, ordering, closed)
            case Some(threshold) => sortWithSpillover(head, features, ordering, closed, threshold)
          }
        }
      }
    } finally {
      features.close()
    }
  }

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      sorted.close() // also closes original features iterator
    }
  }
}

object SortingSimpleFeatureIterator extends LazyLogging {

  /**
   * Sorts the iterator in memory
   *
   * @param head head of iterator
   * @param tail tail of iterator
   * @param ordering ordering
   * @param closed checked to determine if we should stop sorting and return early
   * @return
   */
  private def sortInMemory(
      head: SimpleFeature,
      tail: CloseableIterator[SimpleFeature],
      ordering: Ordering[SimpleFeature],
      closed: AtomicBoolean): CloseableIterator[SimpleFeature] = {
    // use ArrayList for sort-in-place of the underlying array
    val list = new java.util.ArrayList[SimpleFeature](1000)
    list.add(head)

    while (tail.hasNext && !closed.get) {
      list.add(tail.next())
    }

    if (closed.get) {
      // don't bother sorting, just return an empty iterator
      CloseableIterator.empty
    } else {
      sort(list, ordering)
      CloseableIterator(list.iterator)
    }
  }

  /**
   * Sorts the iterator in memory, but spills over to disk if the size in memory exceeds the specified threshold
   *
   * @param head head of iterator
   * @param tail tail of iterator
   * @param ordering ordering
   * @param closed checked to determine if we should stop sorting and return early
   * @param threshold max memory to use before spilling to disk
   * @return
   */
  private def sortWithSpillover(
      head: SimpleFeature,
      tail: CloseableIterator[SimpleFeature],
      ordering: Ordering[SimpleFeature],
      closed: AtomicBoolean,
      threshold: Long): CloseableIterator[SimpleFeature] = {
    var sorter = new SpillToDiskSorter(head, ordering, threshold)
    try {
      while (tail.hasNext && !closed.get()) {
        sorter = sorter + tail.next()
      }
      sorter.result(closed.get)
    } catch {
      case NonFatal(e) =>
        sorter.cleanup()
        throw e
    }
  }

  /**
   * Sort a list in-place
   *
   * @param list list
   * @param ordering ordering
   */
  private def sort(list: java.util.ArrayList[SimpleFeature], ordering: Ordering[SimpleFeature]): Unit = {
    val start = System.nanoTime()
    list.sort(ordering)
    logger.debug(s"Sorted ${list.size()} features in ${(System.nanoTime() - start)/1000000}ms")
  }

  /**
   * Delete a file
   *
   * @param file file to delete
   */
  private def delete(file: File): Unit = {
    try {
      if (!file.delete()) {
        logger.warn(s"Unable to delete tmp file '${file.getAbsolutePath}''")
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Unable to delete tmp file '${file.getAbsolutePath}''", e)
    }
  }

  /**
   * Gets a sizeable implementation based on the feature class
   *
   * @param sf simple feature
   * @return
   */
  private def toSizeable(sf: SimpleFeature): FeatureIsSizable[SimpleFeature] = {
    sf match {
      case _: SimpleFeature with Sizable => SizableFeatureIsSizable.asInstanceOf[FeatureIsSizable[SimpleFeature]]
      case _ =>
        // currently, all of our uses of this class pass in Sizable features... this means we missed one
        logger.warn(
          s"Feature class '${sf.getClass.getName}' doesn't implement Sizable - " +
            "using estimated size for memory threshold calculations")
        UnSizableFeatureIsSizable
    }
  }

  /**
   * Class for sorting with spill-to-disk
   *
   * @param spill write/read to disk
   * @param sizer sizer for the features being sorted
   * @param ordering sort ordering
   * @param threshold memory threshold for dumping to disk, in bytes
   * @param files list of files that have been dumped to disk
   * @param pending a file that is asynchronously being dumped to disk, if any
   */
  private class SpillToDiskSorter(
      spill: SpillToDisk,
      sizer: FeatureIsSizable[SimpleFeature],
      ordering: Ordering[SimpleFeature],
      threshold: Long,
      files: Seq[File] = Seq.empty,
      pending: Option[Future[File]] = None) extends Callable[File] {

    private val start = System.nanoTime()
    private val mem = new java.util.ArrayList[SimpleFeature](1000)
    private var size: Long = 0

    /**
     * Create a new sorter
     *
     * @param sf simple feature, representative of the features being chunked
     * @param ordering ordering
     * @param threshold threshold for triggering a spill-to-disk, in bytes
     * @return
     */
    def this(sf: SimpleFeature, ordering: Ordering[SimpleFeature], threshold: Long) = {
      this(new SpillToDisk(sf.getFeatureType), toSizeable(sf), ordering, threshold)
      mem.add(sf)
      size += sizer.sizeOf(sf)
    }

    /**
     * Add the simple feature to the chunk, returning either this chunk or a new chunk
     *
     * @param sf feature
     * @return next chunk
     */
    def +(sf: SimpleFeature): SpillToDiskSorter = {
      if (size >= threshold) {
        logger.debug(s"Read ${mem.size()} features from the underlying store in ${(System.nanoTime() - start)/1000000}ms")
        // only allow one sort at a time so we don't blow up memory with all the array lists
        val last = pending.map { f =>
          if (f.isDone) { f.get() } else {
            val start = System.nanoTime()
            val file = f.get()
            logger.debug(s"Waited ${(System.nanoTime() - start)/1000000}ms for last batch to be flushed to disk")
            file
          }
        }
        val future = CachedThreadPool.call(this)
        new SpillToDiskSorter(spill, sizer, ordering, threshold, files ++ last, Some(future)) + sf
      } else {
        mem.add(sf)
        size += sizer.sizeOf(sf)
        this
      }
    }

    override def call(): File = {
      sort(mem, ordering)
      spill.write(mem)
    }

    /**
     * Get all the features added so far, in sorted order
     *
     * @return
     */
    def result(closed: Boolean): CloseableIterator[SimpleFeature] = {
      logger.debug(s"Read ${mem.size()} features from the underlying store in ${(System.nanoTime() - start)/1000000}ms")
      if (closed) {
        // don't bother sorting, just clean up and return an empty iterator
        cleanup()
        CloseableIterator.empty
      } else {
        sort(mem, ordering)
        val memIter = CloseableIterator(mem.iterator)
        if (files.isEmpty && pending.isEmpty) {
          memIter
        } else {
          val filesIter = (files ++ pending.map(_.get())).map(spill.read)
          new MergeSortingIterator(IndexedSeq(memIter) ++ filesIter, ordering)
        }
      }
    }

    /**
     * Delete any underlying resources. Should only be called if not calling `result` due to an error/interruption
     */
    def cleanup(): Unit = {
      pending.foreach(_.cancel(true))
      files.foreach(delete)
      Try(pending.map(_.get())).toOption.flatten.foreach(delete)
    }
  }

  /**
   * Class for writing and reading back from disk
   *
   * @param sft feature type
   */
  private class SpillToDisk(sft: SimpleFeatureType) {

    // grouping ID for tmp files, doesn't have to be unique
    private val id =f"${sft.getTypeName.replaceAll("[^A-Za-z0-9_-]", "").take(20)}-${Random.nextInt(10000)}%04d"

    private val serializer = KryoFeatureSerializer(sft, SerializationOption.WithUserData)

    def write(features: java.util.ArrayList[SimpleFeature]): File = {
      val start = System.nanoTime()
      val file = Files.createTempFile(s"gm-sort-$id-", ".kryo").toFile
      logger.trace(s"Created temp sort file '${file.getAbsolutePath}'")
      file.deleteOnExit()
      WithClose(new FileOutputStream(file)) { os =>
        features.forEach { sf =>
          val bytes = serializer.serialize(sf)
          os.write(ByteArrays.toBytes(bytes.length))
          os.write(bytes)
        }
      }
      logger.debug(s"Wrote ${features.size()} features to disk in ${(System.nanoTime() - start)/1000000}ms")
      file
    }

    def read(file: File): CloseableIterator[SimpleFeature] = new FileIterator(file, serializer)
  }

  /**
   * Does a merge sort on a group of locally sorted files and a left-over in-memory iterator
   *
   * @param merging list of feature iterators to merge, already sorted
   * @param ordering feature ordering
   */
  private class MergeSortingIterator(merging: IndexedSeq[CloseableIterator[SimpleFeature]], ordering: Ordering[SimpleFeature])
      extends CloseableIterator[SimpleFeature] {

    // priority queue containing the next feature from each file
    private val heads = {
      val o = Ordering.by[(SimpleFeature, Int), SimpleFeature](_._1)(ordering)
      val q = new java.util.PriorityQueue[(SimpleFeature, Int)](merging.size, o)
      var i = 0
      while (i < merging.length) {
        val m = merging(i)
        if (m.hasNext) {
          q.add(m.next -> i)
        }
        i += 1
      }
      q
    }

    override def hasNext: Boolean = !heads.isEmpty

    override def next(): SimpleFeature = {
      val (f, i) = heads.poll()
      val m = merging(i)
      if (m.hasNext) {
        heads.add(m.next -> i)
      }
      f
    }

    override def close(): Unit = CloseQuietly.raise(merging)
  }

  /**
   * Reads a tmp file of serialized simple features
   *
   * @param file file
   * @param serializer serializer
   */
  private class FileIterator(file: File, serializer: SimpleFeatureSerializer)
      extends CloseableIterator[SimpleFeature] with LazyLogging {

    private val is = new FileInputStream(file)
    private var buf = Array.ofDim[Byte](4)

    override def hasNext: Boolean = is.available() > 0

    override def next(): SimpleFeature = {
      is.read(buf, 0, 4)
      val size = ByteArrays.readInt(buf)
      if (buf.length < size) {
        buf = Array.ofDim((size * 1.2).toInt)
      }
      is.read(buf, 0, size)
      serializer.deserialize(buf, 0, size)
    }

    override def close(): Unit = {
      try { is.close() } finally {
        delete(file)
      }
    }
  }

  /**
   * Evidence that a feature is sizable
   *
   * @tparam T type binding
   */
  private trait FeatureIsSizable[T <: SimpleFeature] {
    def sizeOf(feature: T): Long
  }

  /**
   * Evidence that Sizable features are sizable
   */
  private object SizableFeatureIsSizable extends FeatureIsSizable[SimpleFeature with Sizable] {
    override def sizeOf(feature: SimpleFeature with Sizable): Long = feature.calculateSizeOf()
  }

  /**
   * Use a rough estimate based on attributes and user data
   */
  private object UnSizableFeatureIsSizable extends FeatureIsSizable[SimpleFeature] {
    override def sizeOf(feature: SimpleFeature): Long =
      Sizable.sizeOf(feature) + Sizable.deepSizeOf(feature.getAttributes, feature.getUserData)
  }
}
