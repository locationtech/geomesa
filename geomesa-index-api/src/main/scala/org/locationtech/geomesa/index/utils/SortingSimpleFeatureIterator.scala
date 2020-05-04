/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseQuietly, Sizable, WithClose}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

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
    if (closed.get || !features.hasNext) { features } else {
      val head = features.next()
      if (closed.get || !features.hasNext) { CloseableIterator.single(head, features.close()) } else {
        val ordering = SimpleFeatureOrdering(head.getFeatureType, sortBy)
        QueryProperties.SortMemoryThreshold.toBytes match {
          case None => sortInMemory(head, features, ordering, closed)
          case Some(threshold) => sortWithSpillover(head, features, ordering, closed, threshold)
        }
      }
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

  import scala.collection.JavaConverters._

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
    val list = new java.util.ArrayList[SimpleFeature](100)
    list.add(head)

    while (tail.hasNext && !closed.get) {
      list.add(tail.next())
    }

    if (closed.get) {
      // don't bother sorting, just return an empty iterator
      CloseableIterator(Iterator.empty, tail.close())
    } else {
      list.sort(ordering)
      CloseableIterator(list.iterator.asScala, tail.close())
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

    // grouping ID for tmp files, doesn't have to be unique
    lazy val id = {
      val name = head.getFeatureType.getTypeName.replaceAll("[^A-Za-z0-9_-]", "").take(20)
      f"$name-${Random.nextInt(10000)}%04d"
    }

    val sizable: FeatureIsSizable[SimpleFeature] = head match {
      case _: SimpleFeature with Sizable =>
        SizableFeatureIsSizable.asInstanceOf[FeatureIsSizable[SimpleFeature]]
      case _ =>
        // currently, all of our uses of this class pass in Sizable features... this means we missed one
        logger.warn(
          s"Feature class '${head.getClass.getName}' doesn't implement Sizable - " +
              "using estimated size for memory threshold calculations")
        UnSizableFeatureIsSizable
    }

    val files = ArrayBuffer.empty[File] // tmp files we create when we exceed in-memory threshold
    lazy val serializer = KryoFeatureSerializer(head.getFeatureType, SerializationOptions.withUserData)
    // use ArrayList for sort-in-place of the underlying array
    val list = new java.util.ArrayList[SimpleFeature](100)
    list.add(head)
    var size = sizable.sizeOf(head)

    while (tail.hasNext && !closed.get) {
      if (size >= threshold) {
        // write out the sorted list to disk
        list.sort(ordering)
        val file = Files.createTempFile(s"gm-sort-$id-", ".kryo").toFile
        files += file
        logger.trace(s"Created temp sort file '${file.getAbsolutePath}'")
        WithClose(new FileOutputStream(file)) { os =>
          var i = 0
          while (i < list.size()) {
            val bytes = serializer.serialize(list.get(i))
            os.write(ByteArrays.toBytes(bytes.length))
            os.write(bytes)
            i += 1
          }
        }
        list.clear()
        size = 0
      }
      val next = tail.next()
      list.add(next)
      size += sizable.sizeOf(next)
    }

    if (closed.get) {
      files.foreach { file =>
        try {
          if (!file.delete()) {
            logger.warn(s"Unable to delete tmp file '${file.getAbsolutePath}''")
            file.deleteOnExit()
          }
        } catch {
          case NonFatal(e) => logger.warn(s"Unable to delete tmp file '${file.getAbsolutePath}''", e)
        }
      }
      // don't bother sorting, just return an empty iterator
      CloseableIterator(Iterator.empty, tail.close())
    } else {
      if (!list.isEmpty) {
        list.sort(ordering)
      }
      if (files.isEmpty) {
        CloseableIterator(list.iterator.asScala, tail.close())
      } else {
        new MergeSortingIterator(files, serializer, list.iterator.asScala, tail, ordering)
      }
    }
  }

  /**
   * Does a merge sort on a group of locally sorted files and a left-over in-memory iterator
   *
   * @param files files to merge
   * @param serializer serializer
   * @param mem left-over in-memory features, already sorted
   * @param closeable link to the original (now-empty) feature iterator, for cleaning up on close
   * @param ordering feature ordering
   */
  private class MergeSortingIterator(
      files: IndexedSeq[File],
      serializer: SimpleFeatureSerializer,
      mem: Iterator[SimpleFeature],
      closeable: CloseableIterator[_],
      ordering: Ordering[SimpleFeature]
    ) extends CloseableIterator[SimpleFeature] {

    // list of feature iterators to merge
    private val merging = IndexedSeq(CloseableIterator(mem)) ++ files.map(new FileIterator(_, serializer))

    // priority queue containing the next feature from each file
    private val heads = {
      val o = Ordering.by[(SimpleFeature, Int), SimpleFeature](_._1)(ordering)
      val q = new java.util.PriorityQueue[(SimpleFeature, Int)](files.size + 1, o)
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

    override def close(): Unit = CloseQuietly.raise(merging :+ closeable)
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
        if (!file.delete()) {
          logger.warn(s"Unable to delete tmp file '${file.getAbsolutePath}''")
          file.deleteOnExit()
        }
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
