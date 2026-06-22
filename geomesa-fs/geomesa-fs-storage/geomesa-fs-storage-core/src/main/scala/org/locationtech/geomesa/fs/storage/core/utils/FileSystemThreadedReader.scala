/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.utils

import com.typesafe.scalalogging.StrictLogging
import org.apache.iceberg.DataFile
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.PhaserUtils
import org.locationtech.geomesa.utils.io.WithClose

import java.net.URI
import java.util.concurrent._
import scala.util.control.NonFatal

/**
  * Multi-threaded file reads
  *
  * @param es executor service used by the read tasks
  * @param phaser phaser for tracking read task completion
  * @param queue intermediate queue populated by the read tasks
  */
class FileSystemThreadedReader private (es: ExecutorService, phaser: Phaser, queue: BlockingQueue[SimpleFeature])
    extends CloseableIterator[SimpleFeature] {

  private val localQueue = new java.util.LinkedList[SimpleFeature]()

  private var current: SimpleFeature = _

  override def hasNext: Boolean = {
    if (current != null) {
      return true
    }
    current = localQueue.pollFirst
    if (current != null) {
      return true
    }

    while (!phaser.isTerminated) {
      current = queue.poll(100, TimeUnit.MILLISECONDS)
      if (current != null) {
        queue.drainTo(localQueue, 10000)
        return true
      }
    }
    // last check - if phaser.isTerminated, the queue should have whatever values are left
    current = queue.poll()
    if (current != null) {
      queue.drainTo(localQueue, 10000)
      true
    } else {
      false
    }
  }

  override def next(): SimpleFeature = {
    if (hasNext) {
      val ret = current
      current = null
      ret
    } else {
      Iterator.empty.next
    }
  }

  override def close(): Unit = {
    try { es.shutdownNow() } finally {
      phaser.forceTermination() // unregister any tasks that didn't start
    }
  }
}

object FileSystemThreadedReader extends StrictLogging {

  /**
   * Extract file action from DataFile location
   */
  private def fileContent(file: DataFile): org.apache.iceberg.FileContent = {
    // Use Iceberg's native content type instead of filename conventions
    val content = file.content()
    if (content == null) {
      // For backward compatibility with files created before Iceberg migration,
      // check filename prefix
      val location = file.location()
      val filename = location.substring(location.lastIndexOf('/') + 1)
      filename.take(2) match {
        case "w_" | "c_" => org.apache.iceberg.FileContent.DATA
        case "m_" => org.apache.iceberg.FileContent.DATA // modifications are still data files
        case "d_" => org.apache.iceberg.FileContent.EQUALITY_DELETES
        case _ if filename.startsWith("data_") => org.apache.iceberg.FileContent.DATA
        case _ if filename.startsWith("eq-del_") => org.apache.iceberg.FileContent.EQUALITY_DELETES
        case _ if filename.startsWith("pos-del_") => org.apache.iceberg.FileContent.POSITION_DELETES
        case _ => org.apache.iceberg.FileContent.DATA // default to data
      }
    } else {
      content
    }
  }

  /**
   * Determines if a file needs sequential processing (i.e., it contains modifications/deletes)
   */
  private def isModifyingFile(file: DataFile): Boolean = {
    val content = fileContent(file)
    content == org.apache.iceberg.FileContent.EQUALITY_DELETES ||
    content == org.apache.iceberg.FileContent.POSITION_DELETES ||
    {
      // For backward compatibility: check if this is a modification file (old m_ prefix)
      val location = file.location()
      val filename = location.substring(location.lastIndexOf('/') + 1)
      filename.startsWith("m_")
    }
  }

  def apply(reader: FileSystemPathReader, files: Seq[DataFile], threads: Int): CloseableIterator[SimpleFeature] = {
    if (threads < 2) {
      val mods = new java.util.HashSet[String]()
      // ensure files are sorted in reverse chronological order so mods are handled correctly
      // DataFile doesn't have timestamp, but Iceberg maintains order by sequence number
      CloseableIterator.wrap(files).flatMap(f => read(reader, f, mods))
    } else {
      val queue = new LinkedBlockingQueue[SimpleFeature](2000000)
      val es = Executors.newFixedThreadPool(threads)

      val phaser = new Phaser(1) {
        override protected def onAdvance(phase: Int, registeredParties: Int): Boolean = {
          // when all tasks have been completed, shutdown the executor service
          es.shutdown()
          true // return true to indicate the phaser should terminate
        }
      }

      try {
        def submitParallelRead(toRead: Seq[DataFile]): Unit = {
          // ensure that we don't register too many parties on this phaser
          toRead.grouped(PhaserUtils.MaxParties - 1).foreach { group =>
            val child = new Phaser(phaser)
            child.register() // register new task
            es.submit(new ChainedReaderTask(es, child, reader, group, Seq.empty, queue))
          }
        }

        // if a partitions has modifications, it must be read separately to ensure they're handled correctly
        val partitionsWithMods = files.collect {
          case f if isModifyingFile(f) =>
            f.partition() // Iceberg DataFile has partition() method
        }.distinct
        if (partitionsWithMods.isEmpty) {
         submitParallelRead(files)
        } else {
          files.groupBy(_.partition()).foreach { case (partition, group) =>
            if (!partitionsWithMods.contains(partition)) {
              submitParallelRead(group)
            } else {
              val chain = Seq.newBuilder[Seq[DataFile]]
              var remaining = group // already in sequence order from Iceberg
              var i = remaining.indexWhere(f => isModifyingFile(f))
              while (i != -1) {
                i = math.min(i, PhaserUtils.MaxParties - 1) // ensure that we don't register too many parties on this phaser
                val (first, rest) = remaining.splitAt(i + 1)
                chain += first
                remaining = rest
                i = remaining.indexWhere(f => isModifyingFile(f))
              }
              if (remaining.nonEmpty) {
                chain += remaining
              }
              val groups = chain.result()
              val child = new Phaser(phaser)
              child.register() // register new task
              es.submit(new ChainedReaderTask(es, child, reader, groups.head, groups.tail, queue))
            }
          }
        }
      } catch {
        case NonFatal(e) => es.shutdownNow(); throw e
      } finally {
        phaser.arriveAndDeregister()
      }

      new FileSystemThreadedReader(es, phaser, queue)
    }
  }

  /**
    * Reads a file
    *
    * @param reader reader
    * @param file file to read
    * @param mods collection to track modifications
    * @return
    */
  private def read(reader: FileSystemPathReader, file: DataFile, mods: java.util.Set[String]): CloseableIterator[SimpleFeature] = {
    val uri = new URI(file.location())
    val content = fileContent(file)

    // Check for legacy modification files (m_ prefix) - these are data files that need special handling
    val location = file.location()
    val filename = location.substring(location.lastIndexOf('/') + 1)
    val isLegacyModification = filename.startsWith("m_")

    if (content == org.apache.iceberg.FileContent.EQUALITY_DELETES ||
        content == org.apache.iceberg.FileContent.POSITION_DELETES) {
      // True delete file - read IDs and add to mods set
      new DeletingReaderIterator(reader, uri, mods)
    } else if (isLegacyModification) {
      // Legacy modification file - read features and add IDs to mods set
      new ModifyingReaderIterator(reader, uri, mods)
    } else {
      // Regular data file - read features, skipping any in mods set
      new AppendingReaderIterator(reader, uri, mods)
    }
  }

  /**
    * Performs a set of chained (dependent) reads. The groups must be handled serially, but the files within a
    * given group can be read in parallel.
    *
    * @param es executor service for submitting new read tasks
    * @param phaser phaser to track run lifecycle
    * @param reader reader
    * @param group current group of files that can be read in parallel
    * @param chain remaining groups of files that must be read sequentially
    * @param queue result queue
    * @param mods modifications/deletes for this group of files
    */
  private class ChainedReaderTask(
      es: ExecutorService,
      phaser: Phaser,
      reader: FileSystemPathReader,
      group: Seq[DataFile],
      chain: Seq[Seq[DataFile]],
      queue: BlockingQueue[SimpleFeature],
      mods: java.util.Set[String] = new java.util.HashSet[String]()
    ) extends Runnable {

    override def run(): Unit = {
      val child = new Phaser(1) {
        override protected def onAdvance(phase: Int, registeredParties: Int): Boolean = {
          // when this group is done, submit the next group for processing
          try {
            if (chain.nonEmpty) {
              phaser.register() // register new task
              es.submit(new ChainedReaderTask(es, phaser, reader, chain.head, chain.tail, queue, mods))
            }
            true // return true to indicate the phaser should terminate
          } finally {
            phaser.arriveAndDeregister()
          }
        }
      }
      try {
        group.foreach { file =>
          child.register() // register new task
          es.submit(new ReaderTask(child, queue, file.location(), read(reader, file, mods)))
        }
      } finally {
        child.arriveAndDeregister()
      }
    }
  }

  /**
    * Task to run a reader and add the results to a result queue
    *
    * @param phaser phaser for tracking completion of this task
    * @param queue result queue
    * @param file file path (for logging)
    * @param iter lazily evaluated iterator for reading the path
    */
  private class ReaderTask(
      phaser: Phaser,
      queue: BlockingQueue[SimpleFeature],
      file: String,
      iter: => CloseableIterator[SimpleFeature]
    ) extends Runnable {

    override def run(): Unit = {
      try {
        WithClose(iter)(_.foreach(queue.put))
      } catch {
        case NonFatal(e) => logger.error(s"Error reading file $file", e)
      } finally {
        phaser.arriveAndDeregister()
      }
    }
  }

  /**
    * Reads a file, skipping features marked as modified
    *
    * @param reader reader
    * @param file file path
    * @param mods set of modified feature IDs that shouldn't be returned
    */
  private class AppendingReaderIterator(reader: FileSystemPathReader, file: URI, mods: java.util.Set[String])
      extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading file $file")

    private var count = 0

    private val delegate = {
      val iter = CloseableIterator.wrap(reader.read(file))
      if (mods.isEmpty) {
        iter
      } else {
        iter.filter { f => if (mods.contains(f.getID)) { count += 1; false } else { true } }
      }
    }

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = {
      count += 1
      delegate.next()
    }

    override def close(): Unit = {
      logger.debug(s"File $file produced $count records")
      delegate.close()
    }
  }

  /**
    * Reads a file, skipping features marked as modified
    *
    * @param reader reader
    * @param file file path
    * @param mods set of modified feature IDs that shouldn't be returned
    */
  private class ModifyingReaderIterator(reader: FileSystemPathReader, file: URI, mods: java.util.Set[String])
      extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading modifying file $file")

    private var count = 0

    private val delegate = CloseableIterator.wrap(reader.read(file)).filter { f => count += 1; mods.add(f.getID) }

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = delegate.next()

    override def close(): Unit = {
      logger.debug(s"File $file produced $count modified records")
      delegate.close()
    }
  }

  /**
    * Reads a file, tracking deletes
    *
    * @param reader reader
    * @param file file path
    * @param mods deleted feature ids will be added here
    */
  private class DeletingReaderIterator(reader: FileSystemPathReader, file: URI, mods: java.util.Set[String])
      extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading deleting file $file")

    try {
      var count = 0
      WithClose(reader.read(file)) { features =>
        while (features.hasNext) {
          mods.add(features.next().getID)
          count += 1
        }
      }
      logger.debug(s"File $file produced $count deleted records")
    } catch {
      case NonFatal(e) => logger.error(s"Error reading deleting file $file", e)
    }

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = Iterator.empty.next()

    override def close(): Unit = {}
  }
}
