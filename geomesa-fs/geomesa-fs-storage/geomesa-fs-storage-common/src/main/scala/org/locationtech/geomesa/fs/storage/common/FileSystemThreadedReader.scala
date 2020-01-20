/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.concurrent._

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{StorageFileAction, StorageFilePath}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature

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

  override def close(): Unit = es.shutdownNow()
}

object FileSystemThreadedReader extends StrictLogging {

  def apply(
      readers: Iterator[(FileSystemPathReader, Seq[StorageFilePath])],
      threads: Int): CloseableIterator[SimpleFeature] = {

    if (threads < 2) {
      readers.flatMap { case (reader, files) =>
        val mods = scala.collection.mutable.HashSet.empty[String]
        // ensure files are sorted in reverse chronological order
        CloseableIterator(files.sorted.iterator).flatMap(f => read(reader, f, mods))
      }
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
        readers.foreach { case (reader, files) =>
          // group our files by actions which can be parallelized
          val groups = scala.collection.mutable.ListBuffer.empty[Seq[StorageFilePath]]
          var group = scala.collection.mutable.ArrayBuffer.empty[StorageFilePath]
          // ensure files are sorted in reverse chronological order
          files.sorted.foreach { file =>
            if (file.file.action == StorageFileAction.Append) {
              group += file
            } else {
              if (group.nonEmpty) {
                groups += group
                group = scala.collection.mutable.ArrayBuffer.empty[StorageFilePath]
              }
              groups += Seq(file)
            }
          }
          if (group.nonEmpty) {
            groups += group // add the last group
          }

          es.submit(new ChainedReaderTask(es, phaser, reader, groups.head, groups.tail, queue))
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
  private def read(
      reader: FileSystemPathReader,
      file: StorageFilePath,
      mods: scala.collection.mutable.Set[String]): CloseableIterator[SimpleFeature] = {
    file.file.action match {
      case StorageFileAction.Append => new AppendingReaderIterator(reader, file.path, mods)
      case StorageFileAction.Modify => new ModifyingReaderIterator(reader, file.path, mods)
      case StorageFileAction.Delete => new DeletingReaderIterator(reader, file.path, mods)
      case _ => throw new NotImplementedError(s"Unexpected storage action: ${file.file.action}")
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
      group: Seq[StorageFilePath],
      chain: Seq[Seq[StorageFilePath]],
      queue: BlockingQueue[SimpleFeature],
      mods: scala.collection.mutable.Set[String] = scala.collection.mutable.HashSet.empty[String]
  ) extends Runnable {

    phaser.register()

    override def run(): Unit = {
      val child = new Phaser(1) {
        override protected def onAdvance(phase: Int, registeredParties: Int): Boolean = {
          // when this group is done, submit the next group for processing
          try {
            if (chain.nonEmpty) {
              es.submit(new ChainedReaderTask(es, phaser, reader, chain.head, chain.tail, queue, mods))
            }
            true // return true to indicate the phaser should terminate
          } finally {
            phaser.arriveAndDeregister()
          }
        }
      }
      try {
        group.foreach(file => es.submit(new ReaderTask(child, queue, file.path, read(reader, file, mods))))
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
    * @param path file path (for logging)
    * @param iter lazily evaluated iterator for reading the path
    */
  private class ReaderTask(
      phaser: Phaser,
      queue: BlockingQueue[SimpleFeature],
      path: Path,
      iter: => CloseableIterator[SimpleFeature]
    ) extends Runnable {

    phaser.register()

    override def run(): Unit = {
      try {
        iter.foreach(queue.put)
      } catch {
        case NonFatal(e) => logger.error(s"Error reading file $path", e)
      } finally {
        phaser.arriveAndDeregister()
      }
    }
  }

  /**
    * Reads a file, skipping features marked as modified
    *
    * @param reader reader
    * @param path file path
    * @param mods set of modified feature IDs that shouldn't be returned
    */
  private class AppendingReaderIterator(
      reader: FileSystemPathReader,
      path: Path,
      mods: scala.collection.Set[String]
    ) extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading file $path")

    private var count = 0

    private val delegate = {
      if (mods.isEmpty) {
        reader.read(path)
      } else {
        reader.read(path).filter { f => if (mods.contains(f.getID)) { count += 1; false } else { true } }
      }
    }

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = {
      count += 1
      // need to copy the feature as it can be re-used
      ScalaSimpleFeature.copy(delegate.next())
    }

    override def close(): Unit = {
      logger.debug(s"File $path produced $count records")
      delegate.close()
    }
  }


  /**
    * Reads a file, skipping features marked as modified
    *
    * @param reader reader
    * @param path file path
    * @param mods set of modified feature IDs that shouldn't be returned
    */
  private class ModifyingReaderIterator(
      reader: FileSystemPathReader,
      path: Path,
      mods: scala.collection.mutable.Set[String]
    ) extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading modifying file $path")

    private var count = 0

    private val delegate = reader.read(path).filter { f => count += 1; mods.add(f.getID) }

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = {
      // need to copy the feature as it can be re-used
      ScalaSimpleFeature.copy(delegate.next())
    }

    override def close(): Unit = {
      logger.debug(s"File $path produced $count modified records")
      delegate.close()
    }
  }

  /**
    * Reads a file, tracking deletes
    *
    * @param reader reader
    * @param path file path
    * @param mods deleted feature ids will be added here
    */
  private class DeletingReaderIterator(
      reader: FileSystemPathReader,
      path: Path,
      mods: scala.collection.mutable.Set[String]
    ) extends CloseableIterator[SimpleFeature] {

    logger.debug(s"Reading deleting file $path")

    try {
      var count = 0
      WithClose(reader.read(path)) { features =>
        while (features.hasNext) {
          mods.add(features.next().getID)
          count += 1
        }
      }
      logger.debug(s"File $path produced $count deleted records")
    } catch {
      case NonFatal(e) => logger.error(s"Error reading deleting file $path", e)
    }

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = Iterator.empty.next()

    override def close(): Unit = {}
  }
}
