/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.utils

import com.typesafe.scalalogging.StrictLogging
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

  def apply(reader: FileSystemPathReader, files: Seq[URI], threads: Int): CloseableIterator[SimpleFeature] = {
    if (threads < 2) {
      CloseableIterator.wrap(files).flatMap(f => CloseableIterator.wrap(reader.read(f)))
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
        // ensure that we don't register too many parties on this phaser
        files.grouped(PhaserUtils.MaxParties - 1).foreach { group =>
          val child = new Phaser(phaser)
          child.register() // register new task
          es.submit(new ChainedReaderTask(es, child, reader, group, Seq.empty, queue))
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
      group: Seq[URI],
      chain: Seq[Seq[URI]],
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
          es.submit(new ReaderTask(child, queue, file, CloseableIterator.wrap(reader.read(file))))
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
      file: URI,
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
}
