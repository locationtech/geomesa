/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.util.control.NonFatal

object FileSystemThreadedReader {

  def apply(factory: FileSystemPathReader, paths: Iterator[Path], threads: Int): FileSystemReader = {
    if (threads < 2) {
      new SingleThreadedFileSystemReader(factory, paths)
    } else {
      new MultiThreadedFileSystemReader(factory, paths, threads)
    }
  }

  class SingleThreadedFileSystemReader(factory: FileSystemPathReader, paths: Iterator[Path])
      extends FileSystemReader {

    private val iters = paths.map(factory.read)
    private var iter: CloseableIterator[SimpleFeature] = CloseableIterator.empty

    @tailrec
    override final def hasNext: Boolean = {
      iter.hasNext || {
        CloseWithLogging(iter)
        if (!iters.hasNext) {
          iter = CloseableIterator.empty
          false
        } else {
          iter = iters.next
          hasNext
        }
      }
    }

    override def next(): SimpleFeature = iter.next()
    override def close(): Unit = iter.close()
    override def close(wait: Long, unit: TimeUnit): Boolean = {
      close()
      true
    }
  }

  class MultiThreadedFileSystemReader(factory: FileSystemPathReader, paths: Iterator[Path], threads: Int)
      extends FileSystemReader with StrictLogging {

    private val es = Executors.newFixedThreadPool(threads)

    private val queue = new LinkedBlockingQueue[SimpleFeature](2000000)

    private val localQueue = new java.util.LinkedList[SimpleFeature]()

    private var current: SimpleFeature = _

    paths.foreach { file =>
      val runnable = new Runnable {
        override def run(): Unit = {
          var count = 0
          try {
            logger.debug(s"Reading file $file")
            WithClose(factory.read(file)) { reader =>
              while (reader.hasNext) {
                // need to copy the feature as it can be re-used
                queue.put(ScalaSimpleFeature.copy(reader.next()))
                count += 1
              }
            }
            logger.debug(s"File $file produced $count records")
          } catch {
            case NonFatal(e) => logger.error(s"Error reading file $file", e)
          }
        }
      }
      es.submit(runnable)
    }
    es.shutdown()

    override def hasNext: Boolean = {
      if (current != null) {
        return true
      }
      current = localQueue.pollFirst
      if (current != null) {
        return true
      }
      while (!es.isTerminated) {
        current = queue.poll(100, TimeUnit.MILLISECONDS)
        if (current != null) {
          queue.drainTo(localQueue, 10000)
          return true
        }
      }
      // last check - if es.isTerminated, the queue should have whatever values are left
      current = queue.poll()
      if (current != null) {
        queue.drainTo(localQueue, 10000)
        true
      } else {
        false
      }
    }

    override def next(): SimpleFeature = {
      if (current == null) {
        Iterator.empty.next
      } else {
        val ret = current
        current = null
        ret
      }
    }

    override def close(): Unit = es.shutdownNow()

    override def close(wait: Long, unit: TimeUnit): Boolean = {
      close()
      es.awaitTermination(wait, unit)
    }
  }
}
