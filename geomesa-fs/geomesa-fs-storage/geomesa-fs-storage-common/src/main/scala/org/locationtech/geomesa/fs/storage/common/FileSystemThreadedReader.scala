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
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object FileSystemThreadedReader {

  def apply(
      readers: Iterator[(FileSystemPathReader, Iterator[Path])],
      threads: Int): CloseableIterator[SimpleFeature] = {
    if (threads < 2) {
      CloseableIterator(readers).flatMap { case (factory, paths) => CloseableIterator(paths).flatMap(factory.read) }
    } else {
      new MultiThreadedFileSystemReader(readers, threads)
    }
  }

  class MultiThreadedFileSystemReader(readers: Iterator[(FileSystemPathReader, Iterator[Path])], threads: Int)
      extends CloseableIterator[SimpleFeature] with StrictLogging {

    private val es = Executors.newFixedThreadPool(threads)

    private val queue = new LinkedBlockingQueue[SimpleFeature](2000000)

    private val localQueue = new java.util.LinkedList[SimpleFeature]()

    private var current: SimpleFeature = _

    readers.foreach { case (reader, paths) =>
      paths.foreach { path =>
        val runnable = new Runnable {
          override def run(): Unit = {
            var count = 0
            try {
              logger.debug(s"Reading file $path")
              WithClose(reader.read(path)) { features =>
                while (features.hasNext) {
                  // need to copy the feature as it can be re-used
                  queue.put(ScalaSimpleFeature.copy(features.next()))
                  count += 1
                }
              }
              logger.debug(s"File $path produced $count records")
            } catch {
              case NonFatal(e) => logger.error(s"Error reading file $path", e)
            }
          }
        }
        es.submit(runnable)
      }
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
  }
}
