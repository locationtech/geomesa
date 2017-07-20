/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.util
import java.util.concurrent._

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, PartitionScheme}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class FileSystemFeatureIterator(fs: FileSystem,
                                partitionScheme: PartitionScheme,
                                sft: SimpleFeatureType,
                                q: Query,
                                readThreads: Int,
                                storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] with AutoCloseable {

  private val partitions = FsQueryPlanning.getPartitionsForQuery(storage, sft, q)

  private val iter: java.util.Iterator[SimpleFeature] with AutoCloseable =
    if (partitions.isEmpty) {
      JavaCloseableIterator
    } else {
      new ThreadedReader(storage, partitions, sft, q, readThreads)
    }

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override def close(): Unit = iter.close()
}

object JavaCloseableIterator extends java.util.Iterator[SimpleFeature] with AutoCloseable  {
  override def next(): SimpleFeature = throw new NoSuchElementException
  override def hasNext: Boolean = false
  override def close(): Unit = {}
}

class ThreadedReader(storage: FileSystemStorage,
                     partitions: Seq[String],
                     sft: SimpleFeatureType,
                     q: Query,
                     numThreads: Int)
  extends java.util.Iterator[SimpleFeature] with AutoCloseable with LazyLogging {

  // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
  // because Parquet is also threading the reads underneath I think. using prod/cons pattern was
  // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
  // cost of serializing simple features though. need to investigate more.
  //
  // However, if you are doing lots of filtering it appears that bumping the threads up high
  // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

  logger.debug(s"Threading the read of ${partitions.size} partitions with $numThreads reader threads (and 1 writer thread)")
  private val es = Executors.newFixedThreadPool(numThreads)
  private val latch = new CountDownLatch(partitions.size)

  private val queue = new LinkedBlockingQueue[SimpleFeature](2000000)

  private val localQueue = new util.LinkedList[SimpleFeature]()
  private var numQueued = 0

  private var started: Boolean = false
  private def start(): Unit = {
    partitions.foreach { p =>
      es.submit(new Runnable with LazyLogging {
        override def run(): Unit = {
          try { // For the latch must be careful with the threads and wrap this separately
            var count = 0
            val reader = storage.getPartitionReader(sft, q, p)
            try {
              logger.debug(s"Reading partition of ${reader.getPartition}")
              while (reader.hasNext) {
                count += 1
                val next = reader.next()
                queue.add(next)
              }
            } catch {
              case NonFatal(e) => logger.error(s"Error reading partition ${reader.getPartition}", e)
            } finally {
              try { reader.close() } catch { case NonFatal(e) => logger.error("error closing reader", e) }
              logger.debug(s"Partition ${reader.getPartition} produced $count records")
            }
          } finally {
            latch.countDown()
          }
        }
      })
    }
    es.shutdown()
    started = true
  }

  private var nextQueued: Boolean = false
  private var cur: SimpleFeature = _

  private def queueNext(): Unit = {
    if (!started) {
      start()
    }

    if (numQueued > 0) {
      cur = localQueue.pop()
      numQueued -= 1
    } else {
      while (cur == null && (queue.size() > 0 || latch.getCount > 0)) {
        val tmp = queue.poll(10, TimeUnit.MILLISECONDS)
        if (tmp != null) {
          cur = tmp
          numQueued += queue.drainTo(localQueue, 10000)
        }
      }
    }

    nextQueued = true
  }

  override def next(): SimpleFeature = {
    if (!nextQueued) { queueNext() }
    if (cur == null) {
      throw new NoSuchElementException
    }

    val ret = cur
    cur = null
    nextQueued = false

    ret
  }

  override def hasNext: Boolean = {
    if (!nextQueued) { queueNext() }
    cur != null
  }

  override def close(): Unit = {
    es.awaitTermination(5, TimeUnit.SECONDS)
    latch.await(10, TimeUnit.SECONDS)
  }
}