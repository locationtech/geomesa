/*
 *
 *  Copyright 2014 Commonwealth Computer Research, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the License);
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an AS IS BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.locationtech.geomesa.accumulo.stats

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{Connector, TableExistsException}
import org.locationtech.geomesa.accumulo.stats.StatWriter.TableInstance
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig

import scala.collection.JavaConversions._
import scala.collection.mutable

trait StatWriter {

  def connector: Connector

  // start the background thread
  if (!connector.isInstanceOf[MockConnector]) {
    StatWriter.startIfNeeded()
  }

  /**
   * Writes a stat to accumulo. This implementation adds the stat to a bounded queue, which should
   * be fast, then asynchronously writes the data in the background.
   *
   * @param stat
   */
  def writeStat(stat: Stat, statTable: String): Unit =
    StatWriter.queueStat(stat, TableInstance(connector, statTable))
}

/**
 * Singleton object to manage writing of stats in a background thread.
 */
object StatWriter extends Runnable with Logging {

  private val batchSize = 100

  private val writeDelayMillis = 1000

  private val batchWriterConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)

  // use the guava exiting executor so that this thread doesn't hold up the jvm shutdown
  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))

  private val running = new AtomicBoolean(false)

  // we need to use our own synchronization in order to synchronize the flush method and the queue draining
  private val lock = new ReentrantLock()
  private val notEmpty = lock.newCondition()
  // the queue and table cache should only be accessed when the lock is acquired
  private val queue = scala.collection.mutable.Queue.empty[StatToWrite]
  private val tableCache = scala.collection.mutable.Set.empty[TableInstance]

  sys.addShutdownHook {
    executor.shutdownNow()
  }

  /**
   * Starts the background thread for writing stats, if it hasn't already been started
   */
  private def startIfNeeded() = if (running.compareAndSet(false, true)) {
    // we want to wait between invocations to give more stats a chance to queue up
    executor.scheduleWithFixedDelay(this, writeDelayMillis, writeDelayMillis, TimeUnit.MILLISECONDS)
  }

  /**
   * Queues a stat for writing. We don't want to affect memory and accumulo performance too much...
   * if we exceed the queue size, we drop any further stats
   */
  private def queueStat(stat: Stat, table: TableInstance): Unit = {
    lock.lock()
    try {
      if (queue.length < batchSize) {
        queue.enqueue(StatToWrite(stat, table))
        notEmpty.signal() // notify the writing thread that there is data
      } else {
        logger.debug("Stat queue is full - stat being dropped")
      }
    } finally {
      lock.unlock()
    }
  }

  override def run() = {
    lock.lock()
    try {
      // wait for a stat to be queued
      while (queue.isEmpty) {
        notEmpty.await() // this gives up the lock but will re-acquire it before proceeding
      }
      drainQueue()
    } catch {
      case e: InterruptedException =>
        // normal thread termination, just propagate the interrupt
        Thread.currentThread().interrupt()
      case e: Exception =>
        logger.error("Error in stat writing - stopping stat writer thread", e)
        executor.shutdown()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Flush out any queued stats.
   */
  def flush(): Unit = {
    lock.lock()
    try {
      drainQueue()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Flush out any queued entries - should only be called if the current thread has acquired the lock.
   */
  private def drainQueue(): Unit = {
    // drain out all current stats that may have been queued
    val stats = if (queue.nonEmpty) queue.dequeueAll(_ => true) else Seq.empty
    if (stats.nonEmpty) {
      write(stats)
    }
  }

  /**
   * Writes the stats. This should be called only when the lock is acquired.
   */
  protected[stats] def write(statsToWrite: Seq[StatToWrite]): Unit =
    statsToWrite.groupBy(s => StatGroup(s.table, s.stat.getClass)).foreach { case (group, stats) =>
      // get the appropriate transform for this type of stat
      val transform = group.clas match {
        case c if c == classOf[QueryStat] => QueryStatTransform.asInstanceOf[StatTransform[Stat]]
        case _ => throw new RuntimeException("Not implemented")
      }
      // create the table if necessary
      if (tableCache.add(group.table)) {
        val tableOps = group.table.connector.tableOperations()
        if (!tableOps.exists(group.table.name)) {
          try {
            tableOps.create(group.table.name, true, TimeType.LOGICAL)
          } catch {
            case e: TableExistsException => // unlikely, but this can happen with multiple jvms
          }
        }
      }
      // write to the table
      val writer = group.table.connector.createBatchWriter(group.table.name, batchWriterConfig)
      try {
        writer.addMutations(stats.map(stw => transform.statToMutation(stw.stat)))
        writer.flush()
      } finally {
        writer.close()
      }
    }

  private[stats] case class StatToWrite(stat: Stat, table: TableInstance)

  private[stats] case class TableInstance(connector: Connector, name: String)

  private[stats] case class StatGroup(table: TableInstance, clas: Class[_ <: Stat])
}
