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

package org.locationtech.geomesa.core.stats

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.collect.Queues
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector, TableExistsException}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait StatWriter {

  def connector: Connector

  // start the background thread
  if(!connector.isInstanceOf[MockConnector]) {
    StatWriter.startIfNeeded()
  }

  /**
   * Writes a stat to accumulo. This implementation adds the stat to a bounded queue, which should
   * be fast, then asynchronously writes the data in the background.
   *
   * @param stat
   */
  def writeStat(stat: Stat, statTable: String): Unit = StatWriter.queueStat(stat, statTable, connector)
}

/**
 * Singleton object to manage writing of stats in a background thread.
 */
object StatWriter extends Runnable with Logging {

  private val batchSize = 100

  private val writeDelayMillis = 1000

  private val batchWriterConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)

  // use the guava exiting executor so that this thread doesn't hold up the jvm shutdown
  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))

  private val running = new AtomicBoolean(false)

  private val queue = Queues.newLinkedBlockingQueue[StatToWrite](batchSize)

  private val tableCache = new mutable.HashMap[(Connector, String), Boolean]
                               with mutable.SynchronizedMap[(Connector, String), Boolean]

  sys.addShutdownHook {
    executor.shutdownNow()
  }

  /**
   * Starts the background thread for writing stats, if it hasn't already been started
   */
  private def startIfNeeded() {
    if (running.compareAndSet(false, true)) {
      // we want to wait between invocations to give more stats a chance to queue up
      executor.scheduleWithFixedDelay(this, writeDelayMillis, writeDelayMillis, TimeUnit.MILLISECONDS)
    }
  }

  private[stats] case class StatToWrite(stat: Stat, table: String, connector: Connector)

  /**
   * Queues a stat for writing. We don't want to affect memory and accumulo performance too much...
   * if we exceed the queue size, we drop any further stats
   *
   * @param stat
   */
  private def queueStat(stat: Stat, table: String, connector: Connector): Unit =
    if (!queue.offer(StatToWrite(stat, table, connector))) {
      logger.debug("Stat queue is full - stat being dropped")
    }

  /**
   * Writes the stats.
   *
   * @param statsToWrite
   */
  def write(statsToWrite: Iterable[StatToWrite]): Unit =
    statsToWrite.groupBy(_.connector).foreach { case (connector, statsForConnector) =>
      statsForConnector.groupBy(_.stat.getClass).foreach { case (clas, statsForClass) =>
        // get the appropriate transform for this type of stat
        val transform = clas match {
          case c if c == classOf[QueryStat] => QueryStatTransform.asInstanceOf[StatTransform[Stat]]
          case _ => throw new RuntimeException("Not implemented")
        }

        // write data by table
        statsForClass.groupBy(_.table).foreach { case (table, statsForTable) =>
          checkTable(table, connector)
          val writer = connector.createBatchWriter(table, batchWriterConfig)
          try {
            writer.addMutations(statsForTable.map(stw => transform.statToMutation(stw.stat)).asJava)
            writer.flush()
          } finally {
            writer.close()
          }
        }
      }
    }

  /**
   * Create the stats table if it doesn't exist
   * @param table
   * @return
   */
  private def checkTable(table: String, connector: Connector) =
    tableCache.getOrElseUpdate((connector, table), {
      val tableOps = connector.tableOperations()
      if (!tableOps.exists(table)) {
        try {
          tableOps.create(table, true, TimeType.LOGICAL)
        } catch {
          case e: TableExistsException => // this can happen with multiple threads
        }
      }
      true
    })

  override def run() = {
    try {
      // wait for a stat to be queued
      val head = queue.take()
      // drain out any other stats that have been queued while sleeping
      val stats = collection.mutable.ListBuffer(head)
      queue.drainTo(stats.asJava)
      write(stats)
    } catch {
      case e: InterruptedException =>
        // normal thread termination, just propagate the interrupt
        Thread.currentThread().interrupt()
      case e: Exception =>
        // re-throw the error to shut down the executor service
        logger.error("Error in stat writing thread:", e)
        throw e
    }
  }
}
