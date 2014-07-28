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

package geomesa.core.stats

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.collect.Queues
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait StatWriter {

  def connector: Connector

  // start the background thread now that we have a connector to use
  StatWriter.startIfNeeded(connector)

  /**
   * Writes a stat to accumulo. This implementation adds the stat to a bounded queue, which should
   * be fast, then asynchronously writes the data in the background.
   *
   * @param stat
   */
  def writeStat(stat: Stat): Unit = StatWriter.queueStat(stat)
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

  private val queue = Queues.newLinkedBlockingQueue[Stat](batchSize)

  private val tableCache = new mutable.HashMap[String, Boolean] with mutable.SynchronizedMap[String, Boolean]

  private var connector: Connector = null

  sys.addShutdownHook {
    executor.shutdownNow()
  }

  /**
   * Starts the background thread for writing stats, if it hasn't already been started
   *
   * @param connector
   */
  private def startIfNeeded(connector: Connector) {
    if (running.compareAndSet(false, true)) {
      this.connector = connector
      if(!connector.isInstanceOf[MockConnector]) {
        // we want to wait between invocations to give more stats a chance to queue up
        executor.scheduleWithFixedDelay(this, writeDelayMillis, writeDelayMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * Queues a stat for writing. We don't want to affect memory and accumulo performance too much...
   * if we exceed the queue size, we drop any further stats
   *
   * @param stat
   */
  private def queueStat(stat: Stat): Unit = {
    if (!queue.offer(stat)) {
      logger.debug("Stat queue is full - stat being dropped")
    }
  }

  /**
   * Writes the stats.
   *
   * @param stats
   * @param connector
   */
  def write(stats: Iterable[Stat], connector: Connector): Unit = {
    stats.groupBy(s => s.getClass).foreach { case (clas, clasIter) =>
      // get the appropriate transform for this type of stat
      val transform = clas match {
        case c if c == classOf[QueryStat] => QueryStatTransform.asInstanceOf[StatTransform[Stat]]
        case _ => throw new RuntimeException("Not implemented")
      }
      // group stats by catalog and feature name
      clasIter.groupBy(s => (s.catalogTable, s.featureName)).foreach { case ((catalogTable, featureName), iter) =>
        val table = transform.getStatTable(catalogTable, featureName)
        checkTable(table, connector)
        val writer = connector.createBatchWriter(table, batchWriterConfig)
        val mutations = iter.map(s => transform.statToMutation(s))
        writer.addMutations(mutations.asJava)
        writer.flush()
        writer.close()
      }
    }
  }

  /**
   * Create the stats table if it doesn't exist
   * @param table
   * @return
   */
  private def checkTable(table: String, connector: Connector) =
    tableCache.getOrElseUpdate(table, {
      val tableOps = connector.tableOperations()
      if (!tableOps.exists(table)) {
        tableOps.create(table, true, TimeType.LOGICAL)
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
      write(stats, connector)
    } catch {
      case e: InterruptedException => // thread has been terminated
      case e: Exception => logger.error("Error in stat writing thread:", e)
    }
  }
}
