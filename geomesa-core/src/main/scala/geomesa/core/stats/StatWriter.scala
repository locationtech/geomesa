/*
 *
 *  * Copyright 2014 Commonwealth Computer Research, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the License);
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an AS IS BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package geomesa.core.stats

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, Executors}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}

import com.google.common.collect.Queues
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.Mutation
import org.geotools.data.Query
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.util.Random

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

object StatWriter extends Runnable with Logging {

  private val batchSize = 100

  private val writeDelayMillis = 1000

  private val batchWriterConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(10)

  // use the guava exiting executor so that this thread doesn't hold up the jvm shutdown
  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))

  private val running = new AtomicBoolean(false)

  private val queue = Queues.newLinkedBlockingQueue[Stat](batchSize)

  private val dateFormat = DateTimeFormat.forPattern("yyyyMMdd-HH:mm:ss.SSS").withZoneUTC()

  private val tableCache = collection.mutable.Map.empty[String, Boolean]

  private var connector: Connector = null

  sys.addShutdownHook{
    executor.shutdownNow()
  }

  /**
   * Starts the background thread for writing stats, if it hasn't already been started
   *
   * @param connector
   */
  private def startIfNeeded(connector: Connector) {
    if (!connector.isInstanceOf[MockConnector] && running.compareAndSet(false, true)) {
      this.connector = connector
      // we want to wait between invocations to give more stats a chance to queue up
      executor.scheduleWithFixedDelay(this, writeDelayMillis, writeDelayMillis, TimeUnit.MILLISECONDS)
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
   * Creates a mutation and sets the row
   *
   * @param stat
   * @return
   */
  private def createMutation(stat: Stat): Mutation = new Mutation(dateFormat.print(stat.date))

  /**
   * Translates a query stat into an accumulo row
   *
   * @param stat
   * @return
   */
  private def getMutationForQueryStat(stat: Stat): Mutation = {
    val s = stat.asInstanceOf[QueryStat]
    val mutation = createMutation(s)
    val cf = Random.nextInt(9999).formatted("%1$04d")
    mutation.put(cf, "query", s.query.toString)
    mutation.put(cf, "timePlanning", s.planningTime + "ms")
    mutation.put(cf, "timeScanning", s.scanTime + "ms")
    mutation.put(cf, "timeTotal", (s.scanTime + s.planningTime) + "ms")
    mutation.put(cf, "hits", s.numResults.toString)
    mutation
  }

  /**
   * Writes the stats.
   *
   * @param stats
   * @param connector
   */
  private def write(stats: Iterable[Stat], connector: Connector): Unit = {
    // group stats by class (type of stat) and simple feature type
    stats.groupBy(s => s.getClass).foreach { case (clas, clasIter) =>
      // match the create mutation function and the table suffix to the type of stat
      val (suffix, createMutation): (String, Stat => Mutation) = clas match {
        case c if(clas == classOf[QueryStat]) => ("queries", getMutationForQueryStat)
        case _ => throw new RuntimeException("Not implemented")
      }
      clasIter.groupBy(s => (s.catalogTable, s.featureName)).foreach { case ((catalogTable, featureName), iter) =>
        val table = AccumuloDataStore.formatTableName(catalogTable, featureName, suffix)
        // create the stats table if it doesn't exist
        tableCache.getOrElseUpdate(table, {
          val tableOps = connector.tableOperations()
          if (!tableOps.exists(table)) {
            tableOps.create(table, true, TimeType.LOGICAL)
          }
          true
        })
        val writer = connector.createBatchWriter(table, batchWriterConfig)
        val mutations = iter.map(s => createMutation(s))
        writer.addMutations(mutations.asJava)
        writer.flush()
        writer.close()
      }
    }
  }

  override def run() = {
    try {
      // wait for a stat to be queued
      val head = queue.take()
      // drain out any other stats that have been queued while sleeping
      val stats = collection.mutable.ListBuffer.empty[Stat]
      queue.drainTo(stats.asJava)
      write((List(head) ++ stats), connector)
    } catch {
      case e: InterruptedException => // thread has been terminated
    }
  }
}

trait Stat {
  def catalogTable: String
  def featureName: String
  def date: Long
}

case class QueryStat(catalogTable:  String,
                     featureName:   String,
                     date:          Long,
                     query:         Filter,
                     planningTime:  Long,
                     scanTime:      Long,
                     numResults:    Int) extends Stat
