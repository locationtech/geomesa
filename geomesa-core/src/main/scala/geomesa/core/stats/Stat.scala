/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.stats

import java.util
import java.util.Map.Entry


import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.AccumuloDataStore
import geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.data.{Value, Key, Mutation}
import org.geotools.data.Query
import org.geotools.filter.text.cql2.CQL
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.reflect._
import scala.util.Random

/**
 * Base trait for all stat types
 */
trait Stat {
  def catalogTable: String
  def featureName: String
  def date: Long
}

/**
 * Class for capturing query-related stats
 */
case class QueryStat(catalogTable:  String,
                     featureName:   String,
                     date:          Long,
                     query:         Query,
                     planningTime:  Long,
                     scanTime:      Long,
                     numResults:    Int) extends Stat

/**
 * Trait for mapping stats to accumulo and back
 */
trait StatTransform[S <: Stat] extends Logging {

  protected def createMutation(stat: Stat) = new Mutation(StatTransform.dateFormat.print(stat.date))

  protected def createRandomColumnFamily = Random.nextInt(9999).formatted("%1$04d")

  /**
   * Convert a stat to a mutation
   *
   * @param stat
   * @return
   */
  def statToMutation(stat: S): Mutation

  /**
   * Convert accumulo scan results into a stat
   *
   * @param entries
   * @return
   */
  def rowToStat(entries: Iterable[Entry[Key, Value]]): S

  /**
   * Creates an iterator that returns Stats from accumulo scans
   *
   * @param scanner
   * @return
   */
  def iterator(scanner: Scanner): CloseableIterator[S] = {
    val iter = scanner.iterator()

    val wrappedIter = new CloseableIterator[S] {

      var last: Option[Entry[Key, Value]] = None

      override def close() = scanner.close()

      override def next() = {
        // aggregate rows together
        val entries = collection.mutable.ListBuffer.empty[Entry[Key, Value]]
        if (last.isEmpty) {
          last = Some(iter.next())
        }
        val lastRowKey = last.get.getKey.getRow.toString
        var next: Option[Entry[Key, Value]] = last
        while (next.isDefined && next.get.getKey.getRow.toString == lastRowKey) {
          entries.append(next.get)
          next = if (iter.hasNext) Some(iter.next()) else None
        }
        last = next
        // use the aggregated row data to return a Stat
        rowToStat(entries)
      }

      override def hasNext = last.isDefined || iter.hasNext
    }
    SelfClosingIterator(wrappedIter)
  }
}

/**
 * Helper to get the correct stat transform
 */
object StatTransform {

  val dateFormat = DateTimeFormat.forPattern("yyyyMMdd-HH:mm:ss.SSS").withZoneUTC()

  /**
   * Gets the transform implementation for the class
   *
   * @tparam S
   * @return
   */
  def getTransform[S <: Stat: ClassTag](): StatTransform[S] = {
    classTag[S].runtimeClass match {
      case c if c == classOf[QueryStat] => QueryStatTransform.asInstanceOf[StatTransform[S]]
      case _ => throw new RuntimeException("Not implemented")
    }
  }

  /**
   * Gets the stat table name
   *
   * @param catalogTable
   * @param featureName
   * @tparam S
   * @return
   */
  def getStatTable[S <: Stat: ClassTag](catalogTable: String, featureName: String): String = {
    classTag[S].runtimeClass match {
      case c if c == classOf[QueryStat] => AccumuloDataStore.formatTableName(catalogTable, featureName, "queries")
      case _ => throw new RuntimeException("Not implemented")
    }
  }
}

/**
 * Maps query stats to accumulo
 */
object QueryStatTransform extends StatTransform[QueryStat] {

  private val CQ_QUERY = "query"
  private val CQ_PLANTIME = "timePlanning"
  private val CQ_SCANTIME = "timeScanning"
  private val CQ_TIME = "timeTotal"
  private val CQ_HITS = "hits"

  def statToMutation(stat: QueryStat): Mutation = {
    val mutation = createMutation(stat)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_QUERY, stat.query.toString)
    mutation.put(cf, CQ_PLANTIME, stat.planningTime + "ms")
    mutation.put(cf, CQ_SCANTIME, stat.scanTime + "ms")
    mutation.put(cf, CQ_TIME, (stat.scanTime + stat.planningTime) + "ms")
    mutation.put(cf, CQ_HITS, stat.numResults.toString)
    mutation
  }

  def rowToStat(entries: Iterable[Entry[Key, Value]]): QueryStat = {
    if (entries.isEmpty) {
      return null
    }

    val date = StatTransform.dateFormat.parseMillis(entries.head.getKey.getRow.toString)
    val values = collection.mutable.Map.empty[String, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier.toString match {
        case CQ_QUERY => values.put(CQ_QUERY, e.getValue.toString)
        case CQ_PLANTIME => values.put(CQ_PLANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_SCANTIME => values.put(CQ_SCANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_HITS => values.put(CQ_HITS, e.getValue.toString.toInt)
        case CQ_TIME => // time is an aggregate, doesn't need to map back to anything
        case _ => logger.warn(s"Unmapped entry in query stat ${e.getKey.getColumnQualifier.toString}")
      }
    }

    //TODO reconstitute query
    val query = new Query("test", CQL.toFilter("INCLUDE"))
    val planTime = values.getOrElse(CQ_PLANTIME, 0L).asInstanceOf[Long]
    val scanTime = values.getOrElse(CQ_SCANTIME, 0L).asInstanceOf[Long]
    val hits = values.getOrElse(CQ_HITS, 0).asInstanceOf[Int]

    // TODO do we care about table/schema? they would have to be known to query anything and get this far...
    QueryStat(null, null, date, query, planTime, scanTime, hits)
  }
}