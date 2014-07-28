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

import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.AccumuloDataStore
import geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.joda.time.format.DateTimeFormat

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
 * Trait for mapping stats to accumulo and back
 */
trait StatTransform[S <: Stat] extends Logging {

  protected def createMutation(stat: Stat) = new Mutation(StatTransform.dateFormat.print(stat.date))

  protected def createRandomColumnFamily = Random.nextInt(9999).formatted("%1$04d")

  protected def getStatTableSuffix: String

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
   * Gets the table used for a particular stat
   *
   * @param catalogTable
   * @param featureName
   * @return
   */
  def getStatTable(catalogTable: String, featureName: String): String =
    AccumuloDataStore.formatTableName(catalogTable, featureName, getStatTableSuffix)

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
        // get the data for the stat entry, which consists of a several CQ/values
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
        // use the row data to return a Stat
        rowToStat(entries)
      }

      override def hasNext = last.isDefined || iter.hasNext
    }
    SelfClosingIterator(wrappedIter)
  }
}

object StatTransform {
  val dateFormat = DateTimeFormat.forPattern("yyyyMMdd-HH:mm:ss.SSS").withZoneUTC()
}
