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

import org.apache.accumulo.core.data.{Value, Key, Mutation}
import org.geotools.data.Query
import org.geotools.filter.text.cql2.CQL

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
 * Maps query stats to accumulo
 */
object QueryStatTransform extends StatTransform[QueryStat] {

  private val CQ_QUERY = "query"
  private val CQ_PLANTIME = "timePlanning"
  private val CQ_SCANTIME = "timeScanning"
  private val CQ_TIME = "timeTotal"
  private val CQ_HITS = "hits"

  protected val getStatTableSuffix = "queries"

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