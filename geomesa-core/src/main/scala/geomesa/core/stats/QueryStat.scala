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

import geomesa.core.data._
import geomesa.core.index.QueryHints._
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.geotools.factory.Hints
import org.opengis.filter.Filter

/**
 * Class for capturing query-related stats
 */
case class QueryStat(catalogTable:  String,
                     featureName:   String,
                     date:          Long,
                     queryFilter:   String,
                     queryHints:    String,
                     planningTime:  Long,
                     scanTime:      Long,
                     numResults:    Int) extends Stat

/**
 * Maps query stats to accumulo
 */
object QueryStatTransform extends StatTransform[QueryStat] {

  private val CQ_QUERY_FILTER = "queryFilter"
  private val CQ_QUERY_HINTS = "queryHints"
  private val CQ_PLANTIME = "timePlanning"
  private val CQ_SCANTIME = "timeScanning"
  private val CQ_TIME = "timeTotal"
  private val CQ_HITS = "hits"

  protected val getStatTableSuffix = "queries"

  override def statToMutation(stat: QueryStat): Mutation = {
    val mutation = createMutation(stat)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_QUERY_FILTER, stat.queryFilter)
    mutation.put(cf, CQ_QUERY_HINTS, stat.queryHints)
    mutation.put(cf, CQ_PLANTIME, stat.planningTime + "ms")
    mutation.put(cf, CQ_SCANTIME, stat.scanTime + "ms")
    mutation.put(cf, CQ_TIME, (stat.scanTime + stat.planningTime) + "ms")
    mutation.put(cf, CQ_HITS, stat.numResults.toString)
    mutation
  }

  override def rowToStat(entries: Iterable[Entry[Key, Value]]): QueryStat = {
    if (entries.isEmpty) {
      return null
    }

    val date = StatTransform.dateFormat.parseMillis(entries.head.getKey.getRow.toString)
    val values = collection.mutable.Map.empty[String, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier.toString match {
        case CQ_QUERY_FILTER => values.put(CQ_QUERY_FILTER, e.getValue.toString)
        case CQ_QUERY_HINTS => values.put(CQ_QUERY_HINTS, e.getValue.toString)
        case CQ_PLANTIME => values.put(CQ_PLANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_SCANTIME => values.put(CQ_SCANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_HITS => values.put(CQ_HITS, e.getValue.toString.toInt)
        case CQ_TIME => // time is an aggregate, doesn't need to map back to anything
        case _ => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    val queryHints = values.getOrElse(CQ_QUERY_HINTS, "").asInstanceOf[String]
    val queryFilter = values.getOrElse(CQ_QUERY_FILTER, "").asInstanceOf[String]
    val planTime = values.getOrElse(CQ_PLANTIME, 0L).asInstanceOf[Long]
    val scanTime = values.getOrElse(CQ_SCANTIME, 0L).asInstanceOf[Long]
    val hits = values.getOrElse(CQ_HITS, 0).asInstanceOf[Int]

    // TODO do we care about table/schema? they would have to be known to query anything and get this far...
    QueryStat(null, null, date, queryFilter, queryHints, planTime, scanTime, hits)
  }

  /**
   * Converts a filter object to a string for persisting
   *
   * @param filter
   * @return
   */
  def filterToString(filter: Filter): String = filter.toString

  // list of query hints we want to persist
  val QUERY_HINTS = List[Hints.Key](TRANSFORMS,
                                    TRANSFORM_SCHEMA,
                                    DENSITY_KEY,
                                    BBOX_KEY,
                                    WIDTH_KEY,
                                    HEIGHT_KEY)

  /**
   * Converts a query hints object to a string for persisting
   *
   * @param hints
   * @return
   */
  def hintsToString(hints: Hints): String =
    QUERY_HINTS.flatMap { key =>
      Option(hints.get(key)).map(v => s"${getString(key)}=${hints.get(key)}")
    }.sorted.mkString(",")

  /**
   * Maps a query hint to a string. We need this since the classes themselves don't really have a
   * decent toString representation.
   *
   * @param key
   * @return
   */
  private def getString(key: Hints.Key) =
    key match {
      case TRANSFORMS => "TRANSFORMS"
      case TRANSFORM_SCHEMA => "TRANSFORM_SCHEMA"
      case DENSITY_KEY => "DENSITY_KEY"
      case BBOX_KEY => "BBOX_KEY"
      case WIDTH_KEY => "WIDTH_KEY"
      case HEIGHT_KEY => "HEIGHT_KEY"
      case _ => "unknown_hint"
    }
}