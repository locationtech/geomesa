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

import java.util.Date

import org.apache.accumulo.core.client.{Connector, Scanner}
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations

/**
 * Abstract class for querying stats
 *
 * @param connector
 * @param catalogTable
 * @tparam S
 */
abstract class StatReader[S <: Stat](connector: Connector, catalogTable: String) {

  protected def statTransform: StatTransform[S]

  /**
   * Main query method based on date range. Subclasses can add additional query capability (on
   * attributes, for instance).
   *
   * @param featureName
   * @param start
   * @param end
   * @param authorizations
   * @return
   */
  def query(featureName: String, start: Date, end: Date, authorizations: Authorizations): Iterator[S] = {
    val table = statTransform.getStatTable(catalogTable, featureName)

    val scanner = connector.createScanner(table, authorizations)
    val rangeStart = StatTransform.dateFormat.print(start.getTime)
    val rangeEnd = StatTransform.dateFormat.print(end.getTime)
    scanner.setRange(new AccRange(rangeStart, rangeEnd))

    configureScanner(scanner)

    statTransform.iterator(scanner)
  }

  /**
   * Can be implemented by subclasses to configure scans beyond a simple date range
   *
   * @param scanner
   */
  protected def configureScanner(scanner: Scanner)
}

/**
 * Class for querying query stats
 *
 * @param connector
 * @param catalogTable
 */
class QueryStatReader(connector: Connector, catalogTable: String)
    extends StatReader[QueryStat](connector, catalogTable) {

  override protected val statTransform = QueryStatTransform

  override protected def configureScanner(scanner: Scanner) = {}
}
