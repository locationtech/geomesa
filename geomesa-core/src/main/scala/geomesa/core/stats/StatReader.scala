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

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations

import scala.reflect._

class StatReader[S <: Stat: ClassTag](connector: Connector, catalogTable: String) {

  private val statTransform = StatTransform.getTransform[S]

  def query(featureName: String, start: Date, end: Date, authorizations: Authorizations): Iterator[S] = {
    val table = StatTransform.getStatTable[S](catalogTable, featureName)

    val scanner = connector.createScanner(table, authorizations)
    val rangeStart = StatTransform.dateFormat.print(start.getTime)
    val rangeEnd = StatTransform.dateFormat.print(end.getTime)
    scanner.setRange(new AccRange(rangeStart, rangeEnd))

    statTransform.iterator(scanner)
  }
}
