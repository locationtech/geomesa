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

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, TableNotFoundException}
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatWriterTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat-writer-test"

  val auths = new Authorizations()

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  // mock class we can extend with statwriter
  class MockWriter(c: Connector) {
    val connector = c
  }

  val statReader = new QueryStatReader(connector, catalogTable)

  "StatWriter" should {

    "write query stats asynchronously" in {
      val writer = new MockWriter(connector) with StatWriter

      writer.writeStat(QueryStat(catalogTable,
                                  featureName,
                                  df.parseMillis("2014.07.26 13:20:01"),
                                  "query1",
                                  "hint1=true",
                                  101L,
                                  201L,
                                  11))
      writer.writeStat(QueryStat(catalogTable,
                                  featureName,
                                  df.parseMillis("2014.07.26 14:20:01"),
                                  "query2",
                                  "hint2=true",
                                  102L,
                                  202L,
                                  12))
      writer.writeStat(QueryStat(catalogTable,
                                  featureName,
                                  df.parseMillis("2014.07.27 13:20:01"),
                                  "query3",
                                  "hint3=true",
                                  102L,
                                  202L,
                                  12))

      try {
        val unwritten = statReader.query(featureName, new Date(0), new Date(), auths).toList
        unwritten must not beNull;
        unwritten.size mustEqual 0
      } catch {
        case e: TableNotFoundException => // table doesn't exist yet, since no stats are written
      }

      // this should write the queued stats
      StatWriter.run()

      val written = statReader.query(featureName, new Date(0), new Date(), auths).toList

      written must not beNull;
      written.size mustEqual 3
    }
  }
}
