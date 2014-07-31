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
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatReaderTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val catalogTable = "geomesa_catalog"
  val featureName = "stat-reader-test"

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))

  val auths = new Authorizations()

  def writeStat(stat: Stat) = StatWriter.write(List(stat), connector)

  "QueryStatReader" should {

    writeStat(QueryStat(catalogTable,
                          featureName,
                          df.parseMillis("2014.07.26 13:20:01"),
                          "query1",
                          "hint1=true",
                          101L,
                          201L,
                          11))
    writeStat(QueryStat(catalogTable,
                          featureName,
                          df.parseMillis("2014.07.26 14:20:01"),
                          "query2",
                          "hint2=true",
                          102L,
                          202L,
                          12))
    writeStat(QueryStat(catalogTable,
                          featureName,
                          df.parseMillis("2014.07.27 13:20:01"),
                          "query3",
                          "hint3=true",
                          102L,
                          202L,
                          12))

    val reader = new QueryStatReader(connector, catalogTable)

    "query all stats in order" in {
      val queries = reader.query(featureName, new Date(0), new Date(), auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 3
      list(0).queryFilter mustEqual "query1"
      list(1).queryFilter mustEqual "query2"
      list(2).queryFilter mustEqual "query3"
    }

    "query by day" in {
      val queries = reader.query(featureName,
                                  df.parseDateTime("2014.07.26 00:00:00").toDate,
                                  df.parseDateTime("2014.07.26 23:59:59").toDate,
                                  auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 2
      list(0).queryFilter mustEqual "query1"
      list(1).queryFilter mustEqual "query2"
    }

    "query by hour" in {
      val queries = reader.query(featureName,
                                  df.parseDateTime("2014.07.26 13:00:00").toDate,
                                  df.parseDateTime("2014.07.26 13:59:59").toDate,
                                  auths)

      queries must not beNull

      val list = queries.toList

      list.size mustEqual 1
      list(0).queryFilter mustEqual "query1"
    }

  }

}
