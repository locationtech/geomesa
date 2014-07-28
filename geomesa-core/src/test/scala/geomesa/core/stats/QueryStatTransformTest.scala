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

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class QueryStatTransformTest extends Specification {

  val df = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")

  val table = "QueryStatTransformTest"
  val featureName = "stat-writer-test"

  val connector = new MockInstance().getConnector("user", new PasswordToken("password"))
  connector.tableOperations().create(table)

  "QueryStatTransform" should {

    "convert query stats to and from accumulo" in {

      // currently we don't restore table and feature in the query stat - thus setting them null here
      val stat = QueryStat(null, null, 500L, "query1", 101L, 201L, 11)

      val writer = connector.createBatchWriter(table, new BatchWriterConfig())

      writer.addMutation(QueryStatTransform.statToMutation(stat))
      writer.flush()
      writer.close()

      val scanner = connector.createScanner(table, new Authorizations())

      val converted = QueryStatTransform.rowToStat(scanner.iterator().asScala.toList)

      converted must beEqualTo(stat)
    }
  }
}
