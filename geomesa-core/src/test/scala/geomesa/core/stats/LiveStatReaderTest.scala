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

import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LiveStatReaderTest extends Specification {

  sequential

  lazy val connector = new ZooKeeperInstance("mycloud", "zoo1,zoo2,zoo3")
                         .getConnector("root", new PasswordToken("password"))

  val table = "geomesa_catalog"
  val feature = "twitter"

  "StatReader" should {

    "query accumulo" in {

      skipped("Meant for integration")

      val reader = new QueryStatReader(connector, table)

      val results = reader.query(feature, new Date(0), new Date(), new Authorizations())

      results.foreach(println)

      success
    }
  }

}
