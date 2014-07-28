/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.security

import collection.JavaConversions._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.apache.accumulo.core.security.Authorizations
import java.util
import java.io.Serializable

@RunWith(classOf[JUnitRunner])
class FilteringAuthorizationsProviderTest extends Specification {

  sequential

  val wrapped = new AuthorizationsProvider {
    override def configure(params: util.Map[String, Serializable]): Unit = { }
    override def getAuthorizations: Authorizations = new Authorizations("user", "admin", "test")
  }

  "FilteringAuthorizationsProvider" should {
    "filter wrapped authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]("auths" -> "admin"))
      val auths = filter.getAuthorizations

      auths should not be null
      auths.getAuthorizations.length mustEqual 1

      val strings = auths.getAuthorizations.map(new String(_))
      strings.contains("admin") must beTrue
    }

    "filter multiple authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]("auths" -> "user,test"))
      val auths = filter.getAuthorizations

      auths should not be null
      auths.getAuthorizations.length mustEqual 2

      val strings = auths.getAuthorizations.map(new String(_))
      strings.contains("user") must beTrue
      strings.contains("test") must beTrue
    }

    "not filter if no filter is specified" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]())
      val auths = filter.getAuthorizations
      auths should not be null
      auths.getAuthorizations.length mustEqual 3

      val strings = auths.getAuthorizations.map(new String(_))
      strings.contains("user") must beTrue
      strings.contains("admin") must beTrue
      strings.contains("test") must beTrue
    }
  }
}
