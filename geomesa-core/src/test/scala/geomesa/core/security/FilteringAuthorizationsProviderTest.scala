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
    override def getAuthorizations: Authorizations = new Authorizations("U", "S", "TS")
  }

  "FilteringAuthorizationsProvider" should {
    "filter wrapped authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]("auths" -> "S"))
      val auths = filter.getAuthorizations

      auths should not be null
      auths.getAuthorizations.length should be equalTo(1)

      new java.lang.String(auths.getAuthorizations.get(0)) mustEqual "S"
    }

    "filter multiple authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]("auths" -> "S,U"))
      val auths = filter.getAuthorizations

      auths should not be null
      auths.getAuthorizations.length should be equalTo(2)

      new java.lang.String(auths.getAuthorizations.get(0)) mustEqual "U"
      new java.lang.String(auths.getAuthorizations.get(1)) mustEqual "S"
    }

    "not filter if no filter is specified" in {
      val filter = new FilteringAuthorizationsProvider(wrapped)
      filter.configure(Map[String, Serializable]())
      val auths = filter.getAuthorizations
      auths should not be null
      auths.getAuthorizations.length should be equalTo(3)
      new String(auths.getAuthorizations.get(0)) mustEqual "U"
      new String(auths.getAuthorizations.get(1)) mustEqual "S"
      new String(auths.getAuthorizations.get(2)) mustEqual "TS"
    }
  }
}
