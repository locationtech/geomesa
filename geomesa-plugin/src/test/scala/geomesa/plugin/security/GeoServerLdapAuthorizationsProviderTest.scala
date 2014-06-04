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

package geomesa.core.data

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import geomesa.plugin.security.GeoServerLdapAuthorizationsProvider
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.authentication.TestingAuthenticationToken

@RunWith(classOf[JUnitRunner])
class GeoServerLdapAuthorizationsProviderTest extends Specification {

  sequential

  "GeoServerLdapAuthorizationsProvider" should {

    "read ldap connection info from properties" in {
      val provider = new GeoServerLdapAuthorizationsProvider
      provider.environment must not be none
      provider.environment.size must be equalTo (8)
      provider.searchRoot must be equalTo("o=Spring Framework")
    }

    "connect to ldap" in {

      skipped("Requires active LDAP to connect to")

      SecurityContextHolder.getContext.setAuthentication(new TestingAuthenticationToken("rod", null))
      val provider = new GeoServerLdapAuthorizationsProvider
      val auths = provider.getAuthorizations
      println("auths: " + auths.getClass + " " + auths)
    }
  }

}
