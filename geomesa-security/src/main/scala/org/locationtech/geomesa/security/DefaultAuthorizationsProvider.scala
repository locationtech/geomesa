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

package org.locationtech.geomesa.security

import org.apache.accumulo.core.security.Authorizations

/**
 * Default implementation of the AuthorizationsProvider that doesn't provide any authorizations
 */
class DefaultAuthorizationsProvider extends AuthorizationsProvider {

  var authorizations: Authorizations = new Authorizations

  override def getAuthorizations: Authorizations = authorizations

  override def configure(params: java.util.Map[String, java.io.Serializable]) {
    val authString = authsParam.lookUp(params).asInstanceOf[String]
    if (authString == null || authString.isEmpty)
      authorizations = new Authorizations()
    else
      authorizations = new Authorizations(authString.split(","):_*)
  }

}
