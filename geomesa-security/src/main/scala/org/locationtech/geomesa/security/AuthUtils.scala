/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.locationtech.geomesa.utils.classpath.ServiceLoader

object AuthUtils {

  import scala.collection.JavaConverters._

  /**
   * Static method to load and configure an authorization provider from the classpath
   *
   * @param params parameters
   * @param authorizations master set of authorizations
   * @return authorizations provider
   */
  def getProvider(params: java.util.Map[String, _], authorizations: java.util.List[String]): AuthorizationsProvider =
    getProvider(params, authorizations.asScala.toSeq)

  /**
   * Static method to load and configure an authorization provider from the classpath
   *
   * @param params parameters
   * @param authorizations master set of authorizations
   * @return authorizations provider
   */
  def getProvider(params: java.util.Map[String, _], authorizations: Seq[String]): AuthorizationsProvider = {
    val provider = AuthProviderParam.lookupOpt(params).getOrElse {
      val providers = ServiceLoader.load[AuthorizationsProvider]()
      GEOMESA_AUTH_PROVIDER_IMPL.option match {
        case None =>
          if (providers.isEmpty) {
            new DefaultAuthorizationsProvider()
          } else if (providers.lengthCompare(1) == 0) {
            providers.head
          } else {
            throw new IllegalStateException(
              "Found multiple AuthorizationsProvider implementations. Please specify one to use with the system " +
                  s"property '${GEOMESA_AUTH_PROVIDER_IMPL.property}': " +
                  providers.map(_.getClass.getName).mkString(", "))
          }

        case Some(p) =>
          if (p == classOf[DefaultAuthorizationsProvider].getName) {
            new DefaultAuthorizationsProvider()
          } else {
            providers.find(_.getClass.getName == p).getOrElse {
              throw new IllegalArgumentException(
                s"The service provider class '$p' specified by '${GEOMESA_AUTH_PROVIDER_IMPL.property}' could not " +
                    s"be loaded. Available providers are: ${providers.map(_.getClass.getName).mkString(", ")}")
            }
          }
      }
    }

    // we wrap the authorizations provider in one that will filter based on the configured max auths
    val filtered = new FilteringAuthorizationsProvider(provider)

    // update the authorizations in the parameters and then configure the auth provider
    // we copy the map so as not to modify the original
    val paramsWithAuths = new java.util.HashMap[String, Any](params)
    paramsWithAuths.put(AuthsParam.key, authorizations.mkString(","))
    filtered.configure(paramsWithAuths)

    filtered
  }
}
