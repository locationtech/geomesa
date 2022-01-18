/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.{io => jio, util => ju}

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.ReadWriteFlag
import org.opengis.feature.simple.SimpleFeature

package object security {

  import scala.collection.JavaConverters._

  val GEOMESA_AUDIT_PROVIDER_IMPL: SystemProperty = SystemProperty("geomesa.audit.provider.impl")
  val GEOMESA_AUTH_PROVIDER_IMPL : SystemProperty = SystemProperty("geomesa.auth.provider.impl")

  val AuthsParam =
    new GeoMesaParam[String](
      "geomesa.security.auths",
      "Super-set of authorizations that will be used for queries. The actual authorizations might differ, depending on the authorizations provider, but will be outside this set. Comma-delimited.",
      deprecatedKeys = Seq("auths"),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadUpdate
    )

  val ForceEmptyAuthsParam =
    new GeoMesaParam[java.lang.Boolean](
      "geomesa.security.auths.force-empty",
      "Default to using no authorizations during queries, instead of using the connection user's authorizations",
      default = false,
      deprecatedKeys = Seq("forceEmptyAuths"),
      readWrite = ReadWriteFlag.ReadUpdate
    )

  val AuthProviderParam =
    new GeoMesaParam[AuthorizationsProvider](
      "geomesa.security.auths.provider",
      "Authorizations provider",
      deprecatedKeys = Seq("authProvider"),
      readWrite = ReadWriteFlag.ReadUpdate
    )

  trait SecurityParams {
    val AuthsParam: GeoMesaParam[String] = org.locationtech.geomesa.security.AuthsParam
    val ForceEmptyAuthsParam: GeoMesaParam[java.lang.Boolean] = org.locationtech.geomesa.security.ForceEmptyAuthsParam
  }

  implicit class SecureSimpleFeature(val sf: SimpleFeature) extends AnyVal {

    /**
     * Sets the visibility to the given ``visibility`` expression.
     *
     * @param visibility the visibility expression or null
     */
    def visibility_=(visibility: String): Unit = SecurityUtils.setFeatureVisibility(sf, visibility)

    /**
     * Sets the visibility to the given ``visibility`` expression
     *
     * @param visibility the visibility expression or None
     */
    def visibility_=(visibility: Option[String]): Unit = SecurityUtils.setFeatureVisibility(sf, visibility.orNull)

    /**
     * @return the visibility or None
     */
    def visibility: Option[String] = Option(SecurityUtils.getVisibility(sf))
  }

  def getAuthorizationsProvider(params: ju.Map[String, jio.Serializable], auths: Seq[String]): AuthorizationsProvider =
    AuthorizationsProvider.apply(params, auths.asJava)
}
