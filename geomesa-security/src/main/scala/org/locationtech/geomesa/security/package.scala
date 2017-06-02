/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.util.ServiceLoader
import java.{io => jio, util => ju}

import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeature

package object security {

  val GEOMESA_AUDIT_PROVIDER_IMPL = SystemProperty("geomesa.audit.provider.impl")
  val GEOMESA_AUTH_PROVIDER_IMPL  = SystemProperty("geomesa.auth.provider.impl")

  val AuthsParam =
    new Param(
      "auths",
      classOf[String],
      """
        |Super-set of authorizations that will be used for queries. The actual authorizations might
        |differ, depending on the authorizations provider, but will be outside this set. Comma-delimited."
      """.stripMargin,
      false)

  val ForceEmptyAuthsParam =
    new Param("forceEmptyAuths",
      classOf[java.lang.Boolean],
      "Default to using no authorizations during queries, instead of using the connection user's authorizations",
      false,
      false)

  val AuthProviderParam = new Param("authProvider",
    classOf[AuthorizationsProvider],
    "Authorizations provider",
    false)

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

  def getAuthorizationsProvider(params: ju.Map[String, jio.Serializable], auths: Seq[String]): AuthorizationsProvider = {
    import scala.collection.JavaConversions._

    // we wrap the authorizations provider in one that will filter based on the max auths configured for this store
    val providers = ServiceLoader.load(classOf[AuthorizationsProvider]).toBuffer
    val toWrap = Option(params.get(AuthProviderParam.key).asInstanceOf[AuthorizationsProvider]).getOrElse {
      GEOMESA_AUTH_PROVIDER_IMPL.option match {
        case Some(prop) =>
          if (classOf[DefaultAuthorizationsProvider].getName == prop)
            new DefaultAuthorizationsProvider
          else
            providers.find(_.getClass.getName == prop).getOrElse {
              throw new IllegalArgumentException(s"The service provider class '$prop' specified by " +
                  s"${GEOMESA_AUTH_PROVIDER_IMPL.property} could not be loaded")
            }
        case None =>
          providers.length match {
            case 0 => new DefaultAuthorizationsProvider
            case 1 => providers.head
            case _ =>
              throw new IllegalStateException(
                "Found multiple AuthorizationsProvider implementations. Please specify the one to use with " +
                    s"the system property '${GEOMESA_AUTH_PROVIDER_IMPL.property}' :: " +
                    s"${providers.map(_.getClass.getName).mkString(", ")}")
          }
      }
    }

    val authorizationsProvider = new FilteringAuthorizationsProvider(toWrap)

    // update the authorizations in the parameters and then configure the auth provider
    // we copy the map so as not to modify the original
    val modifiedParams = params ++ Map(AuthsParam.key -> auths.mkString(","))
    authorizationsProvider.configure(modifiedParams)

    authorizationsProvider
  }
}
