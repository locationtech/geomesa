/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import java.{io => jio, util => ju}
import javax.imageio.spi.ServiceRegistry

import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeature

package object security {

  val GEOMESA_AUDIT_PROVIDER_IMPL = SystemProperty("geomesa.audit.provider.impl")
  val GEOMESA_AUTH_PROVIDER_IMPL  = SystemProperty("geomesa.auth.provider.impl")

  val authsParam =
    new Param(
      "auths",
      classOf[String],
      """
        |Super-set of authorizations that will be used for queries. The actual authorizations might
        |differ, depending on the authorizations provider, but will be outside this set. Comma-delimited."
      """.stripMargin,
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
    val providers = ServiceRegistry.lookupProviders(classOf[AuthorizationsProvider]).toBuffer
    val toWrap = GEOMESA_AUTH_PROVIDER_IMPL.option match {
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

    val authorizationsProvider = new FilteringAuthorizationsProvider(toWrap)

    // update the authorizations in the parameters and then configure the auth provider
    // we copy the map so as not to modify the original
    val modifiedParams = params ++ Map(authsParam.key -> auths.mkString(","))
    authorizationsProvider.configure(modifiedParams)

    authorizationsProvider
  }

  def getAuditProvider(params: ju.Map[String, jio.Serializable]): Option[AuditProvider] = {
    import scala.collection.JavaConversions._

    val providers = ServiceRegistry.lookupProviders(classOf[AuditProvider]).toBuffer

    // if the user specifies an auth provider to use, try to use that impl
    val specified = GEOMESA_AUDIT_PROVIDER_IMPL.option.map { prop =>
      providers.find(_.getClass.getName == prop).getOrElse {
        throw new RuntimeException(s"Could not load audit provider $prop")
      }
    }
    val provider = specified.orElse {
      if (providers.length > 1) {
        throw new IllegalStateException(
          "Found multiple AuditProvider implementations. Please specify the one to use with the system " +
              s"property '${GEOMESA_AUDIT_PROVIDER_IMPL.property}' :: " +
              s"${providers.map(_.getClass.getName).mkString(", ")}")
      }
      providers.headOption
    }
    provider.foreach(_.configure(params))
    provider
  }
}
