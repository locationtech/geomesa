/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.plugin.security

import java.io.Serializable
import java.util.{Map => jMap}

import org.locationtech.geomesa.utils.audit.AuditProvider
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails

import scala.collection.JavaConverters._

class SpringAuditProvider extends AuditProvider {

  override def getCurrentUserId: String = {
    val principal = getAuth.flatMap(a => Option(a.getPrincipal)).getOrElse("unknown")
    principal match {
      case p: UserDetails => p.getUsername
      case p => p.toString
    }
  }

  override def getCurrentUserDetails: jMap[AnyRef, AnyRef] = {
    getAuth match {
      case None => Map.empty[AnyRef, AnyRef].asJava
      case Some(auth) =>
        Map[AnyRef, AnyRef](
          SpringAuditProvider.AUTHORITIES   -> auth.getAuthorities,
          SpringAuditProvider.DETAILS       -> auth.getDetails,
          SpringAuditProvider.CREDENTIALS   -> auth.getCredentials,
          SpringAuditProvider.AUTHENTICATED -> auth.isAuthenticated.asInstanceOf[AnyRef]
        ).asJava
    }
  }

  override def configure(params: jMap[String, Serializable]): Unit = {}

  private def getAuth: Option[Authentication] =
    Option(SecurityContextHolder.getContext).flatMap(c => Option(c.getAuthentication))
}

object SpringAuditProvider {
  val AUTHORITIES   = "authorities"
  val DETAILS       = "details"
  val CREDENTIALS   = "credentials"
  val AUTHENTICATED = "authenticated"
}