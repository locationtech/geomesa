/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web

import java.security.cert.X509Certificate
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.scalatra.ScalatraBase
import org.scalatra.auth.{ScentryConfig, ScentryStrategy, ScentrySupport}

package object scalatra {

  case class User(dn:String)

  class PkiStrategy(protected val app: ScalatraBase)
                   (implicit request: HttpServletRequest, response: HttpServletResponse)
    extends ScentryStrategy[User] {
    override def name: String = "Pki"

    override def isValid(implicit request: HttpServletRequest): Boolean = true

    def authenticate()(implicit request: HttpServletRequest, response: HttpServletResponse): Option[User] = {
      val certs = request.getAttribute("javax.servlet.request.X509Certificate").asInstanceOf[Array[X509Certificate]]
      if (certs != null && certs.length > 0) {
        Some(User(certs.head.getSubjectX500Principal.getName))
      } else {
        None
      }
    }
  }

  trait PkiAuthenticationSupport
    extends ScalatraBase with
            ScentrySupport[User] {
    protected def fromSession = { case dn: String => User(dn) }

    protected def toSession = { case usr: User => usr.dn }

    protected val scentryConfig = new ScentryConfig {}.asInstanceOf[ScentryConfiguration]

    override protected def registerAuthStrategies() = {
      scentry.register("Pki", app => new PkiStrategy(app))
    }
  }
}
