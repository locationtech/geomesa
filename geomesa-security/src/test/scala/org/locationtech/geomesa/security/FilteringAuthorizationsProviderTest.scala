/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import java.io.Serializable
import java.util

import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

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
