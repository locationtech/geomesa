/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class FilteringAuthorizationsProviderTest extends Specification {

  val wrapped = new AuthorizationsProvider {
    override def configure(params: util.Map[String, _]): Unit = { }
    override def getAuthorizations: java.util.List[String] = java.util.List.of("user", "admin", "test")
  }

  "FilteringAuthorizationsProvider" should {
    "filter wrapped authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped, java.util.List.of[String]("admin"))
      val auths = filter.getAuthorizations.asScala

      auths should not be null
      auths.length mustEqual 1
      auths.contains("admin") must beTrue
    }

    "filter multiple authorizations" in {
      val filter = new FilteringAuthorizationsProvider(wrapped, java.util.List.of("user", "test"))
      val auths = filter.getAuthorizations

      auths should not be null
      auths.asScala.length mustEqual 2

      auths.contains("user") must beTrue
      auths.contains("test") must beTrue
    }
  }
}
