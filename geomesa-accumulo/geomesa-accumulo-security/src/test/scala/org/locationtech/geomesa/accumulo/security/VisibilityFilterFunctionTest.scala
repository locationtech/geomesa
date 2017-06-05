/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.security

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.security._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class VisibilityFilterFunctionTest extends Specification {

  sequential

  val testSFT = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
  System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, classOf[TestAuthorizationsProvider].getName)

  "VisibilityFilter" should {

    "work with simple viz" in {

      val f = new SimpleFeatureImpl(List.empty[AnyRef], testSFT, new FeatureIdImpl(""))

      f.visibility = "ADMIN&USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN", "USER"))
      SecurityContextHolder.setContext(ctx)

      VisibilityFilterFunction.filter.evaluate(f) must beTrue
    }

    "work with no viz on the feature" in {
      val f = new SimpleFeatureImpl(List.empty[AnyRef], testSFT, new FeatureIdImpl(""))

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN", "USER"))
      SecurityContextHolder.setContext(ctx)

      VisibilityFilterFunction.filter.evaluate(f) must beFalse
    }

    "return false when user does not have the right auths" in {
      val f = new SimpleFeatureImpl(List.empty[AnyRef], testSFT, new FeatureIdImpl(""))
      f.visibility = "ADMIN&USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN"))
      SecurityContextHolder.setContext(ctx)

      VisibilityFilterFunction.filter.evaluate(f) must beFalse
    }

    "return true when dealing with expressions" in {
      val f = new SimpleFeatureImpl(List.empty[AnyRef], testSFT, new FeatureIdImpl(""))
      f.visibility = "ADMIN|USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "USER"))
      SecurityContextHolder.setContext(ctx)

      VisibilityFilterFunction.filter.evaluate(f) must beTrue
    }

  }
}
