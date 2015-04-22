package org.locationtech.geomesa.web.security

import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

@RunWith(classOf[JUnitRunner])
class VisibilityFilterTest extends Specification {

  import org.locationtech.geomesa.security._

  sequential

  val testSFT = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
  System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, classOf[TestAuthorizationsProvider].getName)

  "VisibilityFilter" should {

    "work with simple viz" in {

      val f = new AvroSimpleFeature(new FeatureIdImpl(""), testSFT)
      f.visibility = "ADMIN&USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN", "USER"))
      SecurityContextHolder.setContext(ctx)

      val vizFilter = VisibilityFilter()
      vizFilter.evaluate(f) must beTrue
    }

    "work with no viz on the feature" in {
      val f = new AvroSimpleFeature(new FeatureIdImpl(""), testSFT)

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN", "USER"))
      SecurityContextHolder.setContext(ctx)

      val vizFilter = VisibilityFilter()
      vizFilter.evaluate(f) must beFalse
    }

    "return false when user does not have the right auths" in {
      val f = new AvroSimpleFeature(new FeatureIdImpl(""), testSFT)
      f.visibility = "ADMIN&USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN"))
      SecurityContextHolder.setContext(ctx)

      val vizFilter = VisibilityFilter()
      vizFilter.evaluate(f) must beFalse
    }

    "return true when dealing with expressions" in {
      val f = new AvroSimpleFeature(new FeatureIdImpl(""), testSFT)
      f.visibility = "ADMIN|USER"

      val ctx = SecurityContextHolder.createEmptyContext()
      ctx.setAuthentication(new TestingAuthenticationToken(null, null, "USER"))
      SecurityContextHolder.setContext(ctx)

      val vizFilter = VisibilityFilter()
      vizFilter.evaluate(f) must beTrue
    }

  }
}
