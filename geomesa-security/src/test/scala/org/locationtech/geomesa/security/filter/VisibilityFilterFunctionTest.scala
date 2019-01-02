/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.security._
import org.locationtech.geomesa.security.filter.VisibilityFilterFunctionTest.TestAuthorizationsProvider
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VisibilityFilterFunctionTest extends Specification {

  import scala.collection.JavaConverters._

  val ff2 = CommonFactoryFinder.getFilterFactory2()

  val testSft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")

  def featureWithAttribute(vis: String): SimpleFeature = {
    val f = new SimpleFeatureImpl(Array.ofDim(2), testSft, new FeatureIdImpl(""), false)
    f.setAttribute(0, vis)
    f
  }

  def featureWithUserData(vis: String): SimpleFeature = {
    val f = new SimpleFeatureImpl(Array.ofDim(2), testSft, new FeatureIdImpl(""), false)
    f.visibility = vis
    f
  }

  def filter(attribute: Option[String] = None): Filter = {
    val fn = attribute match {
      case None    => ff2.function(VisibilityFilterFunction.Name.getFunctionName)
      case Some(a) => ff2.function(VisibilityFilterFunction.Name.getFunctionName, ff2.property(a))
    }
    ff2.equals(fn, ff2.literal(true))
  }

  def withAuths[T](auths: Seq[String])(fn: => T): T = {
    try {
      GEOMESA_AUTH_PROVIDER_IMPL.threadLocalValue.set(classOf[TestAuthorizationsProvider].getName)
      VisibilityFilterFunctionTest.auths.set(auths.asJava)
      fn
    } finally {
      GEOMESA_AUTH_PROVIDER_IMPL.threadLocalValue.remove()
      VisibilityFilterFunctionTest.auths.remove()
    }
  }

  "VisibilityFilter" should {

    "work with AND'd vis" in {
      withAuths(Seq("ADMIN", "USER")) {
        filter().evaluate(featureWithUserData("ADMIN&USER")) must beTrue
        filter(Some("name")).evaluate(featureWithAttribute("ADMIN&USER")) must beTrue
      }
    }

    "work with OR'd vis" in {
      withAuths(Seq("USER")) {
        filter().evaluate(featureWithUserData("ADMIN|USER")) must beTrue
        filter(Some("name")).evaluate(featureWithAttribute("ADMIN|USER")) must beTrue
      }
    }

    "evaluate to false with no vis on the feature" in {
      withAuths(Seq("ADMIN", "USER")) {
        filter().evaluate(featureWithUserData(null)) must beFalse
        filter(Some("name")).evaluate(featureWithAttribute(null)) must beFalse
      }
    }

    "evaluate to false when user does not have the right auths" in {
      withAuths(Seq("ADMIN")) {
        filter().evaluate(featureWithUserData("ADMIN&USER")) must beFalse
        filter(Some("name")).evaluate(featureWithAttribute("ADMIN&USER")) must beFalse
      }
    }
  }
}

object VisibilityFilterFunctionTest {

  private val auths = new ThreadLocal[java.util.List[String]]

  class TestAuthorizationsProvider extends AuthorizationsProvider {
    override def configure(params: java.util.Map[String, java.io.Serializable]): Unit = {}
    override def getAuthorizations: java.util.List[String] = auths.get
  }
}
