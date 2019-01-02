/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureValidator.{Validator, ValidatorFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureValidatorTest extends Specification {

  "SimpleFeatureValidator" should {
    "allow custom SPI loading" in {
      val custom = SimpleFeatureValidator(Seq("custom"))
      custom must not(beNull)
      custom.init(null)
      custom.validate(null) must beNull
      SimpleFeatureValidatorTest.errors.set("foo")
      try {
        custom.validate(null) mustEqual "foo"
      } finally {
        SimpleFeatureValidatorTest.errors.remove()
      }
    }
    "allow custom SPI loading with options" in {
      val custom = SimpleFeatureValidator(Seq("custom(foo,bar,baz)"))
      custom must not(beNull)
      custom.init(null)
      custom.validate(null) mustEqual "foo,bar,baz"
      SimpleFeatureValidatorTest.errors.set("foo")
      try {
        custom.validate(null) mustEqual "foo"
      } finally {
        SimpleFeatureValidatorTest.errors.remove()
      }
    }
  }
}

object SimpleFeatureValidatorTest {

  val errors = new ThreadLocal[String]()

  class CustomValidator(val config: Option[String]) extends Validator {
    override def validate(sf: SimpleFeature): String = Option(errors.get).orElse(config).orNull
  }

  // note: registered in
  // src/test/resources/META-INF/services/org.locationtech.geomesa.convert.SimpleFeatureValidator$ValidatorFactory
  class CustomValidatorFactory extends ValidatorFactory {
    override def name: String = "custom"
    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = new CustomValidator(config)
  }
}
