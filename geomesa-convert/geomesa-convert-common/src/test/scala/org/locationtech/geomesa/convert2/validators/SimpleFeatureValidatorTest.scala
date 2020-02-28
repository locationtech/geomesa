/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureValidatorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("foo", "*geom:Point:srid=4326")

  "SimpleFeatureValidator" should {
    "allow custom SPI loading" in {
      val custom = SimpleFeatureValidator(sft, Seq("custom"), ConverterMetrics.empty)
      custom must not(beNull)
      custom.validate(null) must beNull
      SimpleFeatureValidatorTest.errors.set("foo")
      try {
        custom.validate(null) mustEqual "foo"
      } finally {
        SimpleFeatureValidatorTest.errors.remove()
      }
    }
    "allow custom SPI loading with options" in {
      val custom = SimpleFeatureValidator(sft, Seq("custom(foo,bar,baz)"), ConverterMetrics.empty)
      custom must not(beNull)
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

  class CustomValidator(val config: Option[String]) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = Option(errors.get).orElse(config).orNull
    override def close(): Unit = {}
  }

  // note: registered in
  // src/test/resources/META-INF/services/org.locationtech.geomesa.convert2.validators.SimpleFeatureValidatorFactory
  class CustomValidatorFactory extends SimpleFeatureValidatorFactory {
    override def name: String = "custom"
    override def apply(
        sft: SimpleFeatureType,
        metrics: ConverterMetrics,
        config: Option[String]): SimpleFeatureValidator = new CustomValidator(config)
  }
}
