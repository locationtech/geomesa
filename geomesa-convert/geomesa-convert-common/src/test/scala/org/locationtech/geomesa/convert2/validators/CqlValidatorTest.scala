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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CqlValidatorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("foo", "dtg:Date,*geom:Point:srid=4326")

  "CqlValidator" should {
    "require ecql expression" in {
      SimpleFeatureValidator(sft, Seq("cql"), ConverterMetrics.empty) must throwAn[IllegalArgumentException]
    }
    "validate ecql expressions" in {
      val validator = SimpleFeatureValidator(sft, Seq("cql(bbox(geom,45,-90,55,90))"), ConverterMetrics.empty)
      validator must not(beNull)
      val sf = ScalaSimpleFeature.create(sft, "0", "2018-01-01T00:00:00.000Z", "POINT (50 45)")
      validator.validate(sf) must beNull
      sf.setAttribute("geom", "POINT (-50 45)")
      validator.validate(sf) must not(beNull)
    }
  }
}
