/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DateIndexTest extends Specification with TestWithMultipleSfts {

  val spec = "name:String:index=full,dtg:Date,*geom:Point:srid=4326"

  val attributeValues = Array("foo", "1950-01-01T00:00:00.000Z", "POINT (10 10)")

  "GeoMesa" should {
    "fail to index dates before the epoch" >> {
      val sft = createNewSchema(spec)
      addFeature(sft, ScalaSimpleFeature.create(sft, "1", attributeValues: _*)) must throwAn[Exception]
    }
    "allow indexing dates before the epoch with z3 and date disabled" >> {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      val sft = createNewSchema(spec + ";geomesa.ignore.dtg='true'", None)
      sft.getDtgField must beNone
      sft.getIndices.map(_.name) must containTheSameElementsAs(Seq(Z2Index.name, AttributeIndex.name, IdIndex.name))
      addFeature(sft, ScalaSimpleFeature.create(sft, "1", attributeValues: _*)) must not(throwAn[Exception])
    }
  }
}
