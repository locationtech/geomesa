/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.z3.legacy.XZ3IndexV1
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterTransformIteratorTest extends Specification {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("lines", "name:String,dtg:Date,*geom:LineString:srid=4326")

  "FilterTransformIterator" should {
    "configure 1.3 compatibility mode" >> {
      // v1 index corresponds to gm 1.3
      val index = new XZ3IndexV1(null, sft, "geom", "dtg", IndexMode.ReadWrite)
      val filter = ECQL.toFilter("dtg DURING 2020-01-01T00:00:00.000Z/2020-01-01T01:00:00.000Z")
      val hints = new Hints()
      hints.put(QueryHints.FILTER_COMPAT, "1.3")
      val configOpt = FilterTransformIterator.configure(sft, index, Option(filter), hints)
      configOpt must beSome
      val config = configOpt.get
      config.getIteratorClass mustEqual "org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator"
      // expected values taken from a 1.3 install
      config.getOptions.asScala mustEqual
          Map(
            "sft"   -> "name:String,dtg:Date,*geom:LineString:srid=4326;geomesa.index.dtg='dtg'",
            "index" -> "xz3:1",
            "cql"   -> "dtg DURING 2020-01-01T00:00:00+00:00/2020-01-01T01:00:00+00:00"
          )
    }
  }
}
