/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class SimpleFeatureSchemaTest extends Specification with AllExpectations {

  "schema" should {
    "convert lists" >> {
      val sft = SimpleFeatureTypes.createType("test", "foobar:List[String],dtg:Date,*geom:Point:srid=4326")
      val schema = SimpleFeatureParquetSchema(sft)
      schema.getPaths
      success
    }
  }
}
