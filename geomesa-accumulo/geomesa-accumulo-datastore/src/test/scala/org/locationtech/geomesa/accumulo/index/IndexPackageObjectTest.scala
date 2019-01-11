/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexPackageObjectTest extends Specification {

  "index" should {
    "compute target schemas from transformation expressions" in {
      val sftName = "targetSchemaTest"
      val defaultSchema = "name:String,geom:Point:srid=4326,dtg:Date"
      val origSFT = SimpleFeatureTypes.createType(sftName, defaultSchema)
      origSFT.setDtgField("dtg")

      val query = new Query(sftName, Filter.INCLUDE, Array("name", "helloName=strConcat('hello', name)", "geom"))
      QueryPlanner.setQueryTransforms(query, origSFT)

      val transform = query.getHints.getTransformSchema
      transform must beSome
      SimpleFeatureTypes.encodeType(transform.get) mustEqual "name:String,helloName:String,*geom:Point:srid=4326"
    }
  }
}
