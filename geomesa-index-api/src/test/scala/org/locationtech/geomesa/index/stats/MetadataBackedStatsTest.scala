/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetadataBackedStatsTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "trackId:String:index=true,dtg:Date,*geom:Point:srid=4326")

  "MetadataBackedStatsTest" should {
    "work with initial values very close together" in {
      val pt0 = ScalaSimpleFeature.create(sft, "0", s"track-0", "2018-01-01T00:00:00.000Z", "POINT (-87.92926054 41.76166190973163)")
      val pt1 = ScalaSimpleFeature.create(sft, "1", s"track-1", "2018-01-01T01:00:00.000Z", "POINT (-87.92926053956762 41.76166191)")

      val ds = new TestGeoMesaDataStore(false)

      ds.createSchema(sft)
      Seq(pt0, pt1).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
      ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, Array[SimpleFeature](pt0, pt1)))

      ds.stats.writer.updater(sft) must not(throwAn[Exception])
    }
  }
}
