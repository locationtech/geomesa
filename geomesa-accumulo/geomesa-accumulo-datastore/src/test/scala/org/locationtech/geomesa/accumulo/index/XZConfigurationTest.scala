/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.z2.XZ2Index
import org.locationtech.geomesa.index.index.z3.XZ3Index
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class XZConfigurationTest extends Specification with TestWithDataStore {

  val spec = "name:String,dtg:Date,*geom:Polygon:srid=4326;geomesa.xz.precision='10',geomesa.indexes.enabled='xz2,xz3'"

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"$i")
    sf.setAttributes(Array[AnyRef](s"name$i", f"2010-05-07T$i%02d:00:00.000Z",
      s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"))
    sf
  }

  step {
    addFeatures(features)
    ds.stats.generateStats(sft) // re-generate stats so that we get accurate query planning costs
  }

  "XZIndices" should {
    "support configurable precision" >> {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      sft.getXZPrecision mustEqual 10
    }

    "query XZ2Index at configurable precision" >> {
      val filter = "bbox(geom,39,19,41,23)"
      val query = new Query(sftName, ECQL.toFilter(filter))
      forall(ds.getQueryPlan(query))(_.filter.index.name mustEqual XZ2Index.name)
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveSize(4)
      features.map(_.getID.toInt) must containTheSameElementsAs(0 until 4)
    }

    "query XZ3Index at configurable precision" >> {
      val filter = "bbox(geom,39,19,41,23) AND dtg DURING 2010-05-07T01:30:00.000Z/2010-05-07T05:30:00.000Z"
      val query = new Query(sftName, ECQL.toFilter(filter))
      forall(ds.getQueryPlan(query))(_.filter.index.name mustEqual XZ3Index.name)
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveSize(2)
      features.map(_.getID.toInt) must containTheSameElementsAs(2 until 4)
    }
  }
}
