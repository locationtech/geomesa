/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.accumulo.data.AccumuloFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttributeTableTest extends Specification with TestWithDataStore {

  override val spec = "name:String:index=true,age:Integer:index=true,*geom:Point:srid=4326,dtg:Date:index=true;" +
      "override.index.dtg.join=true"

  addFeatures(Seq(
    {
      val sf = new ScalaSimpleFeature("0", sft)
      sf.setAttribute("geom", "POINT (45 55)")
      sf.setAttribute("dtg", "2016-01-01T12:00:00.000Z")
      sf
    },
    {
      val sf = new ScalaSimpleFeature("1", sft)
      sf.setAttribute("geom", "POINT (45 55)")
      sf.setAttribute("dtg", "2016-01-01T14:00:00.000Z")
      sf
    }
  ))

  "AttributeTable" should {

    "encode mutations for attribute index" in {
      val feature = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id1")
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      feature.setDefaultGeometry(geom)
      feature.setAttribute("name","fred")
      feature.setAttribute("age",50.asInstanceOf[Any])

      val toWrite = AccumuloFeature.wrapper(sft, "")(feature)
      val mutations = AttributeIndex.writer(sft, ds)(toWrite)
      mutations.size mustEqual 2 // for null date
      mutations.map(_.getUpdates.size()) must contain(beEqualTo(1)).foreach
      mutations.map(_.getUpdates.get(0).isDeleted) must contain(beEqualTo(false)).foreach
    }

    "encode mutations for delete attribute index" in {
      val descriptors = sft.getAttributeDescriptors

      val feature = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id1")
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      feature.setDefaultGeometry(geom)
      feature.setAttribute("name","fred")
      feature.setAttribute("age",50.asInstanceOf[Any])

      val toWrite = AccumuloFeature.wrapper(sft, "")(feature)
      val mutations = AttributeIndex.remover(sft, ds)(toWrite)
      mutations.size mustEqual 2 // for null date
      mutations.map(_.getUpdates.size()) must contain(beEqualTo(1)).foreach
      mutations.map(_.getUpdates.get(0).isDeleted) must contain(beEqualTo(true)).foreach
    }

    "use the attribute strategy for between on indexed dates" in {
      val filter = ECQL.toFilter("dtg BETWEEN '2016-01-01T11:00:00.000Z' and '2016-01-01T13:00:00.000Z'")
      val query = new Query(sftName, filter, Array("geom", "dtg"))
      val plan = ds.getQueryPlan(query)
      plan must haveLength(1)
      plan.head.filter.index mustEqual AttributeIndex
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head.getID mustEqual "0"
    }
  }

}
