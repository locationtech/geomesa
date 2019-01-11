/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class HighCardinalityAttributeOrQueryTest extends Specification with TestWithDataStore {

  val spec = SchemaBuilder.builder()
    .addString("high").withIndex(Cardinality.HIGH)
    .addString("low").withIndex(Cardinality.LOW)
    .addDate("dtg", default = true)
    .addPoint("geom", default = true)
    .spec

  val numFeatures = 10
  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  val features = (0 until numFeatures).map { i =>
    builder.set("geom", WKTUtils.read(s"POINT(45.0 45.$i)"))
    builder.set("dtg", f"2014-01-01T01:00:$i%02d.000Z")
    builder.set("high", "h" + i.toString)
    builder.set("low", "l" + i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData.update(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  addFeatures(features)

  "AccumuloDataStore" should {
    "return correct features for high cardinality OR attribute queries" >> {
      def query(attrPart: String) = {
        val filterString = s"($attrPart) AND BBOX(geom, 40.0,40.0,50.0,50.0) AND dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val filter = ECQL.toFilter(filterString)
        val res = SelfClosingIterator(ds.getFeatureSource("HighCardinalityAttributeOrQueryTest").getFeatures(filter).features)
        res.length mustEqual numFeatures
      }

      val inQuery = s"high in (${(0 until numFeatures).map(i => s"'h$i'").mkString(", ")})"
      val orQuery = (0 until numFeatures).map( i => s"high = 'h$i'").mkString(" OR ")
      Seq(inQuery, orQuery).forall(query)
    }
  }

}
