/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import java.util.Collections

import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KnnProcessTest extends Specification {

  val process = new KNearestNeighborSearchProcess

  val sft = SimpleFeatureTypes.createType("knn", "*geom:Point:srid=4326")

  val fc = new ListFeatureCollection(sft)

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.setAttribute(0, s"POINT(45 5$i)")
    sf
  }

  step {
    features.foreach(fc.add)
  }

  "KnnProcess" should {
    "manually visit a feature collection" in {
      val input = Collections.singletonList[SimpleFeature](ScalaSimpleFeature.create(sft, "", "POINT (45 55)"))
      val query = new ListFeatureCollection(sft, input)
      val result = SelfClosingIterator(process.execute(query, fc, 3, 0, 0).features).toSeq
      result must containTheSameElementsAs(features.slice(4, 7))
    }
    "manually visit a feature collection smaller than k" in {
      val input = Collections.singletonList[SimpleFeature](ScalaSimpleFeature.create(sft, "", "POINT (45 55)"))
      val query = new ListFeatureCollection(sft, input)
      val result = SelfClosingIterator(process.execute(query, fc, 20, 0, 0).features).toSeq
      result must containTheSameElementsAs(features)
    }
  }
}
