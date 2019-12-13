/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeduplicatingSimpleFeatureIteratorTest extends Specification {

  private val filterFactory = CommonFactoryFinder.getFilterFactory2

  "DeDuplicatingIterator" should {
    "filter on unique elements" in {
      val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")
      val attributes = Array[AnyRef]("POINT(0,0)")
      val features = Seq(
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("1"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("2"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("0"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("1"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("3"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("4"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("1"), false),
        new SimpleFeatureImpl(attributes, sft, filterFactory.featureId("3"), false)
      )
      val deduped = new DeduplicatingSimpleFeatureIterator(features.toIterator).toSeq
      deduped must haveLength(5)
      deduped.map(_.getID) mustEqual Seq("1", "2", "0", "3", "4")
    }
  }
}
