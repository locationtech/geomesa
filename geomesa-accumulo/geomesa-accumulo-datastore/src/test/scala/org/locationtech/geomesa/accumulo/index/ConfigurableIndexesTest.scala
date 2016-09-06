/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index.z3.Z3Index
import org.locationtech.geomesa.index.api.FilterSplitter
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ConfigurableIndexesTest extends Specification {

  val sft = new SftBuilder()
    .date("dtg", default = true)
    .point("geom", default = true)
    .withIndexes(AccumuloFeatureIndex.Schemes.Z3TableScheme)
    .build("ConfigurableIndexesTest")

  val splitter = new FilterSplitter(sft, AccumuloFeatureIndex.indices(sft))

  val ff = CommonFactoryFinder.getFilterFactory2

  val geom                = "BBOX(geom,40,40,50,50)"
  val geom2               = "BBOX(geom,60,60,70,70)"
  val indexedAttr         = "attr2 = 'test'"
  val dtg                 = "dtg DURING 2014-01-01T00:00:00Z/2014-01-01T23:59:59Z"

  def and(clauses: String*) = ff.and(clauses.map(ECQL.toFilter))
  def or(clauses: String*)  = ff.or(clauses.map(ECQL.toFilter))
  def f(filter: String)     = ECQL.toFilter(filter)

  def testFallback(filter: Filter) = {
    val options = splitter.getQueryOptions(filter)
    options must haveLength(1)
    options.head.strategies must haveLength(1)
    options.head.strategies.head.index mustEqual Z3Index
    options.head.strategies.head.primary must beNone
    options.head.strategies.head.secondary must beSome(filter)
  }

  "AccumuloDataStore" should {
    "be able to use z3 for everything" >> {
      "spatial" >>                            { testFallback(f(geom)) }
      "spatial ands" >>                       { testFallback(and(geom, geom2)) }
      "spatial ors" >>                        { testFallback(or(geom, geom2)) }
      "spatial and attribute ors" >>          { testFallback(or(geom, indexedAttr)) }
      "spatial temporal and attribute ors" >> { testFallback(or(geom, dtg, indexedAttr)) }
    }
  }
}
