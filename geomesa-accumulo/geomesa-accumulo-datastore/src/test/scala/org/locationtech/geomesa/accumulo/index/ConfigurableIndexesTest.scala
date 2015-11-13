/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.tables.AvailableTables
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
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
    .withIndexes(AvailableTables.Z3TableSchemeStr)
    .build("ConfigurableIndexesTest")

  val splitter = new QueryFilterSplitter(sft)

  val ff = CommonFactoryFinder.getFilterFactory2

  val geom                = "BBOX(geom,40,40,50,50)"
  val geom2               = "BBOX(geom,60,60,70,70)"
  val indexedAttr         = "attr2 = 'test'"
  val dtg                 = "dtg DURING 2014-01-01T00:00:00Z/2014-01-01T23:59:59Z"

  def and(clauses: String*) = ff.and(clauses.map(ECQL.toFilter))
  def or(clauses: String*)  = ff.or(clauses.map(ECQL.toFilter))
  def f(filter: String)     = ECQL.toFilter(filter)


  implicit def filterToString(f: Filter): String = ECQL.toCQL(f)
  implicit def stringToFilter(f: String): Filter = ECQL.toFilter(f)

  "AccumuloDataStore" should {

    "fall back to records for ST queries" >> {
      "spatial" >> {
        val filter = f(geom)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.RECORD
        options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
        options.head.filters.head.secondary mustEqual Some(filter)
      }
    }

    "fall back to records table on simple ands" >> {
      "spatial" >> {
        val filter = and(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.RECORD
        options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
        options.head.filters.head.secondary mustEqual Some(filter)
      }
      "with multiple spatial clauses" >> {
        val filter = or(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        forall(options.head.filters)(_.strategy mustEqual StrategyType.RECORD)
        options.head.filters.map(_.primary) must containTheSameElementsAs(Seq(Seq(Filter.INCLUDE)))
        forall(options.head.filters)(_.secondary mustEqual Some(filter))
      }
      "with spatiotemporal and indexed attribute clauses" >> {
        val filter = or(geom, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.map(_.strategy) must containTheSameElementsAs(Seq(StrategyType.RECORD))
        options.head.filters.find(_.strategy == StrategyType.RECORD).get.primary mustEqual Seq(Filter.INCLUDE)
        forall(options.head.filters)(_.secondary mustEqual Some(filter))
      }
      "with ORs" >> {
        val filter = or(geom, or(dtg, indexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.map(_.strategy) must
          containTheSameElementsAs(Seq(StrategyType.RECORD))
        options.head.filters.map(_.primary) must
          containTheSameElementsAs(Seq(Seq(Filter.INCLUDE)))
        forall(options.head.filters)(_.secondary mustEqual Some(or(or(geom, indexedAttr), dtg)))
      }
    }
  }
}
