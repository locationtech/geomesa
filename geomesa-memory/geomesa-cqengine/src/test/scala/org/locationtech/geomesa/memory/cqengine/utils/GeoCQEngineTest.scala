/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.index.GeoIndexType
import org.locationtech.geomesa.memory.cqengine.index.param.STRtreeIndexParam
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.core.{Fragment, Fragments}

@RunWith(classOf[JUnitRunner])
class GeoCQEngineTest extends Specification with LazyLogging {

  import SampleFilters._

  lazy val feats = Seq.tabulate(1000)(SampleFeatures.buildFeature)

  // Set up CQEngine with no indexes
  lazy val cqNoIndexes = {
    val cq = new GeoCQEngine(sft, Seq.empty)
    cq.insert(feats)
    cq
  }

  // Set up CQEngine with all indexes
  lazy val cqWithIndexes = {
    val cq = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes), enableFidIndex = true)
    cq.insert(feats)
    cq
  }

  // Set up CQEngine with rtree indexes
  lazy val cqWithSTRtreeIndexes = {
    val attributes = CQIndexType.getDefinedAttributes(sftWithIndexes)
    val cq = new GeoCQEngine(sftWithIndexes, attributes, enableFidIndex = true, GeoIndexType.STRtree)
    cq.insert(feats)
    cq
  }

  // Set up CQEngine with STRtree indexes and node capacity
  lazy val cqWithSTRtreeNodeCapacityIndexes = {
    val attributes = CQIndexType.getDefinedAttributes(sftWithIndexes)
    val geoIndexParam = Some(new STRtreeIndexParam(20))
    val cq = new GeoCQEngine(sftWithIndexes, attributes, enableFidIndex = true, GeoIndexType.STRtree, geoIndexParam)
    cq.insert(feats)
    cq
  }

  // Set up CQEngine with Quadtree indexes
  lazy val cqWithQuadtreeIndexes = {
    val attributes = CQIndexType.getDefinedAttributes(sftWithIndexes)
    val cq = new GeoCQEngine(sftWithIndexes, attributes, enableFidIndex = true, GeoIndexType.QuadTree)
    cq.insert(feats)
    cq
  }

  def checkFilter(filter: Filter, cq: GeoCQEngine): MatchResult[_] = {
    val gtCount = feats.count(filter.evaluate)
    val cqCount = SelfClosingIterator(cq.query(filter)).length
    // since GT count is (presumably) correct
    cqCount mustEqual gtCount
  }

  def buildFilterTests(name: String, filters: Seq[Filter]): Seq[Fragment] = {
    for (f <- filters) yield {
      s"return correct number of results for $name filter $f (geo-only index)" >> {
        checkFilter(f, cqNoIndexes)
      }
      s"return correct number of results for $name filter $f (various indices)" >> {
        checkFilter(f, cqWithIndexes)
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with default config)" >> {
        checkFilter(f, cqWithSTRtreeIndexes)
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with nodecapacity=20)" >> {
        checkFilter(f, cqWithSTRtreeNodeCapacityIndexes)
      }
      s"return correct number of results for $name filter $f (various with QuadTree)" >> {
        checkFilter(f, cqWithQuadtreeIndexes)
      }
    }
  }

  def runFilterTests(name: String, filters: Seq[Filter]): Fragments = Fragments(buildFilterTests(name, filters): _*)

  "GeoCQEngine" should {
    runFilterTests("equality", equalityFilters)
    runFilterTests("special", specialFilters)
    runFilterTests("null", nullFilters)
    runFilterTests("comparable", comparableFilters)
    runFilterTests("temporal", temporalFilters)
    runFilterTests("one level AND", oneLevelAndFilters)
    runFilterTests("one level multiple AND", oneLevelMultipleAndsFilters)
    runFilterTests("one level OR", oneLevelOrFilters)
    runFilterTests("one level multiple OR", oneLevelMultipleOrsFilters)
    runFilterTests("one level NOT", simpleNotFilters)
    runFilterTests("basic spatial predicates", spatialPredicates)
    runFilterTests("attribute predicates", attributePredicates)
    runFilterTests("function predicates", functionPredicates)
  }
}
