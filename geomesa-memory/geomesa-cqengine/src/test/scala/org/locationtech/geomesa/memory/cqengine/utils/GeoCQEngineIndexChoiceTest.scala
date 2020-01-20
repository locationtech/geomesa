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
import org.locationtech.geomesa.memory.cqengine.index.param.STRtreeIndexParam
import org.locationtech.geomesa.memory.cqengine.index.{AbstractGeoIndex, GeoIndexType}
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex, WrappedQuadtree, WrappedSTRtree}
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.core.{Fragment, Fragments}

@RunWith(classOf[JUnitRunner])
class GeoCQEngineIndexChoiceTest extends Specification with LazyLogging {

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

  def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)

  def getCQEngineCount(filter: Filter, cq: GeoCQEngine) = {
    SelfClosingIterator(cq.query(filter)).size
  }

  def checkFilter(filter: Filter, cq: GeoCQEngine, spatialIndex: Option[Class[_ <: SpatialIndex[_]]]): MatchResult[_] = {
    AbstractGeoIndex.lastUsed.remove()

    val gtCount = feats.count(filter.evaluate)
    val cqCount = SelfClosingIterator(cq.query(filter)).length
    // since GT count is (presumably) correct
    cqCount mustEqual gtCount

    foreach(spatialIndex) { i =>
      AbstractGeoIndex.lastUsed.get must not(beNull)
      AbstractGeoIndex.lastUsed.get.getClass mustEqual i
    }
  }

  def buildFilterTests(name: String, filters: Seq[Filter]): Seq[Fragment] = {
    for (f <- filters) yield {
      s"return correct number of results for $name filter $f (geo-only index)" >> {
        checkFilter(f, cqNoIndexes, None)
      }
      s"return correct number of results for $name filter $f (various indices)" >> {
        checkFilter(f, cqWithIndexes, Some(classOf[BucketIndex[_]]))
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with default config)" >> {
        checkFilter(f, cqWithSTRtreeIndexes, Some(classOf[WrappedSTRtree[_]]))
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with nodecapacity=20)" >> {
        checkFilter(f, cqWithSTRtreeNodeCapacityIndexes, Some(classOf[WrappedSTRtree[_]]))
      }
      s"return correct number of results for $name filter $f (various with QuadTree)" >> {
        checkFilter(f, cqWithQuadtreeIndexes, Some(classOf[WrappedQuadtree[_]]))
      }
    }
  }

  def runFilterTests(name: String, filters: Seq[Filter]): Fragments = Fragments(buildFilterTests(name, filters): _*)

  "GeoCQEngine" should {
    runFilterTests("basic spatial predicates", spatialIndexesForIndexChoise)
  }
}
