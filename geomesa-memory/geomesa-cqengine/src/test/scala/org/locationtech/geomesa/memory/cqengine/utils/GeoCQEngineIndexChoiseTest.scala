/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex, WrappedQuadtree, WrappedSTRtree}
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.core.{Fragment, Fragments}

@RunWith(classOf[JUnitRunner])
class GeoCQEngineIndexChoiseTest extends Specification with LazyLogging {

  import SampleFilters._

  System.setProperty("GeoCQEngineDebugEnabled", "true")

  val feats = (0 until 1000).map(SampleFeatures.buildFeature)

  // Set up CQEngine with no indexes
  val cqNoIndexes = new GeoCQEngine(sft, Seq.empty)
  cqNoIndexes.insert(feats)

  // Set up CQEngine with all indexes
  val cqWithIndexes = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes), enableFidIndex = true)
  cqWithIndexes.insert(feats)

  // Set up CQEngine with rtree indexes
  val cqWithSTRtreeIndexes = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes), enableFidIndex = true, GeoIndexType.STRtree)
  cqWithSTRtreeIndexes.insert(feats)

  // Set up CQEngine with STRtree indexes and node capacity
  val cqWithSTRtreeNodeCapacityIndexes = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes), enableFidIndex = true, GeoIndexType.STRtree, Option.apply(new STRtreeIndexParam(20)))
  cqWithSTRtreeNodeCapacityIndexes.insert(feats)

  // Set up CQEngine with Quadtree indexes
  val cqWithQuadtreeIndexes = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes), enableFidIndex = true, GeoIndexType.QuadTree)
  cqWithQuadtreeIndexes.insert(feats)


  def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)

  def getCQEngineCount(filter: Filter, cq: GeoCQEngine) = {
    SelfClosingIterator(cq.query(filter)).size
  }

  def checkFilter(filter: Filter, cq: GeoCQEngine, spatialIndex: Option[Class[_ <: SpatialIndex[_]]]): MatchResult[Int] = {
    val gtCount = getGeoToolsCount(filter)

    val cqCount = getCQEngineCount(filter, cq)

    val msg = s"GT: $gtCount CQ: $cqCount Filter: $filter"
    if (gtCount == cqCount)
      logger.debug(msg)
    else
      logger.error("MISMATCH: " + msg)

    val lastIndexUsed = GeoCQEngine.getLastIndexUsed()
    if (spatialIndex.isDefined) {
      val expected = spatialIndex.get
      lastIndexUsed must not(beNull)
      lastIndexUsed.getClass must equalTo(expected)
    }

    // since GT count is (presumably) correct
    cqCount must equalTo(gtCount)
  }

  def buildFilterTests(name: String, filters: Seq[Filter]): Seq[Fragment] = {

    var spatialIndex: Option[Class[_ <: SpatialIndex[_]]] = Option.empty
    for (f <- filters) yield {
      s"return correct number of results for $name filter $f (geo-only index)" >> {
        checkFilter(f, cqNoIndexes, spatialIndex)
      }
      s"return correct number of results for $name filter $f (various indices)" >> {
        checkFilter(f, cqWithIndexes, Option.apply(classOf[BucketIndex[_]]))
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with default config)" >> {
        checkFilter(f, cqWithSTRtreeIndexes, Option.apply(classOf[WrappedSTRtree[_]]))
      }
      s"return correct number of results for $name filter $f (various with geo-SRTree with nodecapacity=20)" >> {
        checkFilter(f, cqWithSTRtreeNodeCapacityIndexes, Option.apply(classOf[WrappedSTRtree[_]]))
      }
      s"return correct number of results for $name filter $f (various with QuadTree)" >> {
        checkFilter(f, cqWithQuadtreeIndexes, Option.apply(classOf[WrappedQuadtree[_]]))
      }
    }
  }

  def runFilterTests(name: String, filters: Seq[Filter]): Fragments = Fragments(buildFilterTests(name, filters): _*)

  "GeoCQEngine" should {
    runFilterTests("basic spatial predicates", spatialIndexesForIndexChoise)
  }
}
