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

  val feats = (0 until 1000).map(SampleFeatures.buildFeature)

  // Set up CQEngine with no indexes
  val cqNoIndexes = new GeoCQEngine(sft, Seq.empty)
  cqNoIndexes.insert(feats)

  // Set up CQEngine with all indexes
  val cqWithIndexes = new GeoCQEngine(sftWithIndexes, CQIndexType.getDefinedAttributes(sftWithIndexes) , enableFidIndex = true)
  cqWithIndexes.insert(feats)

  def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)

  def getCQEngineCount(filter: Filter, cq: GeoCQEngine) = {
    SelfClosingIterator(cq.query(filter)).size
  }

  def checkFilter(filter: Filter, cq: GeoCQEngine): MatchResult[Int] = {
    val gtCount = getGeoToolsCount(filter)

    val cqCount = getCQEngineCount(filter, cq)

    val msg = s"GT: $gtCount CQ: $cqCount Filter: $filter"
    if (gtCount == cqCount)
      logger.debug(msg)
    else
      logger.error("MISMATCH: "+msg)

    // since GT count is (presumably) correct
    cqCount must equalTo(gtCount)
  }

  def buildFilterTests(name: String, filters: Seq[Filter]): Seq[Fragment] = {
    for (f <- filters) yield {
      s"return correct number of results for $name filter $f (geo-only index)" >> {
        checkFilter(f, cqNoIndexes)
      }
      s"return correct number of results for $name filter $f (various indices)" >> {
        checkFilter(f, cqWithIndexes)
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
