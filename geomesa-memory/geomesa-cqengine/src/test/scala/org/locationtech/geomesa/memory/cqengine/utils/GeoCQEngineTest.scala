/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures._
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineTest extends Specification with LazyLogging {
  "GeoCQEngine" should {
    "return correct number of results" >> {
      import SampleFilters._
      val feats = (0 until 1000).map(SampleFeatures.buildFeature)

      // Set up CQEngine with no indexes
      val cqNoIndexes = new GeoCQEngine(sft, enableGeomIndex=false)
      cqNoIndexes.addAll(feats)

      // Set up CQEngine with all indexes
      val cqWithIndexes = new GeoCQEngine(sftWithIndexes, enableFidIndex=true, enableGeomIndex=true)
      cqWithIndexes.addAll(feats)

      def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)

      def getCQEngineCount(filter: Filter, cq: GeoCQEngine) = {
        cq.getReaderForFilter(filter).toIterator.size
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

      def runFilterTests(name: String, filters: Seq[Filter]) = {
        examplesBlock {
          for (f <- filters) {
            s"$name filter $f (geo-only index)" in {
              checkFilter(f, cqNoIndexes)
            }
            s"$name filter $f (various indices)" in {
              checkFilter(f, cqWithIndexes)
            }
          }
        }
      }

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


    }
  }
}
