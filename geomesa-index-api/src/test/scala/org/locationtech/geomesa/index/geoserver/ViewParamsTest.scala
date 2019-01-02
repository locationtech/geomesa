/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geoserver

import java.util.Collections

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ViewParamsTest extends Specification {

  import org.locationtech.geomesa.index.conf.QueryHints._

  "ViewParams" should {
    "handle all types of query hints" in {
      def testHint(hint: Hints.Key, name: String, param: String, expected: Any): MatchResult[Any] = {
        val query = new Query()
        query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap(name, param))
        ViewParams.setHints(query)
        query.getHints.get(hint) mustEqual expected
      }

      testHint(QUERY_INDEX, "QUERY_INDEX", "index-test", "index-test")
      testHint(BIN_TRACK, "BIN_TRACK", "track", "track")
      testHint(COST_EVALUATION, "COST_EVALUATION", "stats", CostEvaluation.Stats)
      testHint(DENSITY_BBOX, "DENSITY_BBOX", "[-120.0, -45, 10, -35.01]", new ReferencedEnvelope(-120d, 10d, -45d, -35.01d, CRS_EPSG_4326))
      testHint(ENCODE_STATS, "ENCODE_STATS", "true", true)
      testHint(ENCODE_STATS, "ENCODE_STATS", "false", false)
      testHint(DENSITY_WIDTH, "DENSITY_WIDTH", "640", 640)
      testHint(SAMPLING, "SAMPLING", "0.4", 0.4f)
    }
  }
}
