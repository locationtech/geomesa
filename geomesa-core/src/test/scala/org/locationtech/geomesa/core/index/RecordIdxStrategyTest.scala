/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RecordIdxStrategyTest extends Specification {

  "partitionSubFilters with filterIsId" should {
    "correctly extract multiple IDs" in {
       val strategy = new RecordIdxStrategy
       val idFilter1 = ECQL.toFilter("IN ('val56','val55')")
       val idFilter2 = ECQL.toFilter("IN('val59','val54')")
       val oFilter = ECQL.toFilter("attr2 = val56 AND attr2 = val60")
       val theFilter = ff.and (List(oFilter,idFilter1,idFilter2).asJava)
       val (idFilters, _) = partitionSubFilters(theFilter, filterIsId)
       val areSame = (idFilters.toSet diff Set(idFilter1,idFilter2)).isEmpty
       areSame must beTrue
    }

    "return an empty set when no ID filter is present" in {
       val strategy = new RecordIdxStrategy
       val oFilter = ECQL.toFilter("attr2 = val56 AND attr2 = val60")
       val (idFilters, _) = partitionSubFilters(oFilter, filterIsId)
       idFilters.isEmpty must beTrue
    }
  }
}
