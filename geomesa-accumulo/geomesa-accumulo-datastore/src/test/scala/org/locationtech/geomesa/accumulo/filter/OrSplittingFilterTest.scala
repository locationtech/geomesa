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

package org.locationtech.geomesa.accumulo.filter

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.FilterUtils._
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

object OrSplittingFilterTest {
  val geom1: Filter = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
  val geom2: Filter = "INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))"
  val date1: Filter = "(dtg between '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')"
}

import org.locationtech.geomesa.accumulo.filter.OrSplittingFilterTest._

@RunWith(classOf[JUnitRunner])
class OrSplittingFilterTest extends Specification {
  val osf = new OrSplittingFilter
  def splitFilter(f: Filter) = osf.visit(f, null)

  "The OrSplittingFilter" should {

    "not do anything to filters without a top-level OR" in {
      val filterStrings = Seq(geom1, geom1 && date1, 1 && 2, (3 && 4).!, (1 || 3).!)

      forall(filterStrings) { f => Seq(f) mustEqual splitFilter(f) }
    }

    "split an OR into two pieces" in {
      val orStrings = Seq(geom1 || geom2, geom2 || date1, 1 || 2, geom1 || 3)

      forall(orStrings) { f =>
        splitFilter(f).size mustEqual 2
      }
    }

    "recursively split nested ORs" in {
      val nested = Seq((geom1 || date1) || geom2, 1 || 2 || 3, 1 || (2 && 3) || 4, 1 || (geom2 || date1))
      forall(nested) { f =>
        splitFilter(f).size mustEqual 3
      }
    }

    "not run through lower-level filters" in {
      val filter = (3 || 4).! || (1 && 2)
      splitFilter(filter).size mustEqual 2
    }
  }
}
