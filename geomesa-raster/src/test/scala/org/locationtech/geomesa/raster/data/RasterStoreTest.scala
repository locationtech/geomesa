/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.data

import org.locationtech.geomesa.raster.RasterTestUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.feature.Raster
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

//TODO: WCS: Improve this integration test by dealing with issues ID'd below
// GEOMESA-571
@RunWith(classOf[JUnitRunner])
class RasterStoreTest extends Specification {

  sequential

  var testIteration = 0

  def getNewIteration() = {
    testIteration += 1
    s"testRSTTable_$testIteration"
  }
  //TODO: WCS: need tests to show finding one image, no images and several images,

  "RasterStore" should {
    "create a Raster Store" in {
      val tableName = getNewIteration()
      val theStore = RasterTestUtils.createRasterStore(tableName)

      // populate store
      val testRaster = RasterTestUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 50, 0, 50)

      theStore must beAnInstanceOf[RasterStore]
      val theIterator = theStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }
  }
}
