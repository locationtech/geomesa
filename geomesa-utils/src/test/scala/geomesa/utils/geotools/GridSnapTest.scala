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


package geomesa.utils.geotools

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GridSnapTest extends Specification with Logging {

  "GridSnap" should {
    "create a gridsnap around a given bbox" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 100, 100)

      gridSnap must not beNull
    }

    "compute a SimpleFeatureSource Grid over the bbox" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 10, 10)

      val grid = gridSnap.generateCoverageGrid

      grid must not beNull

      val featureIterator = grid.getFeatures.features

      val gridLength = featureIterator.length

      gridLength should be equalTo 100

    }

    "compute a sequence of points between various sets of coordinates" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 10, 10)

      val resultDiagonal = gridSnap.genBresenhamCoordSet(0, 0, 9, 9).toList
      resultDiagonal must not beNull
      val diagonalLength = resultDiagonal.length
      diagonalLength should be equalTo 9

      val resultVeritcal = gridSnap.genBresenhamCoordSet(0, 0, 0, 9).toList
      resultVeritcal must not beNull
      val verticalLength = resultVeritcal.length
      verticalLength should be equalTo 9

      val resultHorizontal = gridSnap.genBresenhamCoordSet(0, 0, 9, 0).toList
      resultHorizontal must not beNull
      val horizontalLength = resultHorizontal.length
      horizontalLength should be equalTo 9

      val resultSamePoint = gridSnap.genBresenhamCoordSet(0, 0, 0, 0).toList
      resultSamePoint must not beNull
      val samePointLength = resultSamePoint.length
      samePointLength should be equalTo 1
    }

  }

}
