/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.index

import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data.DataUtilities
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormattersTest extends Specification {
  "PartitionTextFormatter" should {
    val numTrials = 100

    val featureType = SimpleFeatureTypes.createType("TestFeature",
      "the_geom:Point,lat:Double,lon:Double,date:String")
    val featureWithId = DataUtilities.createFeature(
      featureType, "fid1=POINT(-78.1 38.2)|38.2|-78.1|2014-03-20T07:28:00.0Z" )

    val partitionTextFormatter = PartitionTextFormatter[SimpleFeature](99)

    "map features with non-null identifiers to fixed partitions" in {
      val shardNumbers = (1 to numTrials).map(trial =>
        partitionTextFormatter.format(featureWithId)
      ).toSet

      shardNumbers.size must be equalTo 1
    }
  }
}
