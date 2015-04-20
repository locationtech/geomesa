/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.utils.security

import org.geotools.data.{FeatureSource, FeatureReader}
import org.geotools.feature.FeatureCollection
import org.junit.runner.RunWith
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataStoreSecurityServiceTest extends Specification with Mockito {

  sequential

  "DataStoreSecurityService" should {

    "use default, no security, provider when no provider is registered" >> {
      
      "for a FeatureReader" >> {
        val fr = mock[FeatureReader[SimpleFeatureType, SimpleFeature]]
        val result = DataStoreSecurityService.provider.secure(fr)
        result mustEqual fr
      }
      
      "for a FeatureCollection" >> {
        val fc = mock[FeatureCollection[SimpleFeatureType, SimpleFeature]]
        val result = DataStoreSecurityService.provider.secure(fc)
        result mustEqual fc        
      }

      "for a FeatureSource" >> {
        val fs = mock[FeatureSource[SimpleFeatureType, SimpleFeature]]
        val result = DataStoreSecurityService.provider.secure(fs)
        result mustEqual fs
      }
    }
  }
}
