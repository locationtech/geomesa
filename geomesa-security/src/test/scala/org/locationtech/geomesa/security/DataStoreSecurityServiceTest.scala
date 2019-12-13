/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.geotools.data.{FeatureReader, FeatureSource}
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
