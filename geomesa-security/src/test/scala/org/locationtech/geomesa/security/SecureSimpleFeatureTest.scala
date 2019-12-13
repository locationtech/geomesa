/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SecureSimpleFeatureTest extends Specification with Mockito {

  sequential

  "SecureSimpleFeature" should {

    val sf = mock[SimpleFeature]

    "be able to access visibility" >> {
      "when not set" >> {
        val userData: java.util.Map[AnyRef, AnyRef] = java.util.Collections.emptyMap()
        sf.getUserData returns userData

        sf.visibility mustEqual None
      }

      "when set" >> {
        val userData: java.util.Map[AnyRef, AnyRef] = java.util.Collections.singletonMap(SecurityUtils.FEATURE_VISIBILITY, "vis")
        sf.getUserData returns userData

        sf.visibility mustEqual Some("vis")
      }
    }

    "be able to set visibility" >> {
      val userData = new java.util.HashMap[AnyRef, AnyRef]
      sf.getUserData returns userData

      sf.visibility = "vis"
      userData.size() mustEqual 1
      userData.get(SecurityUtils.FEATURE_VISIBILITY) mustEqual "vis"
    }

    "be able to clear visibility" >> {
      val userData = new java.util.HashMap[AnyRef, AnyRef]
      sf.getUserData returns userData
      sf.visibility = "vis"

      sf.visibility = None
      userData.size() mustEqual 1
      userData.get(SecurityUtils.FEATURE_VISIBILITY) must beNull
      sf.visibility mustEqual None
    }

  }
}
