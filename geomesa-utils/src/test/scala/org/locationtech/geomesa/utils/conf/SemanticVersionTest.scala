/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SemanticVersionTest extends Specification with LazyLogging {

  "SemanticVersion" should {
    "parse normal versions" in {
      SemanticVersion("2.0.0") mustEqual SemanticVersion(2, 0, 0)
      SemanticVersion("1.3.5") mustEqual SemanticVersion(1, 3, 5)
    }
    "parse pre-release versions" in {
      SemanticVersion("2.0.0-SNAPSHOT") mustEqual SemanticVersion(2, 0, 0, Some("SNAPSHOT"))
      SemanticVersion("1.0.0-alpha") mustEqual SemanticVersion(1, 0, 0, Some("alpha"))
      SemanticVersion("1.0.0-alpha.1") mustEqual SemanticVersion(1, 0, 0, Some("alpha.1"))
      SemanticVersion("1.0.0-0.3.7") mustEqual SemanticVersion(1, 0, 0, Some("0.3.7"))
      SemanticVersion("1.0.0-x.7.z.92") mustEqual SemanticVersion(1, 0, 0, Some("x.7.z.92"))
    }
    "parse build versions" in {
      SemanticVersion("1.0.0-alpha+001") mustEqual SemanticVersion(1, 0, 0, Some("alpha"), Some("001"))
      SemanticVersion("1.0.0+20130313144700") mustEqual SemanticVersion(1, 0, 0, build = Some("20130313144700"))
      SemanticVersion("1.0.0-beta+exp.sha.5114f85") mustEqual SemanticVersion(1, 0, 0, Some("beta"), Some("exp.sha.5114f85"))
    }
    "sort correctly" in {
      val sorted = Seq (
        Seq("1.0.0", "2.0.0", "2.1.0", "2.1.1"),
        Seq("1.0.0-alpha", "1.0.0"),
        Seq("1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0")
      ).map(_.map(SemanticVersion.apply))
      // should already be sorted, so verify sorting doesn't change order
      foreach(sorted)(s => s.sorted mustEqual s)
    }
    "handle non-semantic releases" in {
      SemanticVersion("1.3.5.1") must throwAn[Exception]
      SemanticVersion("1.3.5.1", lenient = true) mustEqual SemanticVersion(1, 3, 5)
    }
  }
}
