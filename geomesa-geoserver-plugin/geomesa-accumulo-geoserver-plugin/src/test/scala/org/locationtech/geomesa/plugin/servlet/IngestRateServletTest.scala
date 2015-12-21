/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.plugin.servlet

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestRateServletTest extends Specification {

  "IngestRateServlet" should {

    "format monitor urls correctly" in {

      "suffix xml" in {
        val url = "http://mycloud:50095"
        val formatted = IngestRateServlet.normalizeMonitorUrl(url)
        formatted mustEqual("http://mycloud:50095/xml")
      }

      "suffix xml with trailing slash" in {
        val url = "http://mycloud:50095/"
        val formatted = IngestRateServlet.normalizeMonitorUrl(url)
        formatted mustEqual("http://mycloud:50095/xml")
      }

      "prefix http and suffix port" in {
        val url = "mycloud"
        val formatted = IngestRateServlet.normalizeMonitorUrl(url)
        formatted mustEqual("http://mycloud:50095/xml")
      }

      "prefix http with port" in {
        val url = "mycloud:50095"
        val formatted = IngestRateServlet.normalizeMonitorUrl(url)
        formatted mustEqual("http://mycloud:50095/xml")
      }

      "recognize https" in {
        val url = "https://mycloud:50095"
        val formatted = IngestRateServlet.normalizeMonitorUrl(url)
        formatted mustEqual("https://mycloud:50095/xml")
      }
    }
  }
}