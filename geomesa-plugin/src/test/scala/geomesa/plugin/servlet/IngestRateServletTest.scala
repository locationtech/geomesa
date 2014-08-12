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


package geomesa.plugin.servlet

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