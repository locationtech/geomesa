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

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.slf4j.Logging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SynchronizedQuadtreeTest extends Specification with Logging {

  val qt = new SynchronizedQuadtree

  "Quadtree" should {
    "be thread safe" in {
      val pt = WKTUtils.read("POINT(45 50)")
      val env = pt.getEnvelopeInternal
      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal
      val t1 = new Thread(new Runnable() {
        override def run() = {
          var i = 0
          while (i < 1000) {
            qt.insert(env, pt)
            Thread.sleep(1)
            i += 1
          }
        }
      })
      val t2 = new Thread(new Runnable() {
        override def run() = {
          var i = 0
          while (i < 1000) {
            qt.query(wholeWorld)re
            Thread.sleep(1)
            i += 1
          }
        }
      })
      t1.start()
      t2.start()
      t1.join()
      t2.join()
      success
    }
  }
}



