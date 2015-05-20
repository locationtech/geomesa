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

package org.locationtech.geomesa.accumulo.csv

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.csv.DMS._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DMSTest extends Specification {
  "Hemisphere" should {
    "recognize all valid characters" >> {
      Hemisphere('N') mustEqual North
      Hemisphere('n') mustEqual North
      Hemisphere('S') mustEqual South
      Hemisphere('s') mustEqual South
      Hemisphere('E') mustEqual East
      Hemisphere('e') mustEqual East
      Hemisphere('W') mustEqual West
      Hemisphere('w') mustEqual West
    }

    "reject invalid characters" >> {
      Hemisphere('Q') must throwA[IllegalArgumentException]
    }
  }

  "DMS" should {
    val dms = DMS(38,04,31.17,North)

    "parse DMS strings with colons" >> {
      DMS("38:04:31.17N") mustEqual dms
    }

    "parse DMS strings without colons" >> {
      DMS("380431.17N") mustEqual dms
    }

    "parse DMS strings with signs" >> {
      DMS("-38:04:31.17S") mustEqual dms
    }

    "reject DMS strings with too many seconds" >> {
      DMS("38:04:61.17N") must throwA[IllegalArgumentException]
    }

    "reject DMS strings with too many minutes" >> {
      DMS("38:64:31.17N") must throwA[IllegalArgumentException]
    }

    "reject DMS strings with too many degrees" >> {
      DMS("98:04:61.17N") must throwA[IllegalArgumentException]
    }
  }
}
