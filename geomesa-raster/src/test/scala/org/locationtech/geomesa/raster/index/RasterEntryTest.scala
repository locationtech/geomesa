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

package org.locationtech.geomesa.raster.index

import java.util.Date

import org.apache.accumulo.core.data.Key
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterEntryTest extends Specification {

  sequential

  val now = new DateTime().toDate
  val emptyByte = Array.empty[Byte]

  def makeKey(cq: Array[Byte]): Key = new Key(emptyByte, emptyByte, cq, emptyByte, Long.MaxValue)

  "RasterEntry" should {

    "encode and decode Raster meta-data properly" in {
      val wkt = "POLYGON ((10 0, 10 10, 0 10, 0 0, 10 0))"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val date = now

      // output encoded meta data
      val cqMetaData = RasterEntry.encodeIndexCQMetadata(id, geom, Some(date))

      // convert CQ Array[Byte] to Key (a key with everything as a null except CQ)
      val keyWithCq = makeKey(cqMetaData)

      // decode metadata from key
      val decoded = RasterEntry.decodeIndexCQMetadata(keyWithCq)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      decoded.date.get must be equalTo now
    }

    "encode and decode Raster meta-data properly when there is no datetime" in {
      val wkt = "POLYGON ((10 0, 10 10, 0 10, 0 0, 10 0))"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val dt: Option[Date] = None

      // output encoded meta data
      val cqMetaData = RasterEntry.encodeIndexCQMetadata(id, geom, dt)

      // convert CQ Array[Byte] to Key (a key with everything as a null except CQ)
      val keyWithCq = makeKey(cqMetaData)

      // decode metadata from key
      val decoded = RasterEntry.decodeIndexCQMetadata(keyWithCq)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      dt.isDefined must beFalse
    }

  }

}
