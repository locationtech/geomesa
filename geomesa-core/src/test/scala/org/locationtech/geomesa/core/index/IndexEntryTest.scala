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

package org.locationtech.geomesa.core.index

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexEntryTest extends Specification {

  val now = new DateTime().toDate
  val dummyType = SimpleFeatureTypes.createType("DummyType",
                    s"foo:String,bar:Geometry,baz:Date,$DEFAULT_GEOMETRY_PROPERTY_NAME:Geometry,$DEFAULT_DTG_PROPERTY_NAME:Date,$DEFAULT_DTG_END_PROPERTY_NAME:Date")

  "IndexEntry encoding" should {
    "encode and decode round-trip properly" in {
      // inputs
      val wkt = "POINT (-78.495356 38.075215)"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val dt = now
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(dummyType, List(id, geom, dt, geom, dt, dt), id)

      // output
      val value = IndexEntry.encodeIndexValue(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = IndexEntry.decodeIndexValue(value)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      dt must be equalTo now

    }

    "encode and decode round-trip properly when there is no datetime" in {
      // inputs
      val wkt = "POINT (-78.495356 38.075215)"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val dt: Option[DateTime] = None
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(dummyType, List(id, geom, null, geom, null, null), id)

      // output
      val value = IndexEntry.encodeIndexValue(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = IndexEntry.decodeIndexValue(value)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      dt.isDefined must beFalse
    }

  }
}
