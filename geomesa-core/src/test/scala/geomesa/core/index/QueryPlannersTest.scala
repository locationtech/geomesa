/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.index

import com.vividsolutions.jts.geom.Polygon
import geomesa.utils.geohash.GeoHash
import geomesa.utils.text.WKTUtils
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryPlannersTest extends Specification {
  "QueryPlanner" should {
    "return full ranges for unspecified dates " in {
      val ghPoly =  GeoHash("c23j").bbox.geom match {
        case p: Polygon => p
        case _ => throw new Exception("geohash c23j should have a polygon bounding box")
      }
      DatePlanner(DateTimeFormat.forPattern("MM")).getKeyPlan(SpatialFilter(ghPoly), println) must be equalTo(KeyRange("01", "12"))
      DatePlanner(DateTimeFormat.forPattern("ss")).getKeyPlan(SpatialFilter(ghPoly), println) must be equalTo(KeyRange("00", "59"))
      DatePlanner(DateTimeFormat.forPattern("MM-dd")).getKeyPlan(SpatialFilter(ghPoly), println) must be equalTo(KeyRange("01-01", "12-31"))
    }
    "return apprpriate ranges for date ranges" in {
      val dt1 = new DateTime(2005, 3, 3, 5, 7, DateTimeZone.forID("UTC"))
      val dt2 = new DateTime(2005, 10, 10, 10, 10, DateTimeZone.forID("UTC"))
      val dt3 = new DateTime(2001, 3, 3, 5, 7, DateTimeZone.forID("UTC"))
      val dt4 = new DateTime(2005, 3, 9, 5, 7, DateTimeZone.forID("UTC"))
      val dt5 = new DateTime(2005, 9, 9, 5, 7, DateTimeZone.forID("UTC"))
      DatePlanner(DateTimeFormat.forPattern("MM")).getKeyPlan(DateRangeFilter(dt1, dt2), println) must be equalTo(KeyRangeTiered("03", "10"))
      DatePlanner(DateTimeFormat.forPattern("ss")).getKeyPlan(DateRangeFilter(dt1, dt2), println) must be equalTo(KeyRangeTiered("00", "59"))
      DatePlanner(DateTimeFormat.forPattern("MM")).getKeyPlan(DateRangeFilter(dt3, dt1), println) must be equalTo(KeyRangeTiered("01", "12"))
      DatePlanner(DateTimeFormat.forPattern("MM")).getKeyPlan(DateRangeFilter(dt1, dt4), println) must be equalTo(KeyRangeTiered("03", "03"))
      DatePlanner(DateTimeFormat.forPattern("MM")).getKeyPlan(DateRangeFilter(dt4, dt5), println) must be equalTo(KeyRangeTiered("03", "09"))
    }
    "return appropriate regexes for regex" in {
      val planners = List(ConstStringPlanner("foo"), RandomPartitionPlanner(2), GeoHashKeyPlanner(0,1))
      val cp = CompositePlanner(planners, "~")
      val kp = cp.getKeyPlan(SpatialFilter(WKTUtils.read("POLYGON((-109 31, -115 31, -115 37,-109 37,-109 31))").asInstanceOf[Polygon]), println)
      val expectedKP = KeyRanges(List(
        KeyRange("foo~0~.","foo~0~."),
        KeyRange("foo~0~9","foo~0~9"),
        KeyRange("foo~1~.","foo~1~."),
        KeyRange("foo~1~9","foo~1~9"),
        KeyRange("foo~2~.","foo~2~."),
        KeyRange("foo~2~9","foo~2~9")))
      kp must be equalTo(expectedKP)
    }
  }
}
