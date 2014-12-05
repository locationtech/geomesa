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

package org.locationtech.geomesa.core.csv

import java.lang.{Double => jDouble, Integer => jInt}
import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.csv.Parsable.TimeIsParsable
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.scalacheck.{Arbitrary, Gen}

object CSVTestUtils {
  implicit val randomString  = Arbitrary { for (s <- Gen.alphaStr if !s.isEmpty) yield s }
  implicit val randomFormat  = Arbitrary { Gen.oneOf(TimeIsParsable.timeFormats) }
  implicit val randomDate    = Arbitrary {
                                           for {
                                             s <- Gen.choose(0, 100000000000000l)
                                           } yield new DateTime(s).withZone(DateTimeZone.UTC)
                                         }

  val geomSchema = "int:Integer, double:Double, time:Date,* point:Point:srid=4326:index=true, string:String"
  val geomSFT = SimpleFeatureTypes.createType("geomType", geomSchema)
  val geomCSVHeader = "int,double,time,point,string"
  case class GeomSFTRecord(int: jInt, double: jDouble, time: Date, point: Point, string: String) {
    val datestr = TimeIsParsable.timeFormats.head.print(new DateTime(time.getTime))
    val geomstr = WKTUtils.write(point)
    val csvLine = s"$int,$double,$datestr,$geomstr,$string"
  }
  implicit val geomSFTRecord = Arbitrary {
    for {
      i   <- Arbitrary.arbitrary[Int]
      d   <- Arbitrary.arbitrary[Double]
      t   <- Arbitrary.arbitrary[DateTime]
      lat <- Arbitrary.arbitrary[Double]
      lon <- Arbitrary.arbitrary[Double]
      s   <- Arbitrary.arbitrary[String]
    } yield GeomSFTRecord(new jInt(i),
                          new jDouble(d),
                          t.toDate,
                          WKTUtils.read(s"POINT($lon $lat)").asInstanceOf[Point],
                          s)
                                         }

  val latlonSchema = "lat:Double, lon:Double, time:Date,* point:Point:srid=4326:index=true"
  val latlonSFT = SimpleFeatureTypes.createType("latlonType", latlonSchema)
  val latlonCSVHeader = "lat,lon,time"
  case class LatLonSFTRecord(lat: jDouble, lon: jDouble, time: Date) {
    val datestr = TimeIsParsable.timeFormats.head.print(new DateTime(time.getTime))
    val csvLine = s"$lat,$lon,$datestr"
  }
  implicit val latlonSFTRecord = Arbitrary {
    for {
      lat <- Arbitrary.arbitrary[Double]
      lon <- Arbitrary.arbitrary[Double]
      t   <- Arbitrary.arbitrary[DateTime]
    } yield LatLonSFTRecord(new jDouble(lat),
                            new jDouble(lon),
                            t.toDate)
                                           }
}
