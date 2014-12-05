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

import java.io.StringReader

import com.typesafe.scalalogging.slf4j.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.junit.runner.RunWith
import org.scalacheck.{Gen, Arbitrary, Prop}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, MINUTES}

@RunWith(classOf[JUnitRunner])
class CSVPackageTest
  extends Specification
          with ScalaCheck
          with Logging {

  "guessTypes" should {
    def getSchema(name: String, csv: String) =
      Await.result(guessTypes(name, new StringReader(csv)), Duration(1, MINUTES)).schema

    "recognize int-parsable columns" >> {
      val recognizesInts = Prop.forAll { (i: Int) =>
        val csv = s"int\n$i"
        val schema = getSchema("inttest", csv)
        schema mustEqual "int:Integer"
                                       }
      check(recognizesInts)
    }

    "recognize double-parsable columns" >> {
      val recognizesDoubles = Prop.forAll { (d: Double) =>
        val csv = s"double\n$d"
        val schema = getSchema("doubletest", csv)
        schema mustEqual "double:Double"
                                          }
      check(recognizesDoubles)
    }

    "recognize time-parsable columns" >> {
      import org.locationtech.geomesa.core.csv.CSVTestUtils.{randomDate, randomFormat}
      val recognizesTimes = Prop.forAll { (date: DateTime, format: DateTimeFormatter) =>
        val csv = s"time\n${format.print(date)}"
        val schema = getSchema("timetest", csv)
        schema mustEqual "time:Date"
                                        }
      check(recognizesTimes)
    }

    "recognize point-parsable columns" >> {
      val recognizesPoints = Prop.forAll { (lat: Double, lon: Double) =>
        val csv = s"point\nPOINT($lon $lat)"
        val schema = getSchema("pointtest", csv)
        schema mustEqual "*point:Point:srid=4326:index=true"
                                         }
      check(recognizesPoints)
    }

    "recognize string-parsable columns" >> {
      import org.locationtech.geomesa.core.csv.CSVTestUtils.randomString
      val recognizesStrings = Prop.forAll { (s: String) =>
        val csv = s"string\n$s"
        val schema = getSchema("stringtest", csv)
        schema mustEqual "string:String"
                                          }
      check(recognizesStrings)
    }

  }

  "buildFeatureCollection" should {
    "parse CSVs using WKTs" >> {
      import CSVTestUtils.{GeomSFTRecord, geomCSVHeader, geomSFT, geomSFTRecord}
      implicit val records = Arbitrary { Gen.nonEmptyListOf(Arbitrary.arbitrary[GeomSFTRecord]) }
      val parsesGeomCSV = Prop.forAll { (records: List[GeomSFTRecord]) =>
        val csv = records.map(_.csvLine).mkString(s"$geomCSVHeader\n","\n","")
        val fc = buildFeatureCollection(new StringReader(csv), geomSFT, None).
                 recover {case ex => logger.error("failed to parse", ex); throw ex }.
                 get
        fc.size mustEqual records.size
                                  }
      check(parsesGeomCSV)
    }

    "parse CSVs using LatLon" >> {
      import CSVTestUtils.{LatLonSFTRecord, latlonCSVHeader, latlonSFT, latlonSFTRecord}
      implicit val records = Arbitrary { Gen.nonEmptyListOf(Arbitrary.arbitrary[LatLonSFTRecord]) }
      val parsesLatLonCSV = Prop.forAll { (records: List[LatLonSFTRecord]) =>
        val csv = records.map(_.csvLine).mkString(s"$latlonCSVHeader\n","\n","")
        val fc = buildFeatureCollection(new StringReader(csv), latlonSFT, Some(("lat","lon"))).
                 recover {case ex => logger.error("failed to parse", ex); throw ex }.
                 get
        fc.size mustEqual records.size
                                  }
      check(parsesLatLonCSV)
    }
  }
}
