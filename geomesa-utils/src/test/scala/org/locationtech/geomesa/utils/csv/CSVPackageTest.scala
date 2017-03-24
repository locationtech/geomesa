/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.csv

import java.io.StringReader

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.csv.CSVParser._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CSVPackageTest
  extends Specification
          with LazyLogging {

  "guessTypes" should {
    def getSchema(name: String, csv: String) = guessTypes(name, new StringReader(csv)).schema

    "recognize int-parsable columns" >> {
      val csv = "int\n1"
      val schema = getSchema("inttest", csv)
      schema mustEqual "int:Integer"
    }

    "recognize double-parsable columns" >> {
      val csv = "double\n1.0"
      val schema = getSchema("doubletest", csv)
      schema mustEqual "double:Double"
    }

    "recognize time-parsable columns" >> {
      val time = new DateTime
      TimeParser.timeFormats.forall { format =>
        val csv = s"time\n${format.print(time)}"
        val schema = getSchema("timetest", csv)
        schema mustEqual "time:Date"
      }
    }

    "recognize point-parsable columns" >> {
      val csv = "point\nPOINT(0.0 0.0)"
      val schema = getSchema("pointtest", csv)
      schema mustEqual s"*point:Point:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }

    "recognize string-parsable columns" >> {
      val csv = "string\nargle"
      val schema = getSchema("stringtest", csv)
      schema mustEqual "string:String"
    }

    "recognize LineStrings"  in {
      val csv = "name\n\"LINESTRING(0 2, 2 0, 8 6)\""
      val schema = getSchema("test", csv)
      schema mustEqual s"*name:LineString:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }
    "recognize Polygons"  in {
      val csv = "name\n\"POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))\""
      val schema = getSchema("test", csv)
      schema mustEqual s"*name:Polygon:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }
    "recognize MultiLineStrings"  in {
      val csv = "name\n\"MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))\""
      val schema = getSchema("test", csv)
      schema mustEqual s"*name:MultiLineString:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }
    "recognize MultiPoints"  in {
      val csv = "name\n\"MULTIPOINT(0 0, 2 2)\""
      val schema = getSchema("test", csv)
      schema mustEqual s"*name:MultiPoint:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }
    "recognize MultiPolygons"  in {
      val csv = "name\n\"MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))\""
      val schema = getSchema("test", csv)
      schema mustEqual s"*name:MultiPolygon:srid=4326:index=true:$OPT_INDEX_VALUE=true"
    }
  }

  "buildFeatureCollection" should {
    val geomSchema = "int:Integer, double:Double, time:Date,* point:Point:srid=4326:index=true, string:String"
    val geomSFT = SimpleFeatureTypes.createType("geomType", geomSchema)
    val geomCSVHeader = "int,double,time,point,string"
    val geomCSVLines = Seq(
      "1,1.0,2014-12-08T16:18:31.031+0000,POINT(0.1 1.2),argle",
      "2,3.0,2014-12-08T16:18:31.031+0000,POINT(2.5 8.1),bargle",
      "5,8.0,2014-12-08T16:18:31.031+0000,POINT(3.2 1.3),foo",
      "2,3.0,2014-12-08T16:18:31.031+0000,POINT(4.5 5.8),bar",
      "1,7.0,2014-12-08T16:18:31.031+0000,POINT(9.1 4.4),baz"
    )
    val geomCSVBody = geomCSVLines.mkString("\n")
    val geomCSV = s"$geomCSVHeader\n$geomCSVBody"

    "parse CSVs using WKTs" >> {
      val fc = buildFeatureCollection(new StringReader(geomCSV), true, geomSFT, None)
      fc.size mustEqual geomCSVLines.size
    }

    "parse CSVs without headers" >> {
      val fc = buildFeatureCollection(new StringReader(geomCSVBody), false, geomSFT, None)
      fc.size mustEqual geomCSVLines.size
    }

    val latlonSchema = "lat:Double, lon:Double, time:Date,* point:Point:srid=4326:index=true"
    val latlonSFT = SimpleFeatureTypes.createType("latlonType", latlonSchema)
    val latlonCSVHeader = "lat,lon,time"
    val latlonCSVLines = Seq(
      "0.1,1.2,2014-12-08T16:18:31.031+0000",
      "2.5,8.1,2014-12-08T16:18:31.031+0000",
      "3.2,1.3,2014-12-08T16:18:31.031+0000",
      "4.5,5.8,2014-12-08T16:18:31.031+0000",
      "9.1,4.4,2014-12-08T16:18:31.031+0000"
                          )
    val latlonCSVBody = latlonCSVLines.mkString("\n")
    val latlonCSV = s"$latlonCSVHeader\n$latlonCSVBody"

    "parse CSVs using LatLon" >> {
      val fc = buildFeatureCollection(new StringReader(latlonCSV), true, latlonSFT, Some(("lat","lon")))
      fc.size mustEqual latlonCSVLines.size
    }
  }
}
