/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.StringWriter
import java.util.Date

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  def getFCandDSandSFT(sftName: String) = {
    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Geometry:srid=4326,dtg:Date")

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))
    val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, "fid-1")

    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
    featureCollection.add(feature)

    val connector = new MockInstance().getConnector("", new PasswordToken(""))

    val ds = DataStoreFinder
      .getDataStore(Map("connector" -> connector, "tableName" -> sftName, "caching"   -> false))
    ds.createSchema(sft)
    ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore].addFeatures(featureCollection)
    (featureCollection, ds, sft)
  }

  "DelimitedExport" >> {
    val sftName = "DelimitedExportTest"
    val delimitedFCAndDS = getFCandDSandSFT(sftName)
    val featureCollection = delimitedFCAndDS._1
    val ds = delimitedFCAndDS._2
    val sft = delimitedFCAndDS._3

    "should properly export to CSV" >> {
      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("name,geom,dtg"))
      export.write(featureCollection)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "name,geom,dtg"
      data mustEqual "myname,POINT (45 49),1970-01-01 00:00:00"
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("derived=strConcat(name\\, '-test'),geom,dtg"))
      export.write(features)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "derived,geom,dtg"
      data mustEqual "myname-test,POINT (45 49),1970-01-01 00:00:00"
    }

    "should handle escapes" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, ',test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("derived=strConcat(name\\, '\\,test'),geom,dtg"))
      export.write(features)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "derived,geom,dtg"
      data mustEqual "\"myname,test\",POINT (45 49),1970-01-01 00:00:00"
    }
  }

  "Shapefile Export" >> {
    val sftName = "ShapefileExportTest"
    val shpFCAndDS = getFCandDSandSFT(sftName)
    val sft = shpFCAndDS._3

    def checkReplacedAttributes(attrString: String, expectedString: String) = {
      ShapefileExport.replaceGeomInAttributesString(attrString, sft) mustEqual expectedString
    }

    "should transform 'geom' to 'the_geom' when asking for just 'geom'" >> {
      checkReplacedAttributes("geom", "the_geom=geom")
    }

    "should transform 'geom' to 'the_geom' when asking for just 'geom' with weird whitespace" >> {
      checkReplacedAttributes("geom            ", "the_geom=geom")
    }

    "should transform 'geom' in the attributes string when another attribute follows" >> {
      checkReplacedAttributes("geom, name", "the_geom=geom,name")
    }

    "should transform 'geom' in the attributes string when another attribute follows with weird whitespace" >> {
      checkReplacedAttributes("geom     ,      name", "the_geom=geom,name")
    }

    "should transform 'geom' in the attributes string when it follows another attribute" >> {
      checkReplacedAttributes("name, geom", "name,the_geom=geom")
    }

    "should transform 'geom' in the attributes string when it is between two attributes" >> {
      checkReplacedAttributes("name, geom, dtg", "name,the_geom=geom,dtg")
    }

    "should transform 'geom' in the attributes string when it is between two attributes with weird whitespace" >> {
      checkReplacedAttributes("   name       ,    geom ,      dtg   ", "name,the_geom=geom,dtg")
    }

    "should transform 'geom' in the attributes string when it is between two attributes without spaces" >> {
      checkReplacedAttributes("name,geom,dtg", "name,the_geom=geom,dtg")
    }

    "should NOT transform 'the_geom' in the attributes string" >> {
      checkReplacedAttributes("the_geom", "the_geom")
    }

    "should NOT transform and incorrect transform in the query" >> {
      checkReplacedAttributes("name,geom=the_geom,dtg", "name,geom=the_geom,dtg")
    }
  }
}
