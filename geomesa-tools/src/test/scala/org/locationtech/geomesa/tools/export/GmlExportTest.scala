/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.ByteArrayOutputStream

import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.tools.export.formats.GmlExporter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.xml.XML

@RunWith(classOf[JUnitRunner])
class GmlExportTest extends Specification {

  "GmlExport" >> {
    val sft = SimpleFeatureTypes.createType("GmlExportTest", "name:String,geom:Geometry:srid=4326,dtg:Date")
    val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
    val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

    // create a feature
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    val liveFeature = builder.buildFeature("fid-1")
    val geom = WKTUtils.read("POINT(45.0 49.0)")
    liveFeature.setDefaultGeometry(geom)

    // make sure we ask the system to re-use the provided feature-ID
    liveFeature.getUserData.asScala(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

    "should properly export to GML" >> {
      val out = new ByteArrayOutputStream()
      val gml = new GmlExporter(out)
      gml.start(sft)
      gml.export(Iterator.single(liveFeature))
      gml.close()

      val xml = XML.loadString(new String(out.toByteArray))
      xml.toString must not(contain("null:GmlExportTest"))
      val feat = xml \ "featureMember" \ "GmlExportTest"
      feat must not(beNull)
      val xmlFid = feat \ "@fid"
      xmlFid.text mustEqual "fid-1"
    } 
  }
}
