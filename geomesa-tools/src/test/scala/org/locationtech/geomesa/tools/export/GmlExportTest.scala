/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.ByteArrayOutputStream

import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.OutputStreamCounter
import org.locationtech.geomesa.tools.export.formats.GmlExporter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.xml.XML

@RunWith(classOf[JUnitRunner])
class GmlExportTest extends Specification {

  val sft = SimpleFeatureTypes.createType("GmlExportTest", "name:String,geom:Geometry:srid=4326,dtg:Date")

  // create a feature
  val feature = ScalaSimpleFeature.create(sft, "fid-1", "myname", "POINT(45.0 49.0)", null)
  // make sure we ask the system to re-use the provided feature-ID
  feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

  "GmlExport" >> {
    "should properly export to GML" >> {
      val out = new ByteArrayOutputStream()
      val gml = GmlExporter(out, null)
      gml.start(sft)
      gml.export(Iterator.single(feature))
      gml.close()

      val xml = XML.loadString(new String(out.toByteArray))
      xml.toString must not(contain("null:GmlExportTest"))
      val feat = xml \ "featureMembers" \ "GmlExportTest"
      feat must not(beNull)
      feat must haveLength(1)
    }
    "should properly export to GML v2" >> {
      val out = new ByteArrayOutputStream()
      val gml = GmlExporter.gml2(out, null)
      gml.start(sft)
      gml.export(Iterator.single(feature))
      gml.close()

      val xml = XML.loadString(new String(out.toByteArray))
      xml.toString must not(contain("null:GmlExportTest"))
      val feat = xml \ "featureMember" \ "GmlExportTest"
      feat must not(beNull)
      val xmlFid = feat \ "@fid"
      xmlFid.text mustEqual "fid-1"
    }
    "should support multiple calls to export" >> {
      val out = new ByteArrayOutputStream()
      val counter = new OutputStreamCounter(out)
      val gml = GmlExporter(counter.stream, counter)
      gml.start(sft)
      gml.export(Iterator.fill(2)(feature))
      gml.export(Iterator.single(feature))
      gml.close()

      val xml = XML.loadString(new String(out.toByteArray))
      xml.toString must not(contain("null:GmlExportTest"))
      val feat = xml \ "featureMembers" \ "GmlExportTest"
      feat must not(beNull)
      feat must haveLength(3)
    }
  }
}
