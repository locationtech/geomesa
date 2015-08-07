/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.process

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ImportProcessTest extends Specification with Logging {
  "getFeatureCollectionDefaultGeometryName" should {
    "correctly identify the default geometry when it is the first geometry, and not explicitly set" >> {
      // construct a feature type in which the first geometry is the default geometry
      val builder = new SimpleFeatureTypeBuilder()
      builder.setName("testType")
      builder.setNamespaceURI("http://www.geotools.org/")
      builder.setSRS("EPSG:4326")
      builder.add("intProperty1", classOf[java.lang.Integer])
      builder.add("stringProperty1", classOf[java.lang.String])
      builder.add("pointProperty1", classOf[org.opengis.geometry.primitive.Point])
      val sft = builder.buildFeatureType()
      val features = new DefaultFeatureCollection("id", sft)

      logger.debug(s"[geometry unset] default geometry:  ${sft.getGeometryDescriptor}")

      val expectedGeomName = Try { sft.getGeometryDescriptor.getName.toString }.toOption
      val actualGeomName = ImportProcess.getFeatureCollectionDefaultGeometryNameOption(features)

      actualGeomName.isEmpty must beTrue
      expectedGeomName.isEmpty must beTrue
    }

    "correctly identify the default geometry when it is the first geometry" >> {
      // construct a feature type in which the first geometry is the default geometry
      val builder = new SimpleFeatureTypeBuilder()
      builder.setName("testType")
      builder.setNamespaceURI("http://www.geotools.org/")
      builder.setSRS("EPSG:4326")
      builder.add("intProperty1", classOf[java.lang.Integer])
      builder.add("stringProperty1", classOf[java.lang.String])
      builder.add("pointProperty1", classOf[org.opengis.geometry.primitive.Point])
      builder.setDefaultGeometry("pointProperty1")
      val sft = builder.buildFeatureType()
      val features = new DefaultFeatureCollection("id", sft)

      logger.debug(s"[geometry first] default geometry:  ${sft.getGeometryDescriptor}")

      val expectedGeomName = Try { sft.getGeometryDescriptor.getName.toString }.toOption
      val actualGeomName = ImportProcess.getFeatureCollectionDefaultGeometryNameOption(features)

      actualGeomName.isEmpty must beFalse
      expectedGeomName.isEmpty must beFalse
      actualGeomName.get must equalTo(expectedGeomName.get)
    }

    "correctly identify the default geometry when it is NOT the first geometry" >> {
      // construct a feature type in which the first geometry is NOT the default geometry
      val builder = new SimpleFeatureTypeBuilder()
      builder.setName("testType")
      builder.setNamespaceURI("http://www.geotools.org/")
      builder.setSRS("EPSG:4326")
      builder.add("intProperty1", classOf[java.lang.Integer])
      builder.add("stringProperty1", classOf[java.lang.String])
      builder.add("pointProperty1", classOf[org.opengis.geometry.primitive.Point])
      builder.add("intProperty2", classOf[java.lang.Integer])
      builder.add("stringProperty2", classOf[java.lang.String])
      builder.add("pointProperty2", classOf[org.opengis.geometry.primitive.Point])
      builder.setDefaultGeometry("pointProperty2")
      val sft = builder.buildFeatureType()
      val features = new DefaultFeatureCollection("id", sft)

      logger.debug(s"[geometry NOT first] default geometry:  ${sft.getGeometryDescriptor}")

      val expectedGeomName = Try { sft.getGeometryDescriptor.getName.toString }.toOption
      val actualGeomName = ImportProcess.getFeatureCollectionDefaultGeometryNameOption(features)

      actualGeomName.isEmpty must beFalse
      expectedGeomName.isEmpty must beFalse
      actualGeomName.get must equalTo(expectedGeomName.get)
    }
  }
}
