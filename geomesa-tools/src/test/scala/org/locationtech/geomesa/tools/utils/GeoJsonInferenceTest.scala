/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.io.ByteArrayInputStream

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonInferenceTest extends Specification {

  val correctSpec = "name:String,id:Integer,decimal:Double,status:Boolean,*geometry:Point"
  val correctSft: SimpleFeatureType = SimpleFeatureTypes.createType("geojson", correctSpec)

  "GeoJsonInference" should {
    sequential
    "get sft from json" >> {
      val filepath = getClass.getResource("/convert/geojson-data.json").getPath
      val sft = GeoJsonInference.inferSft(filepath)
      SimpleFeatureTypes.encodeType(sft) mustEqual correctSpec
    }

    "handle missing geometry" >> {
      val geojson = """[{
        |  "type": "Feature",
        |  "properties": {
        |    "name": "Dinagat Islands",
        |    "id": 123,
        |    "decimal": 1.23,
        |    "status": false
        |  }
        |}]""".stripMargin
      val is = new ByteArrayInputStream(geojson.getBytes())
      GeoJsonInference.inferSft(is) must throwAn[UnsupportedOperationException]
    }

    "handle missing attributes" >> {
      val geojson = """
        | [{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Dinagat Islands",
        |    "id": 123,
        |    "decimal": 1.23,
        |    "status": false
        |  }
        |},
        |{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Wuhu Islands",
        |    "decimal": 1.2
        |  }
        |}]""".stripMargin
      val is = new ByteArrayInputStream(geojson.getBytes())
      val sft = GeoJsonInference.inferSft(is)
      sft.getAttributeDescriptors.containsAll(correctSft.getAttributeDescriptors) mustEqual true
    }

    "handle conflicting types" >> {
      val geojson = """
        |[{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Galapagos Islands",
        |    "decimal": 12
        |  }
        |},
        |{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Wuhu Islands",
        |    "decimal": 1.2
        |  }
        |}]""".stripMargin

      val is = new ByteArrayInputStream(geojson.getBytes())
      val sft = GeoJsonInference.inferSft(is)
      sft.getDescriptor("decimal").getType.getBinding mustEqual classOf[java.lang.Double]
    }

    "parse date strings" >> {
      val geojson = """
        |[{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Galapagos Islands",
        |    "dtg": "2018-04-25T12:00:00"
        |  }
        |},
        |{
        |  "type": "Feature",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [125.6, 10.1]
        |  },
        |  "properties": {
        |    "name": "Wuhu Islands",
        |    "dtg": "2018-04-25T11:00:00"
        |  }
        |}]""".stripMargin

      val is = new ByteArrayInputStream(geojson.getBytes())
      val sft = GeoJsonInference.inferSft(is)
      sft.getDescriptor("dtg").getType.getBinding mustEqual classOf[java.util.Date]
    }

  }
}
