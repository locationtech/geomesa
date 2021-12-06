/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.{Geometry, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeParserTest extends Specification {

  private val invalidGeomesaAvroSchemaName1 = "Schema1"
  private val invalidGeomesaAvroSchemaJson1 =
    s"""{
       |  "type":"record",
       |  "name":"$invalidGeomesaAvroSchemaName1",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
       |      "${GeomesaAvroGeomDefault.KEY}":"yes"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKT}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POLYGON}",
       |      "${GeomesaAvroGeomDefault.KEY}":"yes",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO8601}"
       |    },
       |    {
       |      "name":"f3",
       |      "type":"string",
       |      "${GeomesaAvroGeomFormat.KEY}":"TWKB",
       |      "${GeomesaAvroGeomType.KEY}":"MultiGeometryCollection",
       |      "${GeomesaAvroGeomDefault.KEY}":"true"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"dd-mm-yyyy"
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaAvroSchema1 = new Schema.Parser().parse(invalidGeomesaAvroSchemaJson1)

  private val validGeomesaAvroSchemaName1 = "Schema2"
  private val validGeomesaAvroSchemaJson1 =
    s"""{
       |  "type":"record",
       |  "name":"$validGeomesaAvroSchemaName1",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double"
       |    },
       |    {
       |      "name":"f3",
       |      "type":["null","string"],
       |      "${GeomesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeomesaAvroGeomType.KEY}":"geometry",
       |      "${GeomesaAvroGeomDefault.KEY}":"true"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO8601}"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaAvroSchema1 = new Schema.Parser().parse(validGeomesaAvroSchemaJson1)

  private val invalidGeomesaAvroSchemaName2 = "Schema3"
  private val invalidGeomesaAvroSchemaJson2 =
    s"""{
       |  "type":"record",
       |  "name":"$invalidGeomesaAvroSchemaName2",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"double"
       |    },
       |    {
       |      "name":"f2",
       |      "type":["null","double","string"]
       |    },
       |    {
       |      "name":"f3",
       |      "type":{"type":"map","values":"string"}
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaAvroSchema2 = new Schema.Parser().parse(invalidGeomesaAvroSchemaJson2)

  private val validGeomesaAvroSchemaName2 = "Schema4"
  private val validGeomesaAvroSchemaJson2 =
    s"""{
       |  "type":"record",
       |  "name":"$validGeomesaAvroSchemaName2",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"bytes"
       |    },
       |    {
       |      "name":"f2",
       |      "type":["null","string"]
       |    },
       |    {
       |      "name":"f3",
       |      "type":"double"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaAvroSchema2 = new Schema.Parser().parse(validGeomesaAvroSchemaJson2)

  "The GeomesaAvroProperty parser for" >> {
    "default geometry" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema1.getField("f2")
        GeomesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema1.getField("f1")
        GeomesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema1.getField("f2")
        GeomesaAvroGeomDefault.parse(field) must beNone
      }

      "return a boolean value if valid" >> {
        val field = validGeomesaAvroSchema1.getField("f3")
        GeomesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "geometry format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema1.getField("f1")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema1.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema1.getField("f4")
        GeomesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema1.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must beSome(GeomesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema1.getField("f2")
        GeomesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema1.getField("f3")
        GeomesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema1.getField("f4")
        GeomesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field1 = validGeomesaAvroSchema1.getField("f1")
        GeomesaAvroGeomType.parse(field1) must beSome(classOf[Point])

        val field3 = validGeomesaAvroSchema1.getField("f3")
        GeomesaAvroGeomType.parse(field3) must beSome(classOf[Geometry])
      }
    }

    "date format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema1.getField("f2")
        GeomesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema1.getField("f4")
        GeomesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema1.getField("f1")
        GeomesaAvroDateFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema1.getField("f4")
        GeomesaAvroDateFormat.parse(field) must beSome(GeomesaAvroDateFormat.ISO8601)
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema1) must
        throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
    }

    "fail to convert a schema without geomesa properties into an SFT when the field type is not supported" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema2) must
        throwAn[AvroSimpleFeatureTypeParser.UnsupportedAvroTypeException]
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val expectedSft = "f1:Point:geomesa.geom.format=WKB,f2:Double,*f3:Geometry:geomesa.geom.format=WKT," +
        "f4:Date:geomesa.date.format=ISO8601"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema1)

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }

    "convert a schema without geomesa avro properties into an SFT" in {
      val expectedSft = "f1:Bytes,f2:String,f3:Double"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema2)

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }
  }
}
