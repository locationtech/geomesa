/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat,
  GeomesaAvroGeomDefault, GeomesaAvroGeomFormat, GeomesaAvroGeomType, GeomesaAvroProperty, GeomesaAvroFeatureVisibility}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryFactory, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeParserTest extends Specification {

  private val invalidGeomesaAvroSchemaJson1 =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
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
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_DATE}"
       |    },
       |    {
       |      "name":"f3",
       |      "type":"string",
       |      "${GeomesaAvroGeomFormat.KEY}":"TWKB",
       |      "${GeomesaAvroGeomType.KEY}":"MultiGeometryCollection",
       |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"dd-mm-yyyy"
       |    },
       |    {
       |      "name":"f5",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.EPOCH_MILLIS}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"bytes",
       |      "${GeomesaAvroFeatureVisibility.KEY}":""
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaAvroSchema1 = new Schema.Parser().parse(invalidGeomesaAvroSchemaJson1)

  private val validGeomesaAvroSchemaJson1 =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
       |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.FALSE}"
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
       |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"long",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.EPOCH_MILLIS}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":["null","string"],
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_INSTANT}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"string",
       |      "${GeomesaAvroFeatureVisibility.KEY}":""
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaAvroSchema1: Schema = new Schema.Parser().parse(validGeomesaAvroSchemaJson1)

  private val invalidGeomesaAvroSchemaJson2 =
    s"""{
       |  "type":"record",
       |  "name":"schema3",
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

  private val validGeomesaAvroSchemaJson2 =
    s"""{
       |  "type":"record",
       |  "name":"schema4",
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

  private val geomFactory = new GeometryFactory()
  private val coordinateFactory = CoordinateArraySequenceFactory.instance()

  private def generateCoordinate(x: Double, y: Double): CoordinateSequence = {
    coordinateFactory.create(Array(new Coordinate(x, y)))
  }

  "The GeomesaAvroProperty parser for" >> {
    "default geometry" should {
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
        val field1 = invalidGeomesaAvroSchema1.getField("f2")
        GeomesaAvroDateFormat.parse(field1) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]

        val field2 = invalidGeomesaAvroSchema1.getField("f5")
        GeomesaAvroDateFormat.parse(field2) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
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
        GeomesaAvroDateFormat.parse(field) must beSome(GeomesaAvroDateFormat.EPOCH_MILLIS)
      }
    }

    "feature visibility" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema1.getField("f6")
        GeomesaAvroFeatureVisibility.parse(field) must throwA[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema2.getField("f3")
        GeomesaAvroFeatureVisibility.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema1.getField("f6")
        GeomesaAvroFeatureVisibility.parse(field) must beSome
      }
    }
  }

  "The GeomesaAvroProperty deserializer for " >> {
    "geometry format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f3", "POINT(10 20)")
        GeomesaAvroGeomFormat.deserialize(record, "f3", "InvalidGeomFormat") must
          throwA[GeomesaAvroProperty.DeserializationException[Geometry]]
      }

      "fail if the value cannot be deserialized because the geometry cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f3", "POINT(0 0 0 0 0 0)")
        GeomesaAvroGeomFormat.deserialize(record, "f3", GeomesaAvroGeomFormat.WKT) must
          throwA[GeomesaAvroProperty.DeserializationException[Geometry]]
      }

      "fail if the value cannot be deserialized because the type is incorrect" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f3", "POINT(10 20)")
        GeomesaAvroGeomFormat.deserialize(record, "f3", GeomesaAvroGeomFormat.WKB) must
          throwA[GeomesaAvroProperty.DeserializationException[Geometry]]
      }

      "return the geometry if it can be deserialized" >> {
        "for a point" in {
          val record1 = new GenericData.Record(validGeomesaAvroSchema1)
          val expectedGeom1 = new Point(generateCoordinate(10, 20), geomFactory)
          record1.put("f1", WKBUtils.write(expectedGeom1))
          GeomesaAvroGeomFormat.deserialize(record1, "f1", GeomesaAvroGeomFormat.WKB) mustEqual expectedGeom1
        }

        "for a geometry" in {
          val record2 = new GenericData.Record(validGeomesaAvroSchema1)
          val expectedGeom2 = new Point(generateCoordinate(10, 20), geomFactory).asInstanceOf[Geometry]
          record2.put("f3", "POINT(10 20)")
          GeomesaAvroGeomFormat.deserialize(record2, "f3", GeomesaAvroGeomFormat.WKT) mustEqual expectedGeom2
        }
      }
    }

    "date format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f4", "1638912032")
        GeomesaAvroDateFormat.deserialize(record, "f4", "InvalidDateFormat") must
          throwA[GeomesaAvroProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the date cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f5", "12/07/2021")
        GeomesaAvroDateFormat.deserialize(record, "f5", GeomesaAvroDateFormat.ISO_DATE) must
          throwA[GeomesaAvroProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the type is incorrect" in {
        val record = new GenericData.Record(validGeomesaAvroSchema1)
        record.put("f4", 1000)
        GeomesaAvroDateFormat.deserialize(record, "f4", GeomesaAvroDateFormat.EPOCH_MILLIS) must
          throwA[GeomesaAvroProperty.DeserializationException[Date]]
      }

      "return the date if it can be deserialized" >> {
        "for milliseconds timestamp" in {
          val record1 = new GenericData.Record(validGeomesaAvroSchema1)
          val expectedDate1 = new Date(1638915744897L)
          record1.put("f4", 1638915744897L)
          GeomesaAvroDateFormat.deserialize(record1, "f4", GeomesaAvroDateFormat.EPOCH_MILLIS) mustEqual expectedDate1
        }

        "for a null string" >> {
          val record2 = new GenericData.Record(validGeomesaAvroSchema1)
          val expectedDate2 = null
          record2.put("f5", null)
          GeomesaAvroDateFormat.deserialize(record2, "f5", GeomesaAvroDateFormat.ISO_INSTANT) mustEqual expectedDate2
        }

        "for an ISO datetime string" >> {
          val record3 = new GenericData.Record(validGeomesaAvroSchema1)
          val expectedDate3 = new Date(1638915744897L)
          record3.put("f5", "2021-12-07T17:22:24.897-05:00")
          GeomesaAvroDateFormat.deserialize(record3, "f5", GeomesaAvroDateFormat.ISO_INSTANT) mustEqual expectedDate3
        }
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema1, "test-sft") must
        throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
    }

    "fail to convert a schema without geomesa properties into an SFT when the field type is not supported" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema2, "test-sft") must
        throwAn[AvroSimpleFeatureTypeParser.UnsupportedAvroTypeException]
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val expectedSft = "f1:Point:geomesa.geom.format=WKB,f2:Double,*f3:Geometry:geomesa.geom.format=WKT," +
        "f4:Date:geomesa.date.format=EPOCH_MILLIS,f5:Date:geomesa.date.format=ISO_INSTANT," +
        "f6:String:geomesa.feature.visibility=''"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema1, "test-sft")

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }

    "convert a schema without geomesa avro properties into an SFT" in {
      val expectedSft = "f1:Bytes,f2:String,f3:Double"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema2, "test-sft")

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }
  }
}
