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
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroCardinality, GeomesaAvroDateDefault, GeomesaAvroDateFormat, GeomesaAvroDeserializableEnumProperty, GeomesaAvroFeatureVisibility, GeomesaAvroGeomDefault, GeomesaAvroGeomFormat, GeomesaAvroGeomSrid, GeomesaAvroGeomType, GeomesaAvroIndex, GeomesaAvroProperty}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryFactory, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.Date

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeParserTest extends Specification {

  private val invalidGeomesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
       |      "${GeomesaAvroGeomDefault.KEY}":"yes",
       |      "${GeomesaAvroGeomSrid.KEY}":"3401",
       |      "${GeomesaAvroIndex.KEY}":"no",
       |      "${GeomesaAvroCardinality.KEY}":"medium"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKT}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POLYGON}",
       |      "${GeomesaAvroGeomDefault.KEY}":"yes",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_DATE}",
       |      "${GeomesaAvroFeatureVisibility.KEY}":"${GeomesaAvroFeatureVisibility.TRUE}"
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
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaAvroSchema = new Schema.Parser().parse(invalidGeomesaAvroSchemaJson)

  private val validGeomesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "geomesa.table.sharing":"false",
       |  "geomesa.table.compression.enabled":"true",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "${GeomesaAvroIndex.KEY}":"${GeomesaAvroIndex.FULL}",
       |      "${GeomesaAvroCardinality.KEY}":"${GeomesaAvroCardinality.HIGH}"
       |    },
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
       |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
       |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.FALSE}",
       |      "${GeomesaAvroGeomSrid.KEY}":"${GeomesaAvroGeomSrid.EPSG_4326}",
       |      "${GeomesaAvroDateDefault.KEY}":"${GeomesaAvroDateDefault.FALSE}"
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
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.EPOCH_MILLIS}",
       |      "${GeomesaAvroDateDefault.KEY}":"${GeomesaAvroDateDefault.TRUE}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":["null","string"],
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_INSTANT}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_INSTANT_NO_MILLIS}"
       |    },
       |    {
       |      "name":"f7",
       |      "type":"string",
       |      "${GeomesaAvroFeatureVisibility.KEY}":"${GeomesaAvroFeatureVisibility.TRUE}"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaAvroSchema: Schema = new Schema.Parser().parse(validGeomesaAvroSchemaJson)

  private val geomFactory = new GeometryFactory()
  private val coordinateFactory = CoordinateArraySequenceFactory.instance()

  private def generateCoordinate(x: Double, y: Double): CoordinateSequence = {
    coordinateFactory.create(Array(new Coordinate(x, y)))
  }

  "The GeomesaAvroProperty parser for" >> {
    "geometry format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeomesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must beSome(GeomesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeomesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeomesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field1 = validGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomType.parse(field1) must beSome(classOf[Point])

        val field3 = validGeomesaAvroSchema.getField("f3")
        GeomesaAvroGeomType.parse(field3) must beSome(classOf[Geometry])
      }
    }

    "default geometry" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a boolean value if valid" >> {
        val field = validGeomesaAvroSchema.getField("f3")
        GeomesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "geometry srid" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomSrid.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a string if valid" in {
        val field = validGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomSrid.parse(field) must beSome("4326")
      }
    }

    "date format" should {
      "fail if the field does not have the required type(s)" in {
        val field1 = invalidGeomesaAvroSchema.getField("f2")
        GeomesaAvroDateFormat.parse(field1) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]

        val field2 = invalidGeomesaAvroSchema.getField("f5")
        GeomesaAvroDateFormat.parse(field2) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f4")
        GeomesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeomesaAvroDateFormat.parse(field) must beSome(GeomesaAvroDateFormat.EPOCH_MILLIS)
      }
    }

    "index" should {
      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("id")
        GeomesaAvroIndex.parse(field) must beSome(GeomesaAvroIndex.FULL)
      }
    }

    "cardinality" should {
      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("id")
        GeomesaAvroCardinality.parse(field) must beSome(GeomesaAvroCardinality.HIGH)
      }
    }

    "feature visibility" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema.getField("f2")
        GeomesaAvroFeatureVisibility.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f7")
        GeomesaAvroFeatureVisibility.parse(field) must beSome(true)
      }
    }
  }

  "The GeomesaAvroProperty deserializer for " >> {
    "geometry format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f3", "POINT(10 20)")
        GeomesaAvroGeomFormat.deserialize(record, "f3") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Geometry]]
      }

      "fail if the value cannot be deserialized because the geometry cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f3", "POINT(0 0 0 0 0 0)")
        GeomesaAvroGeomFormat.deserialize(record, "f3") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Geometry]]
      }

      "return the geometry if it can be deserialized" >> {
        "for a point" in {
          val record1 = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom1 = new Point(generateCoordinate(10, 20), geomFactory)
          record1.put("f1", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
          GeomesaAvroGeomFormat.deserialize(record1, "f1") mustEqual expectedGeom1
        }

        "for a geometry" in {
          val record2 = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom2 = new Point(generateCoordinate(10, 20), geomFactory).asInstanceOf[Geometry]
          record2.put("f3", "POINT(10 20)")
          GeomesaAvroGeomFormat.deserialize(record2, "f3") mustEqual expectedGeom2
        }
      }
    }

    "date format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f4", "1638912032")
        GeomesaAvroDateFormat.deserialize(record, "f4") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the date cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f5", "12/07/2021")
        GeomesaAvroDateFormat.deserialize(record, "f5") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the type is incorrect" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f4", 1000)
        GeomesaAvroDateFormat.deserialize(record, "f4") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "return the date if it can be deserialized" >> {
        "for milliseconds timestamp" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedDate = new Date(1638915744897L)
          record.put("f4", 1638915744897L)
          GeomesaAvroDateFormat.deserialize(record, "f4") mustEqual expectedDate
        }

        "for a null string" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedDate = null
          record.put("f5", null)
          GeomesaAvroDateFormat.deserialize(record, "f5") mustEqual expectedDate
        }

        "for an ISO datetime string" >> {

          "with milliseconds" in {
            val record = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate = new Date(1638915744897L)
            record.put("f5", "2021-12-07T17:22:24.897-05:00")
            GeomesaAvroDateFormat.deserialize(record, "f5") mustEqual expectedDate
          }

          "without milliseconds" in {
            val record = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate = new Date(1638915744000L)
            record.put("f6", "2021-12-07T17:22:24-05:00")
            GeomesaAvroDateFormat.deserialize(record, "f6") mustEqual expectedDate
          }
        }
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema) must
        throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val expectedSft = "id:String:cardinality=high:index=full,f1:Point:srid=4326,f2:Double,*f3:Geometry,f4:Date," +
        "f5:Date,f6:Date,f7:String;geomesa.index.dtg='f4',geomesa.table.compression.enabled='true'," +
        "geomesa.avro.visibility.field='f7',geomesa.table.sharing='false'"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema)

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }
  }
}
