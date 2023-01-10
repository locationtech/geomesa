/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.Date

@RunWith(classOf[JUnitRunner])
class SchemaParserTest extends Specification {

  import SchemaParser._

  import scala.collection.JavaConverters._

  private val invalidGeomesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POLYGON}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATE}",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
       |    },
       |    {
       |      "name":"f3",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"TWKB",
       |      "${GeoMesaAvroGeomType.KEY}":"MultiGeometryCollection",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"dd-mm-yyyy",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
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
       |  "geomesa.index.dtg":"f4",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "index":"full",
       |      "cardinality":"high"
       |    },
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.FALSE}"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double"
       |    },
       |    {
       |      "name":"f3",
       |      "type":["null","string"],
       |      "${GeoMesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeoMesaAvroGeomType.KEY}":"geometry",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"long",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":["null", "string"],
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATETIME}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"string",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    },
       |    {
       |      "name":"f7",
       |      "type":"int",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
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
      "fail if the field does not have the required type" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must beSome(GeoMesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field1 = validGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomType.parse(field1) must beSome(classOf[Point])

        val field3 = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field3) must beSome(classOf[Geometry])
      }
    }

    "default geometry" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomDefault.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyValueException]
      }

      "return a boolean value if valid" >> {
        val field = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "date format" should {
      "fail if the field does not have the required type" in {
        val field1 = invalidGeomesaAvroSchema.getField("f2")
        GeoMesaAvroDateFormat.parse(field1) must throwAn[GeoMesaAvroProperty.InvalidPropertyTypeException]

        val field2 = invalidGeomesaAvroSchema.getField("f5")
        GeoMesaAvroDateFormat.parse(field2) must throwAn[GeoMesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyValueException]
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must beSome(GeoMesaAvroDateFormat.EPOCH_MILLIS)
      }
    }

    "feature visibility" should {
      "fail if the field does not have the required type" in {
        val field = invalidGeomesaAvroSchema.getField("f2")
        GeoMesaAvroVisibilityField.parse(field) must throwAn[GeoMesaAvroProperty.InvalidPropertyTypeException]
      }

      "return a boolean value if valid" in {
        val field = validGeomesaAvroSchema.getField("f6")
        GeoMesaAvroVisibilityField.parse(field) must beSome(true)
      }
    }
  }

  "The GeomesaAvroProperty deserializer for " >> {
    "geometry format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f3", "POINT(10 20)")
        GeoMesaAvroGeomFormat.getFieldReader(record.getSchema, "f3").apply(record.get("f3")) must
          throwA[GeoMesaAvroDeserializableEnumProperty.DeserializerException[Geometry]]
      }

      "fail if the value cannot be deserialized because the geometry cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f3", "POINT(0 0 0 0 0 0)")
        GeoMesaAvroGeomFormat.getFieldReader(record.getSchema, "f3").apply(record.get("f3")) must throwAn[Exception]
      }

      "return the geometry if it can be deserialized" >> {
        "for a point" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom = new Point(generateCoordinate(10, 20), geomFactory)
          record.put("f1", ByteBuffer.wrap(WKBUtils.write(expectedGeom)))
          GeoMesaAvroGeomFormat.getFieldReader(record.getSchema, "f1").apply(record.get("f1")) mustEqual expectedGeom
        }

        "for a geometry" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom = new Point(generateCoordinate(10, 20), geomFactory).asInstanceOf[Geometry]
          record.put("f3", "POINT(10 20)")
          GeoMesaAvroGeomFormat.getFieldReader(record.getSchema, "f3").apply(record.get("f3")) mustEqual expectedGeom
        }
      }
    }

    "date format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f4", "1638912032")
        GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f4").apply(record.get("f4")) must
          throwA[GeoMesaAvroDeserializableEnumProperty.DeserializerException[Date]]
      }

      "fail if the value cannot be deserialized because the date cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f5", "12/07/2021")
        GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f5").apply(record.get("f5")) must throwAn[Exception]
      }

      "fail if the value cannot be deserialized because the type is incorrect" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f4", 1000)
        GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f4").apply(record.get("f4")) must throwAn[Exception]
      }

      "return the date if it can be deserialized" >> {
        "for milliseconds timestamp" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedDate = new Date(1638915744897L)
          record.put("f4", 1638915744897L)
          GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f4").apply(record.get("f4")) mustEqual expectedDate
        }

        "for an ISO datetime string with generic format" >> {

          "without millis" in {
            val record = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate = new Date(1638915744000L)
            record.put("f5", "2021-12-07T17:22:24-05:00")
            GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f5").apply(record.get("f5")) mustEqual expectedDate
          }

          "with millis" in {
            val record = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate = new Date(1638915744897L)
            record.put("f5", "2021-12-07T17:22:24.897-05:00")
            GeoMesaAvroDateFormat.getFieldReader(record.getSchema, "f5").apply(record.get("f5")) mustEqual expectedDate
          }
        }
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
<<<<<<< HEAD
      SchemaParser.schemaToSft(invalidGeomesaAvroSchema) must throwAn[IllegalArgumentException]
=======
      SchemaParser.schemaToSft(invalidGeomesaAvroSchema) must
        throwAn[GeoMesaAvroProperty.InvalidPropertyTypeException]
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    }

    "fail to convert a schema with multiple default geometries into an SFT" in {
      val schemaJson =
        s"""{
           |  "type":"record",
           |  "name":"schema1",
           |  "fields":[
           |    {
           |      "name":"geom1",
           |      "type":"string",
           |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
           |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.LINESTRING}",
           |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
           |    },
           |    {
           |      "name":"geom2",
           |      "type":"bytes",
           |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
           |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.MULTIPOINT}",
           |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
           |    }
           |  ]
           |}""".stripMargin
      val schema = new Schema.Parser().parse(schemaJson)

      SchemaParser.schemaToSft(schema) must throwAn[IllegalArgumentException]
    }

    "fail to convert a schema with multiple visibility fields into an SFT" in {
      val schemaJson =
        s"""{
           |  "type":"record",
           |  "name":"schema1",
           |  "fields":[
           |    {
           |      "name":"visibility1",
           |      "type":"string",
           |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
           |    },
           |    {
           |      "name":"visibility2",
           |      "type":"string",
           |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
           |    }
           |  ]
           |}""".stripMargin
      val schema = new Schema.Parser().parse(schemaJson)

      SchemaParser.schemaToSft(schema) must throwAn[IllegalArgumentException]
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val sft = SchemaParser.schemaToSft(validGeomesaAvroSchema)

      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("id", "f1", "f2", "f3", "f4", "f5")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Point], classOf[java.lang.Double], classOf[Geometry], classOf[Date], classOf[Date])
      sft.getAttributeDescriptors.asScala.map(_.getUserData.asScala) mustEqual
          Seq(
            Map("cardinality" -> "high", "index" -> "full"),
            Map("srid" -> "4326", "default" -> "false"),
            Map.empty,
            Map("srid" -> "4326", "default" -> "true"),
<<<<<<< HEAD
            Map("geomesa.date.format" -> "epoch-millis"),
            Map("geomesa.date.format" -> "iso-datetime")
=======
            Map.empty,
            Map.empty
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
          )

      sft.getUserData.asScala mustEqual Map(
        "geomesa.index.dtg"->"f4",
        "geomesa.table.compression.enabled" -> "true",
        "geomesa.visibility.field" -> "f6",
        "geomesa.table.sharing" -> "false",
        "geomesa.mixed.geometries" -> "true"
      )
    }
<<<<<<< HEAD

    "support logical date types" in {
      val schema =
        """{
          |  "type":"record",
          |  "name":"dtgSchema",
          |  "fields":[
          |    { "name":"id", "type": "string" },
          |    { "name":"dtg", "type": { "type": "long", "logicalType": "timestamp-millis" } }
          |  ]
          |}""".stripMargin

      val sft = SchemaParser.schemaToSft(new Schema.Parser().parse(schema))
      sft.getAttributeCount mustEqual 2
      sft.getAttributeDescriptors.get(0).getType.getBinding mustEqual classOf[String]
      sft.getAttributeDescriptors.get(1).getType.getBinding mustEqual classOf[Date]
    }
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  }
}
