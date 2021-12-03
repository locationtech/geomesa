package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Polygon
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeParserTest extends Specification {

  private val invalidGeomesaSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"invalid_schema",
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
       |      "${GeomesaAvroGeomType.KEY}":"GeometryCollection",
       |      "${GeomesaAvroGeomDefault.KEY}":"true"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"dd-mm-yyyy"
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaSchema = new Schema.Parser().parse(invalidGeomesaSchemaJson)

  private val validGeomesaSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"valid_schema",
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
       |      "type":"string",
       |      "${GeomesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeomesaAvroGeomType.KEY}":"POLYGON",
       |      "${GeomesaAvroGeomDefault.KEY}":"true"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO8601}"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaSchema = new Schema.Parser().parse(validGeomesaSchemaJson)

  private val invalidNonGeomesaSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"valid_schema",
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
       |      "type":"map"
       |    }
       |  ]
       |}""".stripMargin
  private val invalidNonGeomesaSchema = new Schema.Parser().parse(invalidNonGeomesaSchemaJson)

  private val validNonGeomesaSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"valid_schema",
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
  private val validNonGeomesaSchema = new Schema.Parser().parse(validNonGeomesaSchemaJson)

  "The GeomesaAvroProperty parser for" >> {
    "default geometry" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaSchema.getField("f2")
        GeomesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaSchema.getField("f1")
        GeomesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaSchema.getField("f2")
        GeomesaAvroGeomDefault.parse(field) must beNone
      }

      "return a boolean value if valid" >> {
        val field = validGeomesaSchema.getField("f3")
        GeomesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "geometry format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaSchema.getField("f1")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaSchema.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaSchema.getField("f4")
        GeomesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaSchema.getField("f3")
        GeomesaAvroGeomFormat.parse(field) must beSome(GeomesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaSchema.getField("f2")
        GeomesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaSchema.getField("f3")
        GeomesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaSchema.getField("f4")
        GeomesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field = validGeomesaSchema.getField("f3")
        GeomesaAvroGeomType.parse(field) must beSome(classOf[Polygon])
      }
    }

    "date format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaSchema.getField("f2")
        GeomesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaSchema.getField("f4")
        GeomesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaSchema.getField("f1")
        GeomesaAvroDateFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaSchema.getField("f4")
        GeomesaAvroDateFormat.parse(field) must beSome(GeomesaAvroDateFormat.ISO8601)
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema without geomesa properties into an SFT when the field type is not supported" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidNonGeomesaSchema) must
        throwAn[AvroSimpleFeatureTypeParser.UnsupportedAvroTypeException]
    }

    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaSchema) must
        throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
    }

    "convert a schema without geomesa avro properties into an SFT" in {
      val expected =
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validNonGeomesaSchema)
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val expected =
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaSchema)
      
    }
  }
}
