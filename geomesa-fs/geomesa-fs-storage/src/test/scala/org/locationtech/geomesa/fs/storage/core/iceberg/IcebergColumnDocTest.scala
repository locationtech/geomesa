/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.specs2.mutable.SpecificationWithJUnit

/**
 * Structural type definitions round-trip through a table property rather than a column doc. The
 * catalog refuses a table whose column comment runs over 255 characters and a nested
 * avro schema is several times that, but the behavior here is uniform rather than conditional on
 * any particular schema being long.
 */
class IcebergColumnDocTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  // shaped like the nested UMR attributes: an array of records, which is what pushes a doc long
  private val nestedSchema =
    """{"type":"array","items":["null",{"type":"record","name":"identifiers_item",""" +
      """"namespace":"org.locationtech.geomesa.trino","fields":[""" +
      """{"name":"realm","type":["null","string"],"default":null},""" +
      """{"name":"selector","type":["null","string"],"default":null},""" +
      """{"name":"security_label","type":["null","string"],"default":null}]}]}"""

  // short enough to have fit in a doc, to show the treatment does not depend on length
  private val shortSchema = """{"type":"map","values":"string"}"""

  private def sftWith(attribute: String, schema: String = nestedSchema): SimpleFeatureType = {
    val sft =
      SimpleFeatureTypes.createType("umr", s"name:String,$attribute:String:json=true,dtg:Date,*geom:Point:srid=4326")
    sft.getDescriptor(attribute).getUserData.put(AttributeOptions.OptJsonSchema, schema)
    sft
  }

  private def docFor(sft: SimpleFeatureType, name: String): String =
    SimpleFeatureIcebergSchema.create(sft, GeoParquetWkb).columns().asScala.find(_.name == name).map(_.doc()).orNull

  "SimpleFeatureIcebergSchema" should {

    "leave an attribute with no structural definition untouched" >> {
      val sft = sftWith("identifiers")
      docFor(sft, "name") mustEqual SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor("name"))
    }

    "leave a structural definition out of the column doc" >> {
      val sft = sftWith("identifiers")
      SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor("identifiers")).length must beGreaterThan(255)

      val doc = docFor(sft, "identifiers")
      doc must not(contain(AttributeOptions.OptJsonSchema))
      doc.length must beLessThanOrEqualTo(255)
      // the column is still marked as json, only its shape is left to the struct type
      doc must contain(AttributeOptions.OptJson)
    }

    "leave a short structural definition out of the column doc as well" >> {
      val sft = sftWith("props", shortSchema)
      SimpleFeatureTypes.encodeDescriptor(sft, sft.getDescriptor("props")).length must beLessThanOrEqualTo(255)
      docFor(sft, "props") must not(contain(AttributeOptions.OptJsonSchema))
    }

    "keep the nested shape in the iceberg struct even though the doc omits it" >> {
      val schema = SimpleFeatureIcebergSchema.create(sftWith("identifiers"), GeoParquetWkb)
      val field = schema.columns().asScala.find(_.name == "identifiers").orNull
      field must not(beNull)
      // a variant here would mean the shape was lost along with the option
      field.`type`().isNestedType must beTrue
    }
  }

  "IcebergCatalog" should {

    "write the attribute spec as a property keyed by its column" >> {
      val properties = IcebergCatalog.sftTableProperties(sftWith("identifiers"))
      val spec = properties.get(IcebergCatalog.columnSpecProperty("identifiers"))
      spec must beSome
      // the spec is what reconstructs the attribute, so it carries the schema the doc dropped
      SimpleFeatureTypes.createDescriptor(spec.get).getUserData.get(AttributeOptions.OptJsonSchema) mustEqual nestedSchema
    }

    "write a property for a short structural definition too" >> {
      val properties = IcebergCatalog.sftTableProperties(sftWith("props", shortSchema))
      properties.get(IcebergCatalog.columnSpecProperty("props")) must beSome
    }

    "write no property for an attribute with no structural definition" >> {
      val properties = IcebergCatalog.sftTableProperties(sftWith("identifiers"))
      properties.get(IcebergCatalog.columnSpecProperty("name")) must beNone
      properties.get(IcebergCatalog.columnSpecProperty("dtg")) must beNone
    }

    "key the property by the encoded column name" >> {
      // dots are not valid in a column name, so the column - and the key - use an underscore
      val properties = IcebergCatalog.sftTableProperties(sftWith("source.identifiers"))
      properties.get(IcebergCatalog.columnSpecProperty("source_identifiers")) must beSome
    }

    "not collide with the feature type user data prefix" >> {
      // a column named 'userdata' would otherwise be read back as sft user data named 'spec'
      val key = IcebergCatalog.columnSpecProperty("userdata")
      key must not(startWith(IcebergCatalog.UserDataPrefix))

      val properties = IcebergCatalog.sftTableProperties(sftWith("userdata"))
      properties.get(key) must beSome
      // feature type user data is legitimately written under its own prefix; the point is that
      // the column's spec is not among the keys that get read back out as user data
      properties.keys.filter(_.startsWith(IcebergCatalog.UserDataPrefix)) must not(contain(key))
    }
  }
}
