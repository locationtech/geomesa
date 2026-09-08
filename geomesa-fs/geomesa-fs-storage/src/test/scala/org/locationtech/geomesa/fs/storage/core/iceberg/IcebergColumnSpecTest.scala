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
 * Attribute specs live in table properties, one per column, and nothing is written to the column
 * docs. A spec carrying a structural type definition is a whole avro schema, far longer than the
 * 255 characters a catalog such as AWS Glue accepts as a column comment, so rather than split the
 * feature type across two places by length the properties carry all of it.
 */
class IcebergColumnSpecTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  // shaped like the nested UMR attributes: an array of records, which is what pushes a spec long
  private val nestedSchema =
    """{"type":"array","items":["null",{"type":"record","name":"identifiers_item",""" +
      """"namespace":"org.locationtech.geomesa.trino","fields":[""" +
      """{"name":"realm","type":["null","string"],"default":null},""" +
      """{"name":"selector","type":["null","string"],"default":null},""" +
      """{"name":"security_label","type":["null","string"],"default":null}]}]}"""

  private def sftWith(attribute: String): SimpleFeatureType = {
    val sft =
      SimpleFeatureTypes.createType("umr", s"name:String,$attribute:String:json=true,dtg:Date,*geom:Point:srid=4326")
    sft.getDescriptor(attribute).getUserData.put(AttributeOptions.OptJsonSchema, nestedSchema)
    sft
  }

  private val sft = sftWith("identifiers")

  "SimpleFeatureIcebergSchema" should {

    "write no column docs at all" >> {
      val columns = SimpleFeatureIcebergSchema.create(sft, GeoParquetWkb).columns().asScala
      columns must not(beEmpty)
      forall(columns)(_.doc() must beNull)
    }

    "keep the nested shape in the iceberg struct" >> {
      val schema = SimpleFeatureIcebergSchema.create(sft, GeoParquetWkb)
      val field = schema.columns().asScala.find(_.name == "identifiers").orNull
      field must not(beNull)
      // a variant here would mean the structural definition never reached the schema
      field.`type`().isNestedType must beTrue
    }
  }

  "IcebergCatalog" should {

    "write a spec property for every attribute" >> {
      val properties = IcebergCatalog.sftTableProperties(sft)
      forall(Seq("name", "identifiers", "dtg", "geom")) { column =>
        properties.get(IcebergCatalog.columnSpecProperty(column)) must beSome
      }
    }

    "carry the structural definition in the spec" >> {
      val spec = IcebergCatalog.sftTableProperties(sft).get(IcebergCatalog.columnSpecProperty("identifiers"))
      spec must beSome
      SimpleFeatureTypes.createDescriptor(spec.get).getUserData.get(AttributeOptions.OptJsonSchema) mustEqual
        nestedSchema
    }

    "round-trip an ordinary attribute through its spec" >> {
      val spec = IcebergCatalog.sftTableProperties(sft).get(IcebergCatalog.columnSpecProperty("dtg"))
      spec must beSome
      SimpleFeatureTypes.createDescriptor(spec.get).getLocalName mustEqual "dtg"
    }

    "write no spec for the geometry companion columns" >> {
      // bbox and z-value columns are derived, not attributes, and the reader skips them by name
      val properties = IcebergCatalog.sftTableProperties(sft)
      properties.get(IcebergCatalog.columnSpecProperty("__geom_bbox__")) must beNone
      properties.get(IcebergCatalog.columnSpecProperty("__geom_z2__")) must beNone
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
      properties.keys.filter(_.startsWith(IcebergCatalog.UserDataPrefix)) must not(contain(key))
    }
  }
}
