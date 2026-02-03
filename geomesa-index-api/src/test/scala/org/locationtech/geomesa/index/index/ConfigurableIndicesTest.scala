/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigurableIndicesTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def getIndexConfig(sft: SimpleFeatureType): Seq[String] =
    sft.getIndices.map(i => s"${i.name}=${i.attributes.mkString(":")}").sorted

  "GeoMesaDataStore" should {

    // tests here are the examples from the docs

    "support configurable indices in a spec string" in {
      WithClose(new TestGeoMesaDataStore(true)) { ds =>
        // creates a default attribute index on name and an implicit default z3 and z2 index on geom
        ds.createSchema(SimpleFeatureTypes.createType("test1", "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg
        ds.createSchema(SimpleFeatureTypes.createType("test2", "name:String:index='attr:geom:dtg',dtg:Date,*geom:Point:srid=4326:index='z3:dtg'"))
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg and disables the ID index
        ds.createSchema(SimpleFeatureTypes.createType("test3", "name:String:index='attr:dtg',dtg:Date,*geom:Point:srid=4326:index='z3:dtg';id.index.enabled=false"))
        getIndexConfig(ds.getSchema("test1")) mustEqual Seq("attr=name:dtg", "id=", "z2=geom", "z3=geom:dtg")
        getIndexConfig(ds.getSchema("test2")) mustEqual Seq("attr=name:geom:dtg", "id=", "z3=geom:dtg")
        getIndexConfig(ds.getSchema("test3")) mustEqual Seq("attr=name:dtg", "z3=geom:dtg")
      }
    }

    "support configurable indices with SchemaBuilder" in {
      val sft =
        SchemaBuilder.builder()
          .addString("name").withIndex("attr:geom:dtg") // creates an attribute index on name, with a secondary date index
          .addInt("age").withIndex() // creates an attribute index on age, with a default secondary index
          .addDate("dtg") // not a primary index
          .addPoint("geom", default = true).withIndices("z3:dtg", "z2") // creates a z3 index with dtg, and a z2 index
          .userData
          .disableIdIndex() // disables the ID index
          .build("test1")
      WithClose(new TestGeoMesaDataStore(true)) { ds =>
        ds.createSchema(sft)
        getIndexConfig(ds.getSchema("test1")) mustEqual Seq("attr=age:dtg", "attr=name:geom:dtg", "z2=geom", "z3=geom:dtg")
      }
    }

    "support configurable indices with config" in {
      val config =
        s"""{
           |  type-name = test1
           |  attributes = [
           |    { name = "name", type = "String", index = "attr:geom:dtg" } // creates an attribute index on name, with a secondary date index
           |    { name = "age", type = "Int", index = "true" } // creates an attribute index on age, with a default secondary index
           |    { name = "dtg", type = "Date" } // not a primary index
           |    { name = "geom", type = "Point", srid = "4326", index = "z3:dtg,z2" } // creates a z3 index with dtg, and a z2 index
           |  ]
           |  user-data = {
           |    "id.index.enabled" = "false" // disables the default ID index
           |  }
           |}
           |""".stripMargin
      val sft = SimpleFeatureTypes.createType(ConfigFactory.parseString(config), path = None)
      WithClose(new TestGeoMesaDataStore(true)) { ds =>
        ds.createSchema(sft)
        getIndexConfig(ds.getSchema("test1")) mustEqual Seq("attr=age:dtg", "attr=name:geom:dtg", "z2=geom", "z3=geom:dtg")
      }
    }
  }
}
