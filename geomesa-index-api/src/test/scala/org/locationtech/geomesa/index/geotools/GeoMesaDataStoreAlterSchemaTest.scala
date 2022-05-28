/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import com.typesafe.config.ConfigFactory
import org.geotools.data.{Query, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.SchemaCompatibility
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaDataStoreAlterSchemaTest extends Specification {

  import scala.collection.JavaConverters._

  "GeoMesaDataStore" should {
    "update schemas" in {
      foreach(Seq(true, false)) { partitioning =>
        val ds = new TestGeoMesaDataStore(true)
        val spec = "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326;"
        if (partitioning) {
          ds.createSchema(SimpleFeatureTypes.createType("test", s"$spec${Configs.TablePartitioning}=time"))
        } else {
          ds.createSchema(SimpleFeatureTypes.createType("test", spec))
        }

        var sft = ds.getSchema("test")
        val feature = ScalaSimpleFeature.create(sft, "0", "name0", 0, "2018-01-01T06:00:00.000Z", "POINT (40 55)")
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature, useProvidedFid = true)
        }

        var filters = Seq(
          "name = 'name0'",
          "bbox(geom,38,53,42,57)",
          "bbox(geom,38,53,42,57) AND dtg during 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z",
          "IN ('0')"
        ).map(ECQL.toFilter)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(feature)
        }
        ds.stats.getCount(sft) must beSome(1L)

        // rename
        ds.updateSchema("test", SimpleFeatureTypes.renameSft(sft, "rename"))

        ds.getSchema("test") must beNull

        sft = ds.getSchema("rename")

        sft must not(beNull)
        sft .getTypeName mustEqual "rename"

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "name", exact = false).map(_.max) must beSome("name0")

        // rename column
        Some(new SimpleFeatureTypeBuilder()).foreach { builder =>
          builder.init(ds.getSchema("rename"))
          builder.set(0, SimpleFeatureSpecParser.parseAttribute("names:String:index=true").toDescriptor)
          val update = builder.buildFeatureType()
          update.getUserData.putAll(ds.getSchema("rename").getUserData)
          ds.updateSchema("rename", update)
        }

        sft = ds.getSchema("rename")
        sft must not(beNull)
        sft.getTypeName mustEqual "rename"
        sft.getDescriptor(0).getLocalName mustEqual "names"
        sft.getDescriptor("names") mustEqual sft.getDescriptor(0)

        filters = Seq(ECQL.toFilter("names = 'name0'")) ++ filters.drop(1)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "names", exact = false).map(_.max) must beSome("name0")

        // rename type and column
        Some(new SimpleFeatureTypeBuilder()).foreach { builder =>
          builder.init(ds.getSchema("rename"))
          builder.set(0, SimpleFeatureSpecParser.parseAttribute("n:String").toDescriptor)
          builder.setName("foo")
          val update = builder.buildFeatureType()
          update.getUserData.putAll(ds.getSchema("rename").getUserData)
          ds.updateSchema("rename", update)
        }

        sft = ds.getSchema("foo")
        sft must not(beNull)
        sft.getTypeName mustEqual "foo"
        sft.getDescriptor(0).getLocalName mustEqual "n"
        sft.getDescriptor("n") mustEqual sft.getDescriptor(0)
        ds.getSchema("rename") must beNull

        filters = Seq(ECQL.toFilter("n = 'name0'")) ++ filters.drop(1)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "n", exact = false).map(_.max) must beSome("name0")
      }
    }
    "update compatible schemas from typesafe config changes" in {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      def toSft(config: String): SimpleFeatureType = SimpleFeatureTypes.createType(ConfigFactory.parseString(config))

      val ds = new TestGeoMesaDataStore(true)
      val missingConfig =
        """{
          |  type-name = "test"
          |  attributes = [
          |    { name = "type", type = "String"                             }
          |    { name = "dtg",  type = "Date",  default = true              }
          |    { name = "geom", type = "Point", default = true, srid = 4326 }
          |  ]
          |  user-data = {
          |    "geomesa.indices.enabled" = "z3:geom:dtg,attr:type:dtg"
          |  }
          |}""".stripMargin

      val missing = ds.checkSchemaCompatibility("test", toSft(missingConfig))
      missing must beAnInstanceOf[SchemaCompatibility.DoesNotExist]
      missing.apply()

      val original = ds.getSchema("test")
      original must not(beNull)

      ds.checkSchemaCompatibility("test", toSft(missingConfig)) mustEqual SchemaCompatibility.Unchanged

      val addIndexConfig = missingConfig.replace("z3", "id,z3")

      val addIndex = ds.checkSchemaCompatibility("test", toSft(addIndexConfig))
      addIndex must beAnInstanceOf[SchemaCompatibility.Compatible]
      addIndex.apply()

      val updateAddIndex = ds.getSchema("test")
      updateAddIndex.getIndices.map(_.name).toSet mustEqual Set("id", "z3", "attr")

      ds.checkSchemaCompatibility("test", toSft(addIndexConfig)) mustEqual SchemaCompatibility.Unchanged

      val addAttributeConfig = addIndexConfig.replace("]", s"""    { name = "name", type = "String" }${"\n"}]""")

      val addAttribute = ds.checkSchemaCompatibility("test", toSft(addAttributeConfig))
      addAttribute must beAnInstanceOf[SchemaCompatibility.Compatible]
      addAttribute.apply()

      val updateAddAttribute = ds.getSchema("test")
      updateAddAttribute.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
          Seq("type", "dtg", "geom", "name")

      ds.checkSchemaCompatibility("test", toSft(addAttributeConfig)) mustEqual SchemaCompatibility.Unchanged
    }
    "update compatible schemas from index flags" in {
      def toSft(config: String): SimpleFeatureType = SimpleFeatureTypes.createType(ConfigFactory.parseString(config))

      val ds = new TestGeoMesaDataStore(true)
      val missingConfig =
        """{
          |  type-name = "test"
          |  attributes = [
          |    { name = "type", type = "String", index = true                             }
          |    { name = "dtg",  type = "Date",  default = true                            }
          |    { name = "geom", type = "Point", default = true, index = true, srid = 4326 }
          |  ]
          |}""".stripMargin

      val missing = ds.checkSchemaCompatibility("test", toSft(missingConfig))
      missing must beAnInstanceOf[SchemaCompatibility.DoesNotExist]
      missing.apply()

      ds.checkSchemaCompatibility("test", toSft(missingConfig)) mustEqual SchemaCompatibility.Unchanged
    }
    "update compatible schemas from index flags with enabled indices user data" in {
      def toSft(config: String): SimpleFeatureType = SimpleFeatureTypes.createType(ConfigFactory.parseString(config))

      val ds = new TestGeoMesaDataStore(true)
      val missingConfig =
        """{
          |  type-name = "test"
          |  attributes = [
          |    { name = "type", type = "String", index = true                             }
          |    { name = "dtg",  type = "Date",  default = true                            }
          |    { name = "geom", type = "Point", default = true, index = true, srid = 4326 }
          |  ]
          |  user-data = {
          |  "geomesa.indices.enabled" = "id,z3,attr"
          |  }
          |}""".stripMargin

      val missing = ds.checkSchemaCompatibility("test", toSft(missingConfig))
      missing must beAnInstanceOf[SchemaCompatibility.DoesNotExist]
      missing.apply()

      ds.checkSchemaCompatibility("test", toSft(missingConfig)) mustEqual SchemaCompatibility.Unchanged
    }
  }
}


