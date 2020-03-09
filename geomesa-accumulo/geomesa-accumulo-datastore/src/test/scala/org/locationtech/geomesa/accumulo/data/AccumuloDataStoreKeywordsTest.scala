/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.config.ConfigFactory
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._

class AccumuloDataStoreKeywordsTest extends TestWithMultipleSfts {

  "AccumuloDataStore" should {

    "create a schema with keywords" in {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.Keywords
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.KeywordsDelimiter

      val keywords = Seq("keywordA", "keywordB", "keywordC")
      val spec = s"name:String;$Keywords=${keywords.mkString(KeywordsDelimiter)}"
      val sftWithKeywords = createNewSchema(spec)

      ds.getFeatureSource(sftWithKeywords.getTypeName).getInfo.getKeywords.toSeq must containAllOf(keywords)
    }

    "create a schema w/ keyword array" in {
      val keywords: Seq[String] = Seq("keywordA=foo,bar", "keywordB", "keywordC")
      val regular = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"       , index = true  },
          |    { name = "testCard", type = "string"       , index = true, cardinality = high },
          |    { name = "testList", type = "List[String]" , index = false },
          |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
          |  ]
          |  user-data = {
          |    geomesa.keywords = ["keywordA=foo,bar","keywordB","keywordC"]
          |  }
          |}
          """.stripMargin)
      val sftWithKeywords = SimpleFeatureTypes.createType(regular)
      ds.createSchema(sftWithKeywords)
      val fs = ds.getFeatureSource(sftWithKeywords.getTypeName)
      fs.getInfo.getKeywords.toSeq must containAllOf(keywords)
    }

    "create a schema w/ keyword string" in {
      val keywords: Seq[String] = Seq("keywordA=foo,bar")
      val regular = ConfigFactory.parseString(
        """
          |{
          |  type-name = "testconf"
          |  fields = [
          |    { name = "testStr",  type = "string"       , index = true  },
          |    { name = "testCard", type = "string"       , index = true, cardinality = high },
          |    { name = "testList", type = "List[String]" , index = false },
          |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
          |  ]
          |  user-data = {
          |    geomesa.keywords = "keywordA=foo,bar"
          |  }
          |}
          """.stripMargin)
      val sftWithKeywords = SimpleFeatureTypes.createType(regular)
      ds.createSchema(sftWithKeywords)
      val fs = ds.getFeatureSource(sftWithKeywords.getTypeName)
      fs.getInfo.getKeywords.toSeq must containAllOf(keywords)
    }

    "remove keywords from schema" in {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.Keywords
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.KeywordsDelimiter

      val initialKeywords = Set("keywordA=Hello", "keywordB", "keywordC")
      val spec = s"name:String;$Keywords=${initialKeywords.mkString(KeywordsDelimiter)}"
      val sft = SimpleFeatureTypes.mutable(createNewSchema(spec))

      val keywordsToRemove = Set("keywordA=Hello", "keywordC")
      sft.removeKeywords(keywordsToRemove)
      ds.updateSchema(sft.getTypeName, sft)

      val keywords = ds.getFeatureSource(sft.getTypeName).getInfo.getKeywords.toSeq
      keywords must contain(initialKeywords -- keywordsToRemove)
      keywords must not(contain(keywordsToRemove))
    }

    "add keywords to schema" in {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.Keywords

      val originalKeyword = "keywordB"
      val keywordsToAdd = Set("keywordA", "~!@#$%^&*()_+`=/.,<>?;:|[]{}\\")

      val spec = s"name:String;$Keywords=$originalKeyword"
      val sft = SimpleFeatureTypes.mutable(createNewSchema(spec))

      sft.addKeywords(keywordsToAdd)
      ds.updateSchema(sft.getTypeName, sft)

      val fs = ds.getFeatureSource(sft.getTypeName)
      fs.getInfo.getKeywords.toSeq must containAllOf((keywordsToAdd + originalKeyword).toSeq)
    }

    "not allow updating non-keyword user data" in {
      import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.TableSharing

      val sft = SimpleFeatureTypes.mutable(createNewSchema("name:String"))
      ds.createSchema(sft)

      sft.getUserData.put(TableSharing, "false") // Change table sharing

      ds.updateSchema(sft.getTypeName, sft) must throwAn[UnsupportedOperationException]
    }

    "create config from schema w/ keywords" in {
      val confString = """
                         |{
                         |  type-name = "testconf"
                         |  fields = [
                         |    { name = "testStr",  type = "string"       , index = true  },
                         |    { name = "testCard", type = "string"       , index = true, cardinality = high },
                         |    { name = "testList", type = "List[String]" , index = false },
                         |    { name = "geom",     type = "Point"        , srid = 4326, default = true }
                         |  ]
                         |  user-data = {
                         |    geomesa.keywords = ["keywordA=foo,bar","keywordB","keywordC"]
                         |  }
                         |}
         """.stripMargin
      val regular = ConfigFactory.parseString(confString)
      val sftWithKeywords = SimpleFeatureTypes.createType(regular)

      // Currently breaks as it can't derive the type name from the config string
      val newSft = SimpleFeatureTypes.createType(SimpleFeatureTypes.toConfig(sftWithKeywords))

      sftWithKeywords.getKeywords mustEqual newSft.getKeywords
    }.pendingUntilFixed
  }
}
