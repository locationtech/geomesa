/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.utils

import org.apache.kudu.ColumnSchema.{CompressionAlgorithm, Encoding}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.kudu.KuduSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ColumnConfigurationTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  "ColumnConfiguration" should {
    "default to lz4 compression" in {
      ColumnConfiguration.compression(None) mustEqual CompressionAlgorithm.LZ4
    }

    "lookup compression by name" in {
      foreach(Seq("snappy", "SNAPPY", "Snappy")) { name =>
        ColumnConfiguration.compression(Some(name)) mustEqual CompressionAlgorithm.SNAPPY
      }
    }

    "lookup compression by system property" in {
      foreach(Seq("snappy", "SNAPPY", "Snappy")) { name =>
        KuduSystemProperties.Compression.threadLocalValue.set(name)
        try {
          ColumnConfiguration.compression(Some(name)) mustEqual CompressionAlgorithm.SNAPPY
        } finally {
          KuduSystemProperties.Compression.threadLocalValue.remove()
        }
      }
    }

    "throw exception on invalid compression" in {
      ColumnConfiguration.compression(Some("foo")) must throwAn[IllegalArgumentException]
    }

    "lookup encodings by name" in {
      foreach(Seq("dict_encoding", "DICT_ENCODING", "Dict_Encoding")) { name =>
        ColumnConfiguration.encoding(name) mustEqual Encoding.DICT_ENCODING
      }
    }

    "throw exception on invalid encodings" in {
      ColumnConfiguration.encoding("foo") must throwAn[IllegalArgumentException]
    }

    "accept configuration from attribute user data" in {
      val sft = SimpleFeatureTypes.createType("", "name:String,comp:String:encoding=plain_encoding:compression=snappy")

      val name = sft.getDescriptor(0)
      val nameConfig = ColumnConfiguration(ObjectType.selectType(name).head, name.getUserData.asScala)
      nameConfig.encoding must beNone
      nameConfig.compression must beSome(CompressionAlgorithm.LZ4) // default

      val comp = sft.getDescriptor(1)
      val compConfig = ColumnConfiguration(ObjectType.selectType(comp).head, comp.getUserData.asScala)
      compConfig.encoding must beSome(Encoding.PLAIN_ENCODING)
      compConfig.compression must beSome(CompressionAlgorithm.SNAPPY)
    }
  }
}
