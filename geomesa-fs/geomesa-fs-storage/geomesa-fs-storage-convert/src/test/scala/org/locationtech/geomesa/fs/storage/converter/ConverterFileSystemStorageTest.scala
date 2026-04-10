/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.metadata.{ConverterMetadata, StorageMetadataCatalog}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConverterFileSystemStorageTest extends Specification with LazyLogging {

  private val sftConfig =
    """geomesa.sfts.example = {
      |  attributes = [
      |    { name = "name", type = "String"                             }
      |    { name = "age",  type = "Int"                                }
      |    { name = "dtg",  type = "Date", default = true               }
      |    { name = "geom", type = "Point", srid = 4326, default = true }
      |  ]
      |}""".stripMargin
  private val converterConfig =
    """geomesa.converters.example = {
      |  type   = "delimited-text"
      |  format = "CSV"
      |  options {
      |    skip-lines = 1
      |  }
      |  id-field = "toString($fid)",
      |  fields = [
      |    { name = "fid",  transform = "$1::int"           }
      |    { name = "name", transform = "$2::string"        }
      |    { name = "age",  transform = "$3::int"           }
      |    { name = "dtg",  transform = "datetime($4)"      }
      |    { name = "lon",  transform = "$5::double"        }
      |    { name = "lat",  transform = "$6::double"        }
      |    { name = "geom", transform = "point($lon, $lat)" }
      |  ]
      |}""".stripMargin

  "ConverterFileSystemStorage" should {
    "read features in compressed tar.gz files" in {
      val dir = Option(getClass.getClassLoader.getResource("example-convert-test-1")).map(_.toURI).orNull
      dir must not(beNull)

      foreach(Seq(true, false)) { useConf =>
        val conf = new Configuration()
        val params = Map.newBuilder[String, String]

        // metadata flags can be set in the params, storage params have to go in the conf
        def addConfig(k: String, v: String): Unit = if (useConf) { conf.set(k, v) } else { params += (k -> v) }
        addConfig(ConverterMetadata.SftConfigParam, sftConfig)
        addConfig(ConverterMetadata.PartitionSchemeParam, "daily")
        addConfig(ConverterMetadata.LeafStorageParam, "false")

        conf.set(ConverterMetadata.ConverterPathParam, "example-convert-test-1")
        conf.set(ConverterStorageFactory.ConverterConfigParam, converterConfig)

        val context = FileSystemContext(new Path(dir).getParent, conf)
        val catalog = StorageMetadataCatalog(context, "converter", params.result())
        catalog.getTypeNames mustEqual Seq("example")
        val metadata = catalog.load("example")
        metadata must not(beNull)
        WithClose(FileSystemStorageFactory("converter").apply(context, metadata)) { storage =>
          val query = new Query(metadata.sft.getTypeName, ECQL.toFilter("dtg during 2023-01-17T00:00:00.000Z/2023-01-19T00:00:00.000Z"))
          val iter = CloseableIterator(storage.getReader(query))
          // note: need to copy features in iterator as same object is re-used
          val features = iter.map(ScalaSimpleFeature.copy).toList
          features must haveLength(6)
        }
      }
    }

    "filter file paths by dtg" in {
      val dir = Option(getClass.getClassLoader.getResource("example-convert-test-2")).map(_.toURI).orNull
      dir must not(beNull)

      foreach(Seq(true, false)) { useConf =>
        val conf = new Configuration()
        val params = Map.newBuilder[String, String]

        // metadata flags can be set in the params, storage params have to go in the conf
        def addConfig(k: String, v: String): Unit = if (useConf) { conf.set(k, v) } else { params += (k -> v) }

        addConfig(ConverterMetadata.SftConfigParam, sftConfig)
        addConfig(ConverterMetadata.PartitionSchemeParam, "receipt-time")
        addConfig(ConverterMetadata.PartitionOptsPrefix + "datetime-scheme", "daily")
        addConfig(ConverterMetadata.PartitionOptsPrefix + "buffer", "10 minutes")
        addConfig(ConverterMetadata.LeafStorageParam, "false")

        conf.set(ConverterMetadata.ConverterPathParam, "example-convert-test-2")
        conf.set(ConverterStorageFactory.ConverterConfigParam, converterConfig)
        conf.set(ConverterStorageFactory.PathFilterName, "dtg")
        conf.set(ConverterStorageFactory.PathFilterOptsPrefix + "attribute", "dtg")
        conf.set(ConverterStorageFactory.PathFilterOptsPrefix + "pattern", "^data-(.*)\\.csv$")
        conf.set(ConverterStorageFactory.PathFilterOptsPrefix + "format", "yyyyMMddHHmm")
        conf.set(ConverterStorageFactory.PathFilterOptsPrefix + "buffer", "2 hours")

        val context = FileSystemContext(new Path(dir).getParent, conf)
        val catalog = StorageMetadataCatalog(context, "converter", params.result())
        catalog.getTypeNames mustEqual Seq("example")
        val metadata = catalog.load("example")
        metadata must not(beNull)
        metadata must haveClass[ConverterMetadata]
        WithClose(FileSystemStorageFactory("converter").apply(context, metadata)) { storage =>
          val filterText =
            "dtg DURING 2024-12-11T10:00:00Z/2024-12-11T23:55:00Z " +
              "OR dtg = 2024-12-11T10:00:00Z OR dtg = 2024-12-11T23:55:00Z"
          val query = new Query(metadata.sft.getTypeName, ECQL.toFilter(filterText))
          val iter = CloseableIterator(storage.getReader(query))
          // note: need to copy features in iterator as same object is re-used
          val features = iter.map(ScalaSimpleFeature.copy).toList
          // id 1 is excluded because of the path dtg filter even though dtg is within filter bounds
          // id 5 is excluded because dtg is outside filter bounds even though included by path filter
          // id 8 is included because within partition scheme buffer and path filter buffer
          features must haveLength(5)
          features.map(_.getID) must containTheSameElementsAs(Seq("3", "4", "6", "7", "8"))
        }
      }
    }
  }
}
