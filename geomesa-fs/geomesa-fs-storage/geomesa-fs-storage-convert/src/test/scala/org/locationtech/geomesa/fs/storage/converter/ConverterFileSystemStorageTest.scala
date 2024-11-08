/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorageFactory, StorageMetadataFactory}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConverterFileSystemStorageTest extends Specification with LazyLogging {

  "ConverterFileSystemStorage" should {
    "read features in compressed tar.gz files" in {
      val dir = Option(getClass.getClassLoader.getResource("example-convert-test")).map(_.toURI).orNull
      dir must not(beNull)

      val conf = new Configuration()
      conf.set(ConverterStorageFactory.ConverterPathParam, "example-convert-test")
      conf.set(ConverterStorageFactory.SftConfigParam,
        """geomesa.sfts.example = {
          |  attributes = [
          |    { name = "name", type = "String"                             }
          |    { name = "age",  type = "Int"                                }
          |    { name = "dtg",  type = "Date", default = true               }
          |    { name = "geom", type = "Point", srid = 4326, default = true }
          |  ]
          |}""".stripMargin)
      conf.set(ConverterStorageFactory.ConverterConfigParam,
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
          |}""".stripMargin)
      conf.set(ConverterStorageFactory.PartitionSchemeParam, "daily")
      conf.set(ConverterStorageFactory.LeafStorageParam, "false")

      val context = FileSystemContext(new Path(dir), conf)
      val metadata = StorageMetadataFactory.load(context).orNull
      metadata must not(beNull)
      val storage = FileSystemStorageFactory(context, metadata)

      val query = new Query(metadata.sft.getTypeName, ECQL.toFilter("dtg during 2023-01-17T00:00:00.000Z/2023-01-19T00:00:00.000Z"))
      val features = {
        val iter = SelfClosingIterator(storage.getReader(query))
        // note: need to copy features in iterator as same object is re-used
        iter.map(ScalaSimpleFeature.copy).toList
      }

      features must haveLength(6)
    }
  }
}
