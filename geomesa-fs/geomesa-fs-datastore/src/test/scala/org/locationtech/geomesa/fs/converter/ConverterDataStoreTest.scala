/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.converter

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created by hulbert on 6/21/17.
  */
@RunWith(classOf[JUnitRunner])
class ConverterDataStoreTest extends Specification {

  sequential

  "ConverterDataStore" should {
    "work with one datastore" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path"     -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding" -> "converter",
        "fs.config"   ->
          """
            |fs.options.sft.name=fs-test
            |fs.options.converter.name=fs-test
            |fs.options.converter.path=datastore1
            |fs.partition-scheme.name=datetime
            |fs.partition-scheme.opts.datetime-format=yyyy/DDD/HH/mm
            |fs.partition-scheme.opts.step-unit=MINUTES
            |fs.partition-scheme.opts.step=15
            |fs.partition-scheme.opts.dtg-attribute=dtg
            |fs.partition-scheme.opts.leaf-storage=true
          """.stripMargin
      ))
      ds must not(beNull)

      val types = ds.getTypeNames
      types must haveSize(1)
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val fr = ds.getFeatureReader(q, Transaction.AUTO_COMMIT)
      val feats = mutable.ListBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        feats += fr.next()
      }
      feats.size mustEqual 4
    }

    "work with something else" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path"     -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding" -> "converter",
        "fs.config"   ->
            """
              |fs.options.sft.name=fs-test
              |fs.options.converter.name=fs-test
              |fs.options.converter.path=datastore2
              |fs.partition-scheme.name=datetime
              |fs.partition-scheme.opts.datetime-format=yyyy/DDD/HH/mm
              |fs.partition-scheme.opts.step-unit=MINUTES
              |fs.partition-scheme.opts.step=15
              |fs.partition-scheme.opts.dtg-attribute=dtg
              |fs.partition-scheme.opts.leaf-storage=true
            """.stripMargin
      ))
      ds must not(beNull)

      val types = ds.getTypeNames
      types must haveSize(1)
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val fr = ds.getFeatureReader(q, Transaction.AUTO_COMMIT)
      val feats = mutable.ListBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        feats += fr.next()
      }
      feats.size mustEqual 4
    }

    "load sft as a string" >> {

      val conf = ConfigFactory.parseString(
        """
          |geomesa {
          |  sfts {
          |    "fs-test" = {
          |      attributes = [
          |        { name = "name", type = "String", index = true                              }
          |        { name = "dtg",  type = "Date",   index = false                             }
          |        { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
          |      ]
          |    }
          |  }
          |  converters {
          |    "fs-test" {
          |      type   = "delimited-text",
          |      format = "CSV",
          |      options {
          |        skip-lines = 0
          |      },
          |      id-field = "toString($name)",
          |      fields = [
          |        { name = "name", transform = "$1::string"   }
          |        { name = "dtg",  transform = "dateTime($2)" }
          |        { name = "geom", transform = "point($3)"    }
          |      ]
          |    }
          |
          |  }
          |}
        """.stripMargin
      ).root().render(ConfigRenderOptions.concise)

      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path"     -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding" -> "converter",
        "fs.config"   ->
            s"""
              |fs.options.sft.conf=$conf
              |fs.options.converter.conf=$conf
              |fs.options.converter.path=datastore1
              |fs.partition-scheme.name=datetime
              |fs.partition-scheme.opts.datetime-format=yyyy/DDD/HH/mm
              |fs.partition-scheme.opts.step-unit=MINUTES
              |fs.partition-scheme.opts.step=15
              |fs.partition-scheme.opts.dtg-attribute=dtg
              |fs.partition-scheme.opts.leaf-storage=true
            """.stripMargin
      ))

      ds must not(beNull)

      val types = ds.getTypeNames
      types must haveSize(1)
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val fr = ds.getFeatureReader(q, Transaction.AUTO_COMMIT)
      val feats = mutable.ListBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        feats += fr.next()
      }
      feats.size mustEqual 4
    }
  }
}
