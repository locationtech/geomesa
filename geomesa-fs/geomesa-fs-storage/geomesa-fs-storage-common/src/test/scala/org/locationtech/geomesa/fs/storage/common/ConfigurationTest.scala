/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils
import org.locationtech.geomesa.fs.storage.common.metadata.JdbcMetadata
import org.locationtech.geomesa.fs.storage.common.partitions.{DateTimeScheme, SpatialScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class ConfigurationTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  "SimpleFeatureTypes" should {

    "configure scheme options in user data" >> {
      val config = Map(
        DateTimeScheme.Config.DateTimeFormatOpt -> "yyyy/DDD/HH",
        DateTimeScheme.Config.StepUnitOpt       -> "HOURS",
        DateTimeScheme.Config.StepOpt           -> "1",
        DateTimeScheme.Config.DtgAttribute      -> "dtg",
        SpatialScheme.Config.GeomAttribute      -> "geom",
        SpatialScheme.Config.Z2Resolution       -> "10"
      )
      val options = NamedOptions("datetime,z2", config)
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      foreach(Seq(
        () => ConfigurationUtils.setScheme(sft, options.name, config.asJava),
        () => sft.setScheme(options.name, options.options))) { setter =>
          setter()
          sft.removeScheme() must beSome(options)
          sft.removeScheme() must beNone
      }
    }

    "configure metadata options in user data" >> {
      val config = Map(
        JdbcMetadata.Config.UrlKey      -> "jdbc:h2:split:/tmp/meta",
        JdbcMetadata.Config.UserKey     -> "user",
        JdbcMetadata.Config.PasswordKey -> "pass"
      )
      val options = NamedOptions(JdbcMetadata.MetadataType, config)
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      foreach(Seq(
        () => ConfigurationUtils.setMetadata(sft, options.name, config.asJava),
        () => sft.setMetadata(options.name, options.options))) { setter =>
          setter()
          sft.removeMetadata() must beSome(options)
          sft.removeMetadata() must beNone
      }
    }

    "configure encoding option in user data" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,foo:Date,*bar:Point:srid=4326")
      foreach(Seq(() => ConfigurationUtils.setEncoding(sft, "orc"), () => sft.setEncoding("orc"))) { setter =>
        setter()
        sft.removeEncoding() must beSome("orc")
        sft.removeEncoding() must beNone
      }
    }

    "configure leaf storage option in user data" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,foo:Date,*bar:Point:srid=4326")
      foreach(Seq(() => ConfigurationUtils.setLeafStorage(sft, true), () => sft.setLeafStorage(true))) { setter =>
        setter()
        sft.removeLeafStorage() must beSome(true)
        sft.removeLeafStorage() must beNone
      }
    }
  }
}
