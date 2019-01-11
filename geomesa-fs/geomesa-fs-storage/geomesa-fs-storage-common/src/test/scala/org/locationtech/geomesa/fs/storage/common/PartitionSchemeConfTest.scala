/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.partitions.{CompositeScheme, DateTimeScheme, SpatialPartitionSchemeConfig, Z2Scheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class PartitionSchemeConfTest extends Specification with AllExpectations {

  sequential

  "PartitionScheme" should {
    "load from conf" >> {
      val conf =
        """
          | {
          |   scheme = "datetime,z2"
          |   options = {
          |     datetime-format = "yyyy/DDD/HH"
          |     step-unit = HOURS
          |     step = 1
          |     dtg-attribute = dtg
          |     geom-attribute = geom
          |     z2-resolution = 10
          |     leaf-storage = true
          |   }
          | }
        """.stripMargin

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val scheme = PartitionScheme(sft, ConfigFactory.parseString(conf))

      scheme must not(beNull)
      scheme must beAnInstanceOf[CompositeScheme]
    }

    "load, serialize, deserialize" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val scheme = PartitionScheme.apply(sft, "daily,z2-2bit")
      scheme must beAnInstanceOf[CompositeScheme]

      val schemeStr = PartitionScheme.toConfig(scheme).root().render(ConfigRenderOptions.concise)

      val scheme2 = PartitionScheme.apply(sft, ConfigFactory.parseString(schemeStr))
      scheme2 must beAnInstanceOf[CompositeScheme]
    }

    "load dtg, geom, step, and leaf defaults" >> {
      val conf =
        """
          | {
          |   scheme = "datetime,z2"
          |   options = {
          |     datetime-format = "yyyy/DDD/HH"
          |     step-unit = HOURS
          |     z2-resolution = 10
          |   }
          | }
        """.stripMargin

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,foo:Date,*bar:Point:srid=4326")
      val scheme = PartitionScheme(sft, ConfigFactory.parseString(conf))

      scheme must not(beNull)
      scheme must beAnInstanceOf[CompositeScheme]

      scheme.isLeafStorage must beTrue
      val opts = scheme.getOptions
      opts.get(SpatialPartitionSchemeConfig.GeomAttribute) mustEqual "bar"
      opts.get(DateTimeScheme.Config.DtgAttribute) mustEqual "foo"
      opts.get(DateTimeScheme.Config.StepOpt).toInt mustEqual 1
      opts.get(SpatialPartitionSchemeConfig.LeafStorage).toBoolean must beTrue
    }
  }
}
