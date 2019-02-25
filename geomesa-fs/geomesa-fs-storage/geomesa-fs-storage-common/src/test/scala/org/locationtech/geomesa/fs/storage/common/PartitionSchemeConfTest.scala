/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.{CompositeScheme, DateTimeScheme, Z2Scheme}
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
      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))
      scheme must beAnInstanceOf[CompositeScheme]
    }

    "load, serialize, deserialize" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val options = NamedOptions("daily,z2-2bit")
      val scheme = PartitionSchemeFactory.load(sft, options)
      scheme must beAnInstanceOf[CompositeScheme]

      val schemeStr = StorageSerialization.serialize(options)
      val scheme2 = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(schemeStr))
      scheme2 mustEqual scheme
    }

    "load dtg, geom, and step defaults" >> {
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
      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))
      scheme must beAnInstanceOf[CompositeScheme]
      scheme.asInstanceOf[CompositeScheme].schemes must haveLength(2)
      scheme.asInstanceOf[CompositeScheme].schemes must
          contain(beAnInstanceOf[DateTimeScheme], beAnInstanceOf[Z2Scheme]).copy(checkOrder = true)
    }
  }
}
