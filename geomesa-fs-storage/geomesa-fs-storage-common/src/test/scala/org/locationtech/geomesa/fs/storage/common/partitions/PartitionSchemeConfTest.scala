/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.StorageSerialization
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.util.Try

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
          |     step = 2
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
      scheme.asInstanceOf[CompositeScheme].schemes must haveLength(2)
      scheme.asInstanceOf[CompositeScheme].schemes must
        contain(beAnInstanceOf[DateTimeScheme], beAnInstanceOf[Z2Scheme]).copy(checkOrder = true)
      scheme.asInstanceOf[CompositeScheme].schemes.collectFirst { case d: DateTimeScheme => d.step } must beSome(2)
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

    "throw appropriate errors when a composite scheme can't be loaded" >> {
      // note: missing datetime-format
      val conf =
        """
          | {
          |   scheme = "datetime,z2"
          |   options = {
          |     step-unit = HOURS
          |     z2-resolution = 10
          |   }
          | }
        """.stripMargin

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,foo:Date,*bar:Point:srid=4326")
      val loaded = Try(PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf)))
      loaded must beAFailedTry
      val e = loaded.failed.get
      e must beAnInstanceOf[IllegalArgumentException]
      e.getCause must not(beNull) // verify cause is from the failed delegate load
    }
  }
}
