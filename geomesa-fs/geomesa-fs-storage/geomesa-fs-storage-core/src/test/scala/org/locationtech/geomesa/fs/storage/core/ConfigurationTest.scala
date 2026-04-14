/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class ConfigurationTest extends Specification with AllExpectations {

  "SimpleFeatureTypes" should {

    "configure scheme options in user data" >> {
      val config = "hourly,z2:bits=10"
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      foreach(Seq(
        () => ConfigurationUtils.setScheme(sft, config),
        () => sft.setScheme(config))) { setter =>
          setter()
          sft.removeScheme() must beSome(config.split(",").toSeq)
          sft.removeScheme() must beNone
      }
    }

    "configure observers in user data" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,foo:Date,*bar:Point:srid=4326")
      val setters = Seq(
        () => ConfigurationUtils.setObservers(sft, java.util.Arrays.asList("foo.bar", "foo.baz")),
        () => sft.setObservers(Seq("foo.bar", "foo.baz")))
      foreach(setters) { setter =>
        setter.apply()
        sft.getObservers mustEqual Seq("foo.bar", "foo.baz")
        sft.getUserData.remove(StorageKeys.ObserversKey)
        sft.getObservers must beEmpty
      }
    }
  }
}
