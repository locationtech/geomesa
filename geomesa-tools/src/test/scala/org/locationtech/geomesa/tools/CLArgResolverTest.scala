/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import com.beust.jcommander.ParameterException
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CLArgResolverTest extends Specification {

  sequential

  val sftSpec = "name:String,age:Integer,*geom:Point:srid=4326"
  val featureName = "specTest"
  val sftConfig =
    """
      | geomesa.sfts.specTest = {
      |  attributes = [
      |    { name = "name", type = "String", index = false },
      |    { name = "age",  type = "Integer", index = false },
      |    { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
      |  ]
      |}
    """.stripMargin

  val data =
    """
      |Andrew,30,Point(45 56)
      |Jim,33,Point(46 46)
      |Anthony,35,Point(47 47)
    """.stripMargin

  val convertConfig =
    """
      | geomesa.converters.foobarConverter = {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "name",  transform = "$1" },
      |     { name = "age",   transform = "$2" },
      |     { name = "geom",  transform = "point($3)" }
      |   ]
      | }
    """.stripMargin

  val combined =
    """
      | geomesa.sfts.specTest = {
      |   attributes = [
      |     { name = "name", type = "String", index = false },
      |     { name = "age",  type = "Integer", index = false },
      |     { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
      |   ]
      | },
      | geomesa.converters.foobar = {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "name",  transform = "$1" },
      |     { name = "age",   transform = "$2" },
      |     { name = "geom",  transform = "point($3)" }
      |   ]
      | }
      """.stripMargin

  "CLArgResolver" should {
    "work with " >> {
      val sft = CLArgResolver.getSft(sftSpec, featureName)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "fail when given spec and no feature name" >> {
      CLArgResolver.getSft(sftSpec, null) must throwA[ParameterException]
    }

    "create a spec from sft conf" >> {
      val sft = CLArgResolver.getSft(sftConfig, null)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config" >> {
      val sft = CLArgResolver.getSft(combined, null)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }
  }
}
