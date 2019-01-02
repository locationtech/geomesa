/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import org.apache.avro.generic._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AvroPathTest extends Specification with AvroUtils {

  sequential

  "AvroPath" should {
    "select a top level path" in {
      val path = "/content"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "select from a union by schema type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "return None when element in union has wrong type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr2)
      result.isDefined mustEqual false
    }

    "return nested records" in {
      val path = "/content$type=TObj/kvmap"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val arr = result.get.asInstanceOf[GenericArray[GenericRecord]]
      arr.length mustEqual 5
    }

    "filter arrays of records by a field predicate" in {
      val path = "/content$type=TObj/kvmap[$k=lat]"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val r = result.get.asInstanceOf[GenericRecord]
      r.get("v").asInstanceOf[Double] mustEqual 45.0
    }

    "select a property out of a record in an array" in {
      "filter arrays of records by a field predicate" in {
        val path = "/content$type=TObj/kvmap[$k=lat]/v"
        val avroPath = AvroPath(path)
        val result = avroPath.eval(gr1)
        result.isDefined mustEqual true
        val v = result.get.asInstanceOf[Double]
        v mustEqual 45.0
      }
    }
  }
}
