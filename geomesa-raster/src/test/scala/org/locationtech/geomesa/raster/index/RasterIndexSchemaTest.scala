/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.index

import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.data.ResolutionPlanner
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterIndexSchemaTest extends Specification {

  "RasterIndexSchemaTest" should {
    "parse a valid string containing a positive resolution and positive exponent" in {
      val res = 1.000000e+02
      val s = s"%~#s%${lexiEncodeDoubleToString(res)}#ires%0,1#gh%99#r::%~#s%1,5#gh::%~#s%5,2#gh%15#id"
      val matched = RasterIndexSchema.buildKeyPlanner(s) match {
        case CompositePlanner(List(ResolutionPlanner(100.0), GeoHashKeyPlanner(0,1), RandomPartitionPlanner(99)),"~") => true
        case _ => false
      }
      matched must beTrue
    }

    "parse a valid string containing a negative resolution and positive exponent" in {
      val res = -1.000000e+02
      val s = s"%~#s%${lexiEncodeDoubleToString(res)}#ires%0,1#gh%99#r::%~#s%1,5#gh::%~#s%5,2#gh%15#id"
      val matched = RasterIndexSchema.buildKeyPlanner(s) match {
        case CompositePlanner(List(ResolutionPlanner(-100.0), GeoHashKeyPlanner(0,1), RandomPartitionPlanner(99)),"~") => true
        case _ => false
      }
      matched must beTrue
    }

    "parse a valid string containing a positive resolution and negative exponent" in {
      val res = 1.000000e-02
      val s = s"%~#s%${lexiEncodeDoubleToString(res)}#ires%0,1#gh%99#r::%~#s%1,5#gh::%~#s%5,2#gh%15#id"
      val matched = RasterIndexSchema.buildKeyPlanner(s) match {
        case CompositePlanner(List(ResolutionPlanner(0.01), GeoHashKeyPlanner(0,1), RandomPartitionPlanner(99)),"~") => true
        case _ => false
      }
      matched must beTrue
    }

    "parse a valid string containing a negative resolution and negative exponent" in {
      val res = -1.000000e-02
      val s = s"%~#s%${lexiEncodeDoubleToString(res)}#ires%0,1#gh%99#r::%~#s%1,5#gh::%~#s%5,2#gh%15#id"
      val matched = RasterIndexSchema.buildKeyPlanner(s) match {
        case CompositePlanner(List(ResolutionPlanner(-0.01), GeoHashKeyPlanner(0,1), RandomPartitionPlanner(99)),"~") => true
        case _ => false
      }
      matched must beTrue
    }

  }

  "RasterIndexSchemaBuilder" should {
    "be able to create index schema with resolution" in {
      val maxShard = 31
      val name = "test"
      val res = 100.0
      val oldSchema = s"%~#s%$maxShard#r%$name#cstr%${lexiEncodeDoubleToString(res)}#ires%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id"

      val schema = new RasterIndexSchemaBuilder("~")
        .randomNumber(maxShard)
        .constant(name)
        .resolution(res)
        .geoHash(0, 3)
        .date("yyyyMMdd")
        .nextPart()
        .geoHash(3, 2)
        .nextPart()
        .id()
        .build()

      schema must be equalTo oldSchema
    }

    "be able to create index schema with resolution and band cf" in {
      val maxShard = 31
      val name = "test"
      val res = 100.0
      val band = "RGB"
      val oldSchema = s"%~#s%$maxShard#r%$name#cstr%${lexiEncodeDoubleToString(res)}#ires%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh%$band#b::%~#s%#id"

      val schema = new RasterIndexSchemaBuilder("~")
        .randomNumber(maxShard)
        .constant(name)
        .resolution(100.0)
        .geoHash(0, 3)
        .date("yyyyMMdd")
        .nextPart()
        .geoHash(3, 2)
        .band(band)
        .nextPart()
        .id()
        .build()

      schema must be equalTo oldSchema
    }
  }

}
