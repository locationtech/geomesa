/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.languageFeature.postfixOps

@RunWith(classOf[JUnitRunner])
class KryoGeometrySerializerTest extends Specification {

  "KryoGeometrySerializer" should {

    "correctly serialize and deserialize different geometries" in {
      val geoms = Seq(
        "LINESTRING(0 2, 2 0, 8 6)",
        "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))",
        "MULTIPOINT(0 0, 2 2)",
        "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))",
        "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))",
        "MULTIPOINT(0 0, 2 2)",
        "POINT(55.0 49.0)"
      ).map(WKTUtils.read)

      "using byte arrays" >> {
        geoms.foreach { geom =>
          val out = new Output(512)
          KryoGeometrySerialization.serialize(out, geom)
          val in = new Input(out.toBytes)
          val deserialized = KryoGeometrySerialization.deserialize(in)
          deserialized mustEqual geom
        }
        success
      }
      "using streams" >> {
        geoms.foreach { geom =>
          val out = new Output(new ByteArrayOutputStream(), 512)
          KryoGeometrySerialization.serialize(out, geom)
          val in = new Input(new ByteArrayInputStream(out.toBytes))
          val deserialized = KryoGeometrySerialization.deserialize(in)
          deserialized mustEqual geom
        }
        success
      }
    }
  }
}
