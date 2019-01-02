/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
        foreach(geoms) { geom =>
          val out = new Output(512)
          KryoGeometrySerialization.serialize(out, geom)
          val in = new Input(out.toBytes)
          val deserialized = KryoGeometrySerialization.deserialize(in)
          deserialized mustEqual geom
        }
      }
      "using streams" >> {
        foreach(geoms) { geom =>
          val out = new Output(new ByteArrayOutputStream(), 512)
          KryoGeometrySerialization.serialize(out, geom)
          val in = new Input(new ByteArrayInputStream(out.toBytes))
          val deserialized = KryoGeometrySerialization.deserialize(in)
          deserialized mustEqual geom
        }
      }
    }

    "be backwards compatible with geometry collections" in {
      val geoms = Seq(
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
        "MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
        "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))",
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
        "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))"
      ).map(WKTUtils.read)

      val serialized126 = Seq(
        "1,4,4,1,1,64,36,0,0,0,0,0,0,64,68,0,0,0,0,0,0,1,1,64,68,0,0,0,0,0,0,64,62,0,0,0,0,0,0,1,1,64,52,0,0,0,0,0,0,64,52,0,0,0,0,0,0,1,1,64,62,0,0,0,0,0,0,64,36,0,0,0,0,0,0",
        "1,4,4,1,1,64,36,0,0,0,0,0,0,64,68,0,0,0,0,0,0,1,1,64,68,0,0,0,0,0,0,64,62,0,0,0,0,0,0,1,1,64,52,0,0,0,0,0,0,64,52,0,0,0,0,0,0,1,1,64,62,0,0,0,0,0,0,64,36,0,0,0,0,0,0",
        "1,5,2,1,2,3,64,36,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,68,0,0,0,0,0,0,1,2,4,64,68,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,36,0,0,0,0,0,0",
        "1,6,2,1,3,4,64,62,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,70,-128,0,0,0,0,0,64,68,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,52,0,0,0,0,0,0,0,1,3,5,64,46,0,0,0,0,0,0,64,20,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,20,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,46,0,0,0,0,0,0,64,20,0,0,0,0,0,0,0",
        "1,6,2,1,3,4,64,68,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,70,-128,0,0,0,0,0,64,70,-128,0,0,0,0,0,64,62,0,0,0,0,0,0,64,68,0,0,0,0,0,0,64,68,0,0,0,0,0,0,0,1,3,6,64,52,0,0,0,0,0,0,64,65,-128,0,0,0,0,0,64,36,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,36,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,20,0,0,0,0,0,0,64,70,-128,0,0,0,0,0,64,52,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,65,-128,0,0,0,0,0,1,4,64,62,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,46,0,0,0,0,0,0,64,52,0,0,0,0,0,0,64,57,0,0,0,0,0,0,64,62,0,0,0,0,0,0,64,52,0,0,0,0,0,0"
      ).map(_.split(",").map(_.toByte))

      forall(geoms.zip(serialized126)) { case (geom, bytes) =>
        val in = new Input(new ByteArrayInputStream(bytes))
        val read = KryoGeometrySerialization.deserialize(in)
        read mustEqual geom
      }
    }
  }
}
