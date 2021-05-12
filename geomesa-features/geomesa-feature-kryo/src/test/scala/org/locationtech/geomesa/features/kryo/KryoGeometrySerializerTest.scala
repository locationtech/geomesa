/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.features.serialization.{GeometryLengthThreshold, GeometryNestingThreshold, TwkbSerialization}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
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
        val out = new Output(512)
        val serializers = Seq(
          KryoGeometrySerialization.serialize(out, _: Geometry),
          KryoGeometrySerialization.serializeWkb(out, _: Geometry)
        )
        foreach(serializers) { serializer =>
          foreach(geoms) { geom =>
            out.clear()
            serializer(geom)
            val bytes = out.toBytes
            // ensure we didn't write 3 dimensions
            bytes.length must beLessThan(geom.getCoordinates.length * 24)
            KryoGeometrySerialization.deserialize(new Input(bytes)) mustEqual geom
          }
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

    "correctly serialize and deserialize geometries with n dimensions" in {
      val zs = Seq(
        "LINESTRING Z (0 2 0, 2 0 1, 8 6 2)",
        "POLYGON Z ((20 10 0, 30 0 10, 40 10 10, 30 20 0, 20 10 0))",
        "MULTIPOINT Z (0 0 0, 2 2 2)",
        "MULTILINESTRING Z ((0 2 0, 2 0 1, 8 6 2),(0 2 0, 2 0 0, 8 6 0))",
        "MULTIPOLYGON Z (((-1 0 0, 0 1 0, 1 0 0, 0 -1 0, -1 0 0)), ((-2 6 2, 1 6 3, 1 3 3, -2 3 3, -2 6 2)), " +
            "((-1 5 0, 2 5 0, 2 2 0, -1 2 0, -1 5 0)))",
        "MULTIPOINT Z (0 0 2, 2 2 0)",
        "POINT Z (55.0 49.0 37.0)"
      ).map(WKTUtils.read)

      val ms = Seq(
        "LINESTRING M (0 2 0, 2 0 1, 8 6 2)",
        "POLYGON M ((20 10 0, 30 0 10, 40 10 10, 30 20 0, 20 10 0))",
        "MULTIPOINT M (0 0 0, 2 2 2)",
        "MULTILINESTRING M ((0 2 0, 2 0 1, 8 6 2),(0 2 0, 2 0 0, 8 6 0))",
        "MULTIPOLYGON M (((-1 0 0, 0 1 0, 1 0 0, 0 -1 0, -1 0 0)), ((-2 6 2, 1 6 3, 1 3 3, -2 3 3, -2 6 2)), " +
            "((-1 5 0, 2 5 0, 2 2 0, -1 2 0, -1 5 0)))",
        "MULTIPOINT M (0 0 2, 2 2 0)",
        "POINT M (55.0 49.0 37.0)"
      ).map(WKTUtils.read)

      val zms = Seq(
        "LINESTRING ZM (0 2 0 2, 2 0 1 1, 8 6 2 0)",
        "POLYGON ZM ((20 10 0 55, 30 0 10 45, 40 10 10 -45, 30 20 0 -30, 20 10 0 55))",
        "MULTIPOINT ZM (0 0 0 -1, 2 2 2 0)",
        "MULTILINESTRING ZM ((0 2 0 1, 2 0 1 2, 8 6 2 3),(0 2 0 4, 2 0 0 5, 8 6 0 6))",
        "MULTIPOLYGON ZM (((-1 0 0 4, 0 1 0 4, 1 0 0 4, 0 -1 0 4, -1 0 0 4)), " +
            "((-2 6 2 3, 1 6 3 3, 1 3 3 3, -2 3 3 3, -2 6 2 3)), " +
            "((-1 5 0 2, 2 5 0 2, 2 2 0 2, -1 2 0 2, -1 5 0 2)))",
        "MULTIPOINT ZM (0 0 2 5, 2 2 0 5)",
        "POINT ZM (55.0 49.0 37.0 5)"
      ).map(WKTUtils.read)

      val out = new Output(512)
      val serializers = Seq(
        KryoGeometrySerialization.serialize(out, _: Geometry),
        KryoGeometrySerialization.serializeWkb(out, _: Geometry)
      )
      foreach(serializers) { serializer =>
        foreach(zs) { geom =>
          java.lang.Double.isNaN(geom.getCoordinate.getZ) must beFalse
          out.clear()
          serializer(geom)
          val deserialized = KryoGeometrySerialization.deserialize(new Input(out.toBytes))
          java.lang.Double.isNaN(deserialized.getCoordinate.getZ) must beFalse
          deserialized mustEqual geom
          compare(deserialized.getCoordinates, geom.getCoordinates)
        }
        foreach(ms) { geom =>
          java.lang.Double.isNaN(geom.getCoordinate.getM) must beFalse
          out.clear()
          serializer(geom)
          val deserialized = KryoGeometrySerialization.deserialize(new Input(out.toBytes))
          java.lang.Double.isNaN(deserialized.getCoordinate.getM) must beFalse
          deserialized mustEqual geom
          compare(deserialized.getCoordinates, geom.getCoordinates)
        }
        foreach(zms) { geom =>
          java.lang.Double.isNaN(geom.getCoordinate.getZ) must beFalse
          java.lang.Double.isNaN(geom.getCoordinate.getM) must beFalse
          out.clear()
          serializer(geom)
          val deserialized = KryoGeometrySerialization.deserialize(new Input(out.toBytes))
          java.lang.Double.isNaN(deserialized.getCoordinate.getZ) must beFalse
          java.lang.Double.isNaN(deserialized.getCoordinate.getM) must beFalse
          deserialized mustEqual geom
          compare(deserialized.getCoordinates, geom.getCoordinates)
        }
      }
    }

    "allow limits on length of geometries" in {
      GeometryLengthThreshold.threadLocalValue.set("3")
      try {
        // create a new deserializer to pick up the sys prop change
        val deserializer = new TwkbSerialization[Output, Input](){}
        val out = new Output(512)
        val serializers = Seq(
          KryoGeometrySerialization.serialize(out, _: Geometry),
          KryoGeometrySerialization.serializeWkb(out, _: Geometry)
        )
        foreach(serializers) { serializer =>
          out.clear()
          serializer.apply(WKTUtils.read("LINESTRING (0 0, 1 1, 2 2)"))
          deserializer.deserialize(new Input(out.toBytes)) must not(beNull)
          out.clear()
          serializer.apply(WKTUtils.read("LINESTRING (0 0, 1 1, 2 2, 3 3)"))
          deserializer.deserialize(new Input(out.toBytes)) must beNull
        }
      } finally {
        GeometryLengthThreshold.threadLocalValue.remove()
      }
    }

    "allow limits on nesting of geometry collections" in {
      GeometryNestingThreshold.threadLocalValue.set("1")
      try {
        // create a new deserializer to pick up the sys prop change
        val deserializer = new TwkbSerialization[Output, Input](){}
        val out = new Output(512)
        val serializers = Seq(
          KryoGeometrySerialization.serialize(out, _: Geometry),
          KryoGeometrySerialization.serializeWkb(out, _: Geometry)
        )
        foreach(serializers) { serializer =>
          out.clear()
          serializer.apply(WKTUtils.read("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 0)))"))
          deserializer.deserialize(new Input(out.toBytes)) must not(beNull)
          out.clear()
          serializer.apply(WKTUtils.read("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 0))))"))
          deserializer.deserialize(new Input(out.toBytes)) must beNull
        }
      } finally {
        GeometryNestingThreshold.threadLocalValue.remove()
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

  def compare(c1: Array[Coordinate], c2: Array[Coordinate]): MatchResult[Any] = {
    c1.length mustEqual c2.length
    forall(c1.zip(c2)) { case (c1, c2) =>
      c1.getX mustEqual c2.getX
      c1.getY mustEqual c2.getY
      if (java.lang.Double.isNaN(c1.getZ)) {
        java.lang.Double.isNaN(c2.getZ) must beTrue
      } else {
        c1.getZ mustEqual c2.getZ
      }
      if (java.lang.Double.isNaN(c1.getM)) {
        java.lang.Double.isNaN(c2.getM) must beTrue
      } else {
        c1.getM mustEqual c2.getM
      }
    }
  }
}
