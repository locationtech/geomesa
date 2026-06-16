/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.filter.{Filter, PropertyIsLessThan}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.expression.AttributeExpression.FunctionLiteral
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.locationtech.geomesa.filter.{checkOrder, decomposeAnd}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.util.HexFormat

class Z2SchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")

  val hexFormat = HexFormat.of()
  val hex = Seq.tabulate(16)(hexFormat.toHexDigits(_).takeRight(1))

  "Z2Scheme" should {

    "partition with a 12 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=12")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 12

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "c0b"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "9a6"
    }

    "partition with a 20 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=20")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 20

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "c0bd0"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "9a6c4"
    }

    "get intersecting partitions with a 4 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=4")

      val wholeWorld = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -180, -90, 180, 90)"))
      wholeWorld must beSome
      wholeWorld.get must haveSize(1)
      wholeWorld.get.head mustEqual PartitionRange(ps.name, "0", "fz")

      val nullIsland = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -1, -1, 1, 1)"))
      nullIsland must beSome
      nullIsland.get must haveSize(4)
      nullIsland.get must contain(PartitionRange(ps.name, "3", "4"))
      nullIsland.get must contain(PartitionRange(ps.name, "6", "7"))
      nullIsland.get must contain(PartitionRange(ps.name, "9", "a"))
      nullIsland.get must contain(PartitionRange(ps.name, "c", "d"))

      val narrowNorth = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -10, 5, 10, 6)"))
      narrowNorth must beSome
      narrowNorth.get must haveSize(2)
      narrowNorth.get must contain(PartitionRange(ps.name, "9", "a"))
      narrowNorth.get must contain(PartitionRange(ps.name, "c", "d"))

      val wideNorth = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -90, 5, 90, 6)"))
      wideNorth must beSome
      wideNorth.get must haveSize(2)
      wideNorth.get must contain(PartitionRange(ps.name, "9", "a"))
      wideNorth.get must contain(PartitionRange(ps.name, "c", "e"))

      val edgeNorth = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -90.000000001, 5, 90, 6)"))
      edgeNorth must beSome
      edgeNorth.get must haveSize(2)
      edgeNorth.get must contain(PartitionRange(ps.name, "8", "a"))
      edgeNorth.get must contain(PartitionRange(ps.name, "c", "e"))

      val edgeNorthWide = ps.getRangesForFilter(ECQL.toFilter("bbox(geom, -90.000000001, 5, 180, 6)"))
      edgeNorthWide must beSome
      edgeNorthWide.get must haveSize(2)
      edgeNorthWide.get must contain(PartitionRange(ps.name, "8", "a"))
      edgeNorthWide.get must contain(PartitionRange(ps.name, "c", "e"))
    }

    "enumerate partitions with a 4 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=4")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 4

      val partitions = ps.getPartitionsForFilter(Filter.INCLUDE).orNull
      partitions must not(beNull)
      partitions must haveLength(16)
      foreach(hex) { digit =>
        partitions must contain(PartitionKey(ps.name, digit))
      }
    }

    "calculate covering filters" in {
      foreach(Seq(4, 8)) { bits =>
        val ps = PartitionSchemeFactory.load(sft, s"z2:bits=$bits")
        ps must beAnInstanceOf[Z2Scheme]
        ps.asInstanceOf[Z2Scheme].bits mustEqual bits
        val partitions = (0 until math.pow(2, bits).toInt).map(p => PartitionKey("", hexFormat.toHexDigits(p).drop(8 - (bits/4))))
        val filters = partitions.map(ps.getCoveringFilter)
        val envelopes = filters.map(BoundsFilterVisitor.visit(_))
        // verify none of the envelopes overlap (common borders are ok)
        foreach(envelopes.tails.toSeq.dropRight(1)) { tails =>
          foreach(tails.tail) { t =>
            val i = t.intersection(tails.head)
            i.isEmpty || i.getWidth == 0 || i.getHeight == 0 must beTrue
          }
        }
        // verify the envelopes cover the entire world
        envelopes.map(_.getArea).sum mustEqual 360d * 180
      }
    }

    "exclude endpoints in covering filters" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=4")
      val partitions = hex.map(PartitionKey("", _))
      val checks = partitions.map { p =>
        val filter = ps.getCoveringFilter(p)
        val decomposed = decomposeAnd(filter)
        val envelope = BoundsFilterVisitor.visit(filter)
        val xInclusive = envelope.getMaxX == 180d
        val yInclusive = envelope.getMaxY == 90d
        (decomposed, xInclusive, yInclusive)
      }

      checks.count { case (_, xInclusive, yInclusive) => xInclusive && yInclusive } mustEqual 1
      checks.count { case (_, xInclusive, _) => xInclusive } mustEqual 4
      checks.count { case (_, _, yInclusive) => yInclusive } mustEqual 4

      foreach(checks) { case (decomposed, xInclusive, yInclusive) =>
        val functions = decomposed.collect { case lt: PropertyIsLessThan =>
          checkOrder(lt.getExpression2, lt.getExpression1) match {
            case Some(f: FunctionLiteral) => f.function.getName
            case _ => null
          }
        }
        if (xInclusive && yInclusive) {
          decomposed must haveLength(1)
        } else if (xInclusive) {
          decomposed must haveLength(2)
          functions mustEqual Seq("getY")
        } else if (yInclusive) {
          decomposed must haveLength(2)
          functions mustEqual Seq("getX")
        } else {
          decomposed must haveLength(3)
          functions must containTheSameElementsAs(Seq("getX", "getY"))
        }
      }
    }
  }
}
