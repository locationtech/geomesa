/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.filter.PropertyIsLessThan
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.{checkOrder, decomposeAnd}
import org.locationtech.geomesa.filter.expression.AttributeExpression.FunctionLiteral
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class Z2SchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")

  "Z2Scheme" should {

    "partition with a 10 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=10")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 10

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")) mustEqual "0770"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")) mustEqual "0617"
    }

    "partition with a 20 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=20")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 20

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")) mustEqual "0789456"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")) mustEqual "0632516"
    }

    "get intersecting partitions with a 2 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=2")
      ps must beAnInstanceOf[Z2Scheme]
      ps.asInstanceOf[Z2Scheme].bits mustEqual 2

      val almostWholeWorld = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom,-179,-89,179,89)"))
      almostWholeWorld must beSome
      almostWholeWorld.get must haveSize(1)
      almostWholeWorld.get.head.bounds must haveSize(1)
      almostWholeWorld.get.head.bounds.head mustEqual PartitionRange(ps.name, "0", "4")

      val center = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom,-1,-1,1,1)"))
      center must beSome
      center.get must haveSize(1)
      center.get.head.bounds must haveSize(1)
      center.get.head.bounds.head mustEqual PartitionRange(ps.name, "0", "4")

      val north = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom,-10,5,10,6)"))
      north must beSome
      north.get must haveSize(1)
      north.get.head.bounds must haveSize(1)
      north.get.head.bounds.head mustEqual PartitionRange(ps.name, "2", "4")
    }

    "get intersecting partitions with a 4 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=4")

      val wholeWorld = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -180, -90, 180, 90)"))
      wholeWorld must beSome
      wholeWorld.get must haveSize(1)
      wholeWorld.get.head.bounds must haveSize(1)
      wholeWorld.get.head.bounds.head mustEqual PartitionRange(ps.name, "00", "16")

      val nullIsland = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -1, -1, 1, 1)"))
      nullIsland must beSome
      nullIsland.get must haveSize(1)
      nullIsland.get.head.bounds must haveSize(4)
      nullIsland.get.head.bounds must containAllOf(Seq("03", "06", "09", "12").map(p => SinglePartition(ps.name, p)))

      val narrowNorth = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -10, 5, 10, 6)"))
      narrowNorth must beSome
      narrowNorth.get must haveSize(1)
      narrowNorth.get.head.bounds must haveSize(2)
      narrowNorth.get.head.bounds must containAllOf(Seq("09", "12").map(p => SinglePartition(ps.name, p)))

      val wideNorth = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -90, 5, 90, 6)"))
      wideNorth must beSome
      wideNorth.get must haveSize(1)
      wideNorth.get.head.bounds must haveSize(2)
      wideNorth.get.head.bounds must contain(SinglePartition(ps.name, "09"))
      wideNorth.get.head.bounds must contain(PartitionRange(ps.name, "12", "14"))

      val edgeNorth = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 90, 6)"))
      edgeNorth must beSome
      edgeNorth.get must haveSize(1)
      edgeNorth.get.head.bounds must haveSize(2)
      edgeNorth.get.head.bounds must contain(PartitionRange(ps.name, "08", "10"))
      edgeNorth.get.head.bounds must contain(PartitionRange(ps.name, "12", "14"))

      val edgeNorthWide = ps.getIntersectingPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 180, 6)"))
      edgeNorthWide must beSome
      edgeNorthWide.get must haveSize(1)
      edgeNorthWide.get.head.bounds must haveSize(2)
      edgeNorthWide.get.head.bounds must contain(PartitionRange(ps.name, "08", "10"))
      edgeNorthWide.get.head.bounds must contain(PartitionRange(ps.name, "12", "14"))
    }

    "calculate covering filters" in {
      foreach(Seq(2, 4, 8)) { bits =>
        val ps = PartitionSchemeFactory.load(sft, s"z2:bits=$bits")
        ps must beAnInstanceOf[Z2Scheme]
        ps.asInstanceOf[Z2Scheme].bits mustEqual bits
        val partitions = (0 until math.pow(2, bits).toInt).map(_.toString)
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
      val partitions = (0 until 16).map(_.toString)
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
