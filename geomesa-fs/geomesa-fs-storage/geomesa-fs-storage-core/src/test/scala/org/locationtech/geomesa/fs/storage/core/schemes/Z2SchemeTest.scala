/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.filter.PropertyIsLessThan
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
