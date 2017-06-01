/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BoundsTest extends Specification {

  "Bounds" should {

    "merge different types" >> {
      "ints" >> {
        val leftLower: Option[java.lang.Integer]  = Some(0)
        val leftUpper: Option[java.lang.Integer]  = Some(10)
        val rightLower: Option[java.lang.Integer] = Some(5)
        val rightUpper: Option[java.lang.Integer] = Some(15)
        val left  = Bounds(leftLower, leftUpper, inclusive = true)
        val right = Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "longs" >> {
        val leftLower: Option[java.lang.Long]  = Some(0L)
        val leftUpper: Option[java.lang.Long]  = Some(10L)
        val rightLower: Option[java.lang.Long] = Some(5L)
        val rightUpper: Option[java.lang.Long] = Some(15L)
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "floats" >> {
        val leftLower: Option[java.lang.Float]  = Some(0f)
        val leftUpper: Option[java.lang.Float]  = Some(10f)
        val rightLower: Option[java.lang.Float] = Some(5f)
        val rightUpper: Option[java.lang.Float] = Some(15f)
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "doubles" >> {
        val leftLower: Option[java.lang.Double]  = Some(0d)
        val leftUpper: Option[java.lang.Double]  = Some(10d)
        val rightLower: Option[java.lang.Double] = Some(5d)
        val rightUpper: Option[java.lang.Double] = Some(15d)
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "strings" >> {
        val leftLower: Option[String]  = Some("0")
        val leftUpper: Option[String]  = Some("6")
        val rightLower: Option[String] = Some("3")
        val rightUpper: Option[String] = Some("9")
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "dates" >> {
        val leftLower: Option[Date]  = Some(new Date(0))
        val leftUpper: Option[Date]  = Some(new Date(10))
        val rightLower: Option[Date] = Some(new Date(5))
        val rightUpper: Option[Date] = Some(new Date(15))
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
      "uuids" >> {
        val leftLower: Option[UUID]  = Some(UUID.fromString("00000000-0000-0000-0000-000000000000"))
        val leftUpper: Option[UUID]  = Some(UUID.fromString("00000000-0000-0000-0000-000000000006"))
        val rightLower: Option[UUID] = Some(UUID.fromString("00000000-0000-0000-0000-000000000003"))
        val rightUpper: Option[UUID] = Some(UUID.fromString("00000000-0000-0000-0000-000000000009"))
        val left  =  Bounds(leftLower, leftUpper, inclusive = true)
        val right =  Bounds(rightLower, rightUpper, inclusive = true)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper, inclusive = true))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper, inclusive = true))
      }
    }

    "merge simple ands/ors" >> {
      "for strings" >> {
        val bounds = Bounds(Some("b"), Some("f"), inclusive = true)
        "overlapping" >> {
          val toMerge = Bounds(Some("d"), Some("i"), inclusive = true)
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Some("d"), Some("f"), inclusive = true))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Some("b"), Some("i"), inclusive = true))
        }
        "disjoint" >> {
          val toMerge = Bounds(Some("i"), Some("z"), inclusive = true)
          Bounds.intersection(bounds, toMerge) must beNone
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(
            Bounds(Some("b"), Some("f"), inclusive = true),
            Bounds(Some("i"), Some("z"), inclusive = true)
          )
        }
        "contained" >> {
          val toMerge = Bounds(Some("c"), Some("d"), inclusive = true)
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Some("c"), Some("d"), inclusive = true))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Some("b"), Some("f"), inclusive = true))
        }
        "containing" >> {
          val toMerge = Bounds(Some("a"), Some("i"), inclusive = true)
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Some("b"), Some("f"), inclusive = true))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Some("a"), Some("i"), inclusive = true))
        }
      }
    }

    "merge complex ands/ors" >> {
      "for strings" >> {
        val bounds = Bounds(Some("b"), Some("f"), inclusive = true)
        val or = Bounds.union(Seq(bounds), Seq(Bounds(Some("i"), Some("m"), inclusive = true)))
        or mustEqual Seq(
          Bounds(Some("b"), Some("f"), inclusive = true),
          Bounds(Some("i"), Some("m"), inclusive = true)
        )
        val and = or.flatMap(Bounds.intersection(_, Bounds(Some("e"), Some("k"), inclusive = true)))
        and mustEqual Seq(
          Bounds(Some("e"), Some("f"), inclusive = true),
          Bounds(Some("i"), Some("k"), inclusive = true)
        )
        val or2 = Bounds.union(and, Seq(Bounds(Some("f"), Some("i"), inclusive = true)))
        or2 mustEqual Seq(Bounds(Some("e"), Some("k"), inclusive = true))
      }
    }
  }
}
