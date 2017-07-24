/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.Bounds.Bound
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BoundsTest extends Specification {

  "Bounds" should {

    "merge different types" >> {
      "ints" >> {
        val leftLower: Bound[java.lang.Integer]  = Bound(Some(0), inclusive = true)
        val leftUpper: Bound[java.lang.Integer]  = Bound(Some(10), inclusive = true)
        val rightLower: Bound[java.lang.Integer] = Bound(Some(5), inclusive = true)
        val rightUpper: Bound[java.lang.Integer] = Bound(Some(15), inclusive = true)
        val left  = Bounds(leftLower, leftUpper)
        val right = Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "longs" >> {
        val leftLower: Bound[java.lang.Long]  = Bound(Some(0L), inclusive = true)
        val leftUpper: Bound[java.lang.Long]  = Bound(Some(10L), inclusive = true)
        val rightLower: Bound[java.lang.Long] = Bound(Some(5L), inclusive = true)
        val rightUpper: Bound[java.lang.Long] = Bound(Some(15L), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "floats" >> {
        val leftLower: Bound[java.lang.Float]  = Bound(Some(0f), inclusive = true)
        val leftUpper: Bound[java.lang.Float]  = Bound(Some(10f), inclusive = true)
        val rightLower: Bound[java.lang.Float] = Bound(Some(5f), inclusive = true)
        val rightUpper: Bound[java.lang.Float] = Bound(Some(15f), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "doubles" >> {
        val leftLower: Bound[java.lang.Double]  = Bound(Some(0d), inclusive = true)
        val leftUpper: Bound[java.lang.Double]  = Bound(Some(10d), inclusive = true)
        val rightLower: Bound[java.lang.Double] = Bound(Some(5d), inclusive = true)
        val rightUpper: Bound[java.lang.Double] = Bound(Some(15d), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "strings" >> {
        val leftLower: Bound[String]  = Bound(Some("0"), inclusive = true)
        val leftUpper: Bound[String]  = Bound(Some("6"), inclusive = true)
        val rightLower: Bound[String] = Bound(Some("3"), inclusive = true)
        val rightUpper: Bound[String] = Bound(Some("9"), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "dates" >> {
        val leftLower: Bound[Date]  = Bound(Some(new Date(0)), inclusive = true)
        val leftUpper: Bound[Date]  = Bound(Some(new Date(10)), inclusive = true)
        val rightLower: Bound[Date] = Bound(Some(new Date(5)), inclusive = true)
        val rightUpper: Bound[Date] = Bound(Some(new Date(15)), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
      "uuids" >> {
        val leftLower: Bound[UUID]  = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000000")), inclusive = true)
        val leftUpper: Bound[UUID]  = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000006")), inclusive = true)
        val rightLower: Bound[UUID] = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000003")), inclusive = true)
        val rightUpper: Bound[UUID] = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000009")), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
        Bounds.union(Seq(left), Seq(right)) mustEqual Seq(Bounds(leftLower, rightUpper))
      }
    }

    "handle exclusive/inclusive bounds" >> {
      val inclusive = Bound(Some(0), inclusive = true)
      val exclusive = Bound(Some(0), inclusive = false)

      Bounds.smallerLowerBound(inclusive, exclusive) mustEqual inclusive
      Bounds.smallerLowerBound(exclusive, inclusive) mustEqual inclusive
      Bounds.largerLowerBound(inclusive, exclusive) mustEqual exclusive
      Bounds.largerLowerBound(exclusive, inclusive) mustEqual exclusive
      Bounds.largerUpperBound(inclusive, exclusive) mustEqual inclusive
      Bounds.largerUpperBound(exclusive, inclusive) mustEqual inclusive
      Bounds.smallerUpperBound(inclusive, exclusive) mustEqual exclusive
      Bounds.smallerUpperBound(exclusive, inclusive) mustEqual exclusive

      Bounds.intersection(Bounds(inclusive, Bound.unbounded), Bounds(exclusive, Bound.unbounded)) must beSome(Bounds(exclusive, Bound.unbounded))
      Bounds.intersection(Bounds(exclusive, Bound.unbounded), Bounds(inclusive, Bound.unbounded)) must beSome(Bounds(exclusive, Bound.unbounded))
      Bounds.intersection(Bounds(Bound.unbounded, inclusive), Bounds(Bound.unbounded, exclusive)) must beSome(Bounds(Bound.unbounded, exclusive))
      Bounds.intersection(Bounds(Bound.unbounded, exclusive), Bounds(Bound.unbounded, inclusive)) must beSome(Bounds(Bound.unbounded, exclusive))

      Bounds.union(Seq(Bounds(inclusive, Bound.unbounded)), Seq(Bounds(exclusive, Bound.unbounded))) mustEqual Seq(Bounds(inclusive, Bound.unbounded))
      Bounds.union(Seq(Bounds(exclusive, Bound.unbounded)), Seq(Bounds(inclusive, Bound.unbounded))) mustEqual Seq(Bounds(inclusive, Bound.unbounded))
      Bounds.union(Seq(Bounds(Bound.unbounded, inclusive)), Seq(Bounds(Bound.unbounded, exclusive))) mustEqual Seq(Bounds(Bound.unbounded, inclusive))
      Bounds.union(Seq(Bounds(Bound.unbounded, exclusive)), Seq(Bounds(Bound.unbounded, inclusive))) mustEqual Seq(Bounds(Bound.unbounded, inclusive))
    }

    "merge simple ands/ors" >> {
      "for strings" >> {
        val bounds = Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true))
        "overlapping" >> {
          val toMerge = Bounds(Bound(Some("d"), inclusive = true), Bound(Some("i"), inclusive = true))
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Bound(Some("d"), inclusive = true), Bound(Some("f"), inclusive = true)))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Bound(Some("b"), inclusive = true), Bound(Some("i"), inclusive = true)))
        }
        "disjoint" >> {
          val toMerge = Bounds(Bound(Some("i"), inclusive = true), Bound(Some("z"), inclusive = true))
          Bounds.intersection(bounds, toMerge) must beNone
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(
            Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true)),
            Bounds(Bound(Some("i"), inclusive = true), Bound(Some("z"), inclusive = true))
          )
        }
        "contained" >> {
          val toMerge = Bounds(Bound(Some("c"), inclusive = true), Bound(Some("d"), inclusive = true))
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Bound(Some("c"), inclusive = true), Bound(Some("d"), inclusive = true)))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true)))
        }
        "containing" >> {
          val toMerge = Bounds(Bound(Some("a"), inclusive = true), Bound(Some("i"), inclusive = true))
          Bounds.intersection(bounds, toMerge) must beSome(Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true)))
          Bounds.union(Seq(bounds), Seq(toMerge)) mustEqual Seq(Bounds(Bound(Some("a"), inclusive = true), Bound(Some("i"), inclusive = true)))
        }
      }
    }

    "merge complex ands/ors" >> {
      "for strings" >> {
        val bounds = Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true))
        val or = Bounds.union(Seq(bounds), Seq(Bounds(Bound(Some("i"), inclusive = true), Bound(Some("m"), inclusive = true))))
        or mustEqual Seq(
          Bounds(Bound(Some("b"), inclusive = true), Bound(Some("f"), inclusive = true)),
          Bounds(Bound(Some("i"), inclusive = true), Bound(Some("m"), inclusive = true))
        )
        val and = or.flatMap(Bounds.intersection(_, Bounds(Bound(Some("e"), inclusive = true), Bound(Some("k"), inclusive = true))))
        and mustEqual Seq(
          Bounds(Bound(Some("e"), inclusive = true), Bound(Some("f"), inclusive = true)),
          Bounds(Bound(Some("i"), inclusive = true), Bound(Some("k"), inclusive = true))
        )
        val or2 = Bounds.union(and, Seq(Bounds(Bound(Some("f"), inclusive = true), Bound(Some("i"), inclusive = true))))
        or2 mustEqual Seq(Bounds(Bound(Some("e"), inclusive = true), Bound(Some("k"), inclusive = true)))
      }
    }
  }
}
