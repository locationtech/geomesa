/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
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
      }
      "longs" >> {
        val leftLower: Bound[java.lang.Long]  = Bound(Some(0L), inclusive = true)
        val leftUpper: Bound[java.lang.Long]  = Bound(Some(10L), inclusive = true)
        val rightLower: Bound[java.lang.Long] = Bound(Some(5L), inclusive = true)
        val rightUpper: Bound[java.lang.Long] = Bound(Some(15L), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
      }
      "floats" >> {
        val leftLower: Bound[java.lang.Float]  = Bound(Some(0f), inclusive = true)
        val leftUpper: Bound[java.lang.Float]  = Bound(Some(10f), inclusive = true)
        val rightLower: Bound[java.lang.Float] = Bound(Some(5f), inclusive = true)
        val rightUpper: Bound[java.lang.Float] = Bound(Some(15f), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
      }
      "doubles" >> {
        val leftLower: Bound[java.lang.Double]  = Bound(Some(0d), inclusive = true)
        val leftUpper: Bound[java.lang.Double]  = Bound(Some(10d), inclusive = true)
        val rightLower: Bound[java.lang.Double] = Bound(Some(5d), inclusive = true)
        val rightUpper: Bound[java.lang.Double] = Bound(Some(15d), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
      }
      "strings" >> {
        val leftLower: Bound[String]  = Bound(Some("0"), inclusive = true)
        val leftUpper: Bound[String]  = Bound(Some("6"), inclusive = true)
        val rightLower: Bound[String] = Bound(Some("3"), inclusive = true)
        val rightUpper: Bound[String] = Bound(Some("9"), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
      }
      "dates" >> {
        val leftLower: Bound[Date]  = Bound(Some(new Date(0)), inclusive = true)
        val leftUpper: Bound[Date]  = Bound(Some(new Date(10)), inclusive = true)
        val rightLower: Bound[Date] = Bound(Some(new Date(5)), inclusive = true)
        val rightUpper: Bound[Date] = Bound(Some(new Date(15)), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
      }
      "uuids" >> {
        val leftLower: Bound[UUID]  = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000000")), inclusive = true)
        val leftUpper: Bound[UUID]  = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000006")), inclusive = true)
        val rightLower: Bound[UUID] = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000003")), inclusive = true)
        val rightUpper: Bound[UUID] = Bound(Some(UUID.fromString("00000000-0000-0000-0000-000000000009")), inclusive = true)
        val left  =  Bounds(leftLower, leftUpper)
        val right =  Bounds(rightLower, rightUpper)
        Bounds.intersection(left, right) must beSome(Bounds(rightLower, leftUpper))
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
    }

    "cover" >> {
      val range = Bounds(
        Bound(Some(FastConverter.convert("2019-01-01T00:00:00.000Z", classOf[Date])), inclusive = true),
        Bound(Some(FastConverter.convert("2019-01-01T00:53:00.000Z", classOf[Date])), inclusive = false))

      range.covers(range) must beTrue

      foreach(Seq("2019-01-01T01:00:00.000Z", "2019-01-01T02:00:00.000Z")) { date =>
        val bounds = Bounds(
          Bound(Some(FastConverter.convert("2019-01-01T00:00:00.000Z", classOf[Date])), inclusive = true),
          Bound(Some(FastConverter.convert(date, classOf[Date])), inclusive = false))
        bounds.covers(range) must beTrue
        range.covers(bounds) must beFalse
      }

      foreach(Seq("2019-01-01T00:30:00.000Z", "2019-01-01T00:45:00.000Z")) { date =>
        val bounds = Bounds(
          Bound(Some(FastConverter.convert("2019-01-01T00:00:00.000Z", classOf[Date])), inclusive = true),
          Bound(Some(FastConverter.convert(date, classOf[Date])), inclusive = false))
        bounds.covers(range) must beFalse
        range.covers(bounds) must beTrue
      }
    }

    "intersect" >> {
      val range = Bounds[Integer](Bound(Some(0), inclusive = true), Bound(Some(10), inclusive = true))

      foreach(Seq(5 -> 15, 0 -> 10, 10 -> 20)) { case (lo, hi) =>
        val bound = Bounds[Integer](Bound(Some(lo), inclusive = true), Bound(Some(hi), inclusive = true))
        range.intersects(bound) must beTrue
        bound.intersects(range) must beTrue
      }

      foreach(Seq(-10 -> -1, 15 -> 25)) { case (lo, hi) =>
        val bound = Bounds[Integer](Bound(Some(lo), inclusive = true), Bound(Some(hi), inclusive = true))
        range.intersects(bound) must beFalse
        bound.intersects(range) must beFalse
      }

      foreach(Seq(-10 -> 0, 10 -> 20)) { case (lo, hi) =>
        val bound = Bounds[Integer](Bound(Some(lo), inclusive = false), Bound(Some(hi), inclusive = false))
        range.intersects(bound) must beFalse
        bound.intersects(range) must beFalse
      }
    }
  }
}
