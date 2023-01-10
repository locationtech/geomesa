/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.zorder

package object sfcurve {

  case class ZPrefix(prefix: Long, precision: Int) // precision in bits

  /**
   * Represents a rectangle in defined by min and max as two opposing points
   *
   * @param min: lower-left point
   * @param max: upper-right point
   */
  case class ZRange(min: Long, max: Long) {

    require(min <= max, s"Range bounds must be ordered, but $min > $max")

    def mid: Long = (max + min)  >>> 1 // overflow safe mean

    def length: Long = max - min + 1

    // contains in index space (e.g. the long value)
    def contains(bits: Long): Boolean = bits >= min && bits <= max

    // contains in index space (e.g. the long value)
    def contains(r: ZRange): Boolean =  contains(r.min) && contains(r.max)

    // overlaps in index space (e.g. the long value)
    def overlaps(r: ZRange): Boolean = contains(r.min) || contains(r.max)
  }

  object ZRange {
    def apply(min: Z2, max: Z2)(implicit d: DummyImplicit): ZRange = ZRange(min.z, max.z)
    def apply(min: Z3, max: Z3)(implicit d1: DummyImplicit, d2: DummyImplicit): ZRange = ZRange(min.z, max.z)
  }

  sealed trait IndexRange {
    def lower: Long
    def upper: Long
    def contained: Boolean
    def tuple: (Long, Long, Boolean) = (lower, upper, contained)
  }

  case class CoveredRange(lower: Long, upper: Long) extends IndexRange {
    val contained = true
  }

  case class OverlappingRange(lower: Long, upper: Long) extends IndexRange {
    val contained = false
  }

  object IndexRange {
    trait IndexRangeOrdering extends Ordering[IndexRange] {
      override def compare(x: IndexRange, y: IndexRange): Int = {
        val c1 = x.lower.compareTo(y.lower)
        if(c1 != 0) return c1
        val c2 = x.upper.compareTo(y.upper)
        if(c2 != 0) return c2
        0
      }
    }

    implicit object IndexRangeIsOrdered extends IndexRangeOrdering

    def apply(l: Long, u: Long, contained: Boolean): IndexRange =
      if(contained) CoveredRange(l, u)
      else          OverlappingRange(l, u)
  }
}
