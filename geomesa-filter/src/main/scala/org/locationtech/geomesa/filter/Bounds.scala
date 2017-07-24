/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.locationtech.geomesa.filter.Bounds.Bound

/**
  * Single typed bound. If filter is unbounded on one or both sides, the associated bound will be None.
  *
  * For example, bounds for 'foo < 5' would be (None, Some(5))
  * Special case for 'foo NOT NULL' will have both bounds be None
  *
  * @param lower lower bound, if any
  * @param upper upper bound, if any
  * @tparam T binding of the attribute type
  */
case class Bounds[T](lower: Bound[T], upper: Bound[T]) {
  def bounds: (Option[T], Option[T]) = (lower.value, upper.value)
  def unbounded: Boolean = lower.value.isEmpty || upper.value.isEmpty
  def everything: Boolean = lower.value.isEmpty && upper.value.isEmpty
  override def toString: String = {
    (if (lower.inclusive) { "[" } else { "(" }) + lower.value.getOrElse("-\u221E") + "," +
      upper.value.getOrElse("+\u221E") + (if (upper.inclusive) { "]" } else { ")" })
  }
}

object Bounds {

  /**
    * Single bound (lower or upper).
    *
    * Bound may be unbounded, in which case value is None. Note by convention unbounded bounds are exclusive
    *
    * @param value value of this bound, if bounded
    * @param inclusive whether the bound is inclusive or exclusive.
    *                  for example, 'foo < 5' is exclusive, 'foo <= 5' is inclusive
    */
  case class Bound[T](value: Option[T], inclusive: Boolean) {
    def exclusive: Boolean = !inclusive
  }

  object Bound {
    private val unboundedBound = Bound[Any](None, inclusive = false)
    def unbounded[T]: Bound[T] = unboundedBound.asInstanceOf[Bound[T]]
  }

  private val mergeOrdering: Ordering[Bounds[Any]] = {
    val inner = Ordering.Option(new Ordering[Any] {
      override def compare(x: Any, y: Any): Int = x.asInstanceOf[Comparable[Any]].compareTo(y)
    })
    val outer = new Ordering[Bound[Any]] {
      override def compare(x: Bound[Any], y: Bound[Any]): Int = inner.compare(x.value, y.value)
    }
    val tuple = Ordering.Tuple2(outer, outer)
    new Ordering[Bounds[Any]] {
      override def compare(x: Bounds[Any], y: Bounds[Any]): Int = tuple.compare((x.lower, x.upper), (y.lower, y.upper))
    }
  }

  private val allValues = Bounds(Bound.unbounded, Bound.unbounded)

  def everything[T]: Bounds[T] = allValues.asInstanceOf[Bounds[T]]

  /**
    * Gets the smaller value between two lower bounds, taking into account exclusivity.
    * If the bounds are equal, the first bound will always be returned
    *
    * @param bound1 first bound
    * @param bound2 second bound
    * @return smaller bound
    */
  def smallerLowerBound[T](bound1: Bound[T], bound2: Bound[T]): Bound[T] = {
    if (bound1.value.isEmpty) {
      bound1
    } else if (bound2.value.isEmpty) {
      bound2
    } else {
      val c = bound1.value.get.asInstanceOf[Comparable[Any]].compareTo(bound2.value.get)
      if (c < 0 || (c == 0 && (bound1.inclusive || bound2.exclusive))) { bound1 } else { bound2 }
    }
  }

  /**
    * Gets the larger value between two upper bounds, taking into account exclusivity.
    * If the bounds are equal, the first bound will always be returned
    *
    * @param bound1 first bound
    * @param bound2 second bound
    * @return larger bound
    */
  def largerUpperBound[T](bound1: Bound[T], bound2: Bound[T]): Bound[T] = {
    if (bound1.value.isEmpty) {
      bound1
    } else if (bound2.value.isEmpty) {
      bound2
    } else {
      val c = bound1.value.get.asInstanceOf[Comparable[Any]].compareTo(bound2.value.get)
      if (c > 0 || (c == 0 && (bound1.inclusive || bound2.exclusive))) { bound1 } else { bound2 }
    }
  }

  /**
    * Gets the smaller value between two upper bounds, taking into account exclusivity.
    * If the bounds are equal, the first bound will always be returned
    *
    * @param bound1 first bound
    * @param bound2 second bound
    * @return smaller bound
    */
  def smallerUpperBound[T](bound1: Bound[T], bound2: Bound[T]): Bound[T] = {
    if (bound2.value.isEmpty) {
      bound1
    } else if (bound1.value.isEmpty) {
      bound2
    } else {
      val c = bound1.value.get.asInstanceOf[Comparable[Any]].compareTo(bound2.value.get)
      if (c < 0 || (c == 0 && (bound2.inclusive || bound1.exclusive))) { bound1 } else { bound2 }
    }
  }

  /**
    * Gets the larger value between two upper bounds, taking into account exclusivity.
    * If the bounds are equal, the first bound will always be returned
    *
    * @param bound1 first bound
    * @param bound2 second bound
    * @return larger bound
    */
  def largerLowerBound[T](bound1: Bound[T], bound2: Bound[T]): Bound[T] = {
    if (bound2.value.isEmpty) {
      bound1
    } else if (bound1.value.isEmpty) {
      bound2
    } else {
      val c = bound1.value.get.asInstanceOf[Comparable[Any]].compareTo(bound2.value.get)
      if (c > 0 || (c == 0 && (bound2.inclusive || bound1.exclusive))) { bound1 } else { bound2 }
    }
  }

  /**
    * Takes the intersection of two bounds. If they are disjoint, will return None.
    *
    * @param left first bounds
    * @param right second bounds
    * @tparam T type parameter
    * @return intersection
    */
  def intersection[T](left: Bounds[T], right: Bounds[T]): Option[Bounds[T]] = {
    val lower = largerLowerBound(left.lower, right.lower)
    val upper = smallerUpperBound(right.upper, left.upper)
    (lower.value, upper.value) match {
      case (Some(lo), Some(up)) if lo.asInstanceOf[Comparable[Any]].compareTo(up) > 0 => None
      case _ => Some(Bounds(lower, upper))
    }
  }

  /**
    * Takes the union of two bounds
    *
    * @param left first bounds
    * @param right second bounds
    * @tparam T type parameter
    * @return union
    */
  def union[T](left: Seq[Bounds[T]], right: Seq[Bounds[T]]): Seq[Bounds[T]] = {
    var updated = left
    right.foreach(b => updated = or(updated, b))
    updated
  }

  /**
    * Takes the 'and' of a new bound with a sequence of existing bounds
    *
    * @param bounds bounds
    * @param and bound to 'and'
    * @tparam T type parameter
    * @return result of 'and' - may be empty if bounds are disjoint
    */
  private def and[T](bounds: Seq[Bounds[T]], and: Bounds[T]): Seq[Bounds[T]] =
    bounds.flatMap(intersection(and, _))

  /**
    * Takes the 'or' of a new bound with a sequence of existing bounds
    *
    * @param bounds bounds
    * @param or bound to 'or'
    * @tparam T type parameter
    * @return result of 'or', sorted by endpoints
    */
  private def or[T](bounds: Seq[Bounds[T]], or: Bounds[T]): Seq[Bounds[T]] = {
    val merged = (Seq(or) ++ bounds).foldLeft(List.empty[Bounds[T]]) { case (list, bound) =>
      val (overlapped, disjoint) = list.partition(overlaps(bound, _))
      disjoint ++ List(overlapped.foldLeft(bound)(mergeOverlapping))
    }
    merged.sorted(mergeOrdering.asInstanceOf[Ordering[Bounds[T]]])
  }

  /**
    * Determines if the two bounds overlap each other
    *
    * @param bounds1 first bounds
    * @param bounds2 second bounds
    * @tparam T type class
    * @return true if bounds overlap (or abut)
    */
  private def overlaps[T](bounds1: Bounds[T], bounds2: Bounds[T]): Boolean = {
    def overlaps(left: T, leftInclusive: Boolean, right: T, rightInclusive: Boolean): Boolean = {
      val c = left.asInstanceOf[Comparable[Any]].compareTo(right)
      c > 0 || (c == 0 && (leftInclusive || rightInclusive))
    }
    (bounds1.lower.value, bounds1.upper.value) match {
      case (None, None) => true
      case (None, Some(b1Up)) => bounds2.lower.value.forall(overlaps(b1Up, bounds1.upper.inclusive, _, bounds2.lower.inclusive))
      case (Some(b1Lo), None) => bounds2.upper.value.forall(overlaps(_, bounds1.lower.inclusive, b1Lo, bounds2.upper.inclusive))
      case (Some(b1Lo), Some(b1Up)) =>
        bounds2.lower.value.forall(overlaps(b1Up, bounds1.upper.inclusive, _, bounds2.lower.inclusive)) &&
            bounds2.upper.value.forall(overlaps(_, bounds2.upper.inclusive, b1Lo, bounds1.lower.inclusive))
    }
  }

  /**
    * Merges two bounds that overlap each other
    *
    * @param left first bound
    * @param right second bound
    * @tparam T type parameter
    * @return merged bounds
    */
  private def mergeOverlapping[T](left: Bounds[T], right: Bounds[T]): Bounds[T] =
    Bounds(smallerLowerBound(left.lower, right.lower), largerUpperBound(left.upper, right.upper))
}