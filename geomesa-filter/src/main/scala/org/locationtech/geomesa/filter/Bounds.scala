/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

/**
  * Single typed bound. If filter is unbounded on one or both sides, the associated bound will be None.
  *
  * For example, bounds for 'foo < 5' would be (None, Some(5))
  * Special case for 'foo NOT NULL' will have both bounds be None
  *
  * @param lower lower bound, if any
  * @param upper upper bound, if any
  * @param inclusive whether the bounds are inclusive or exclusive.
  *                  for example, 'foo < 5' is exclusive, 'foo <= 5' is inclusive
  * @tparam T binding of the attribute type
  */
case class Bounds[T](lower: Option[T], upper: Option[T], inclusive: Boolean) {
  def bounds: (Option[T], Option[T]) = (lower, upper)
}

object Bounds {

  /**
    * Takes the intersection of two bounds. If they are disjoint, will return None.
    *
    * @param left first bounds
    * @param right second bounds
    * @tparam T type parameter
    * @return intersection
    */
  def intersection[T](left: Bounds[T], right: Bounds[T]): Option[Bounds[T]] = {
    val lower = left.lower match {
      case None => right.lower
      case Some(lo) => right.lower.filter(_.compareTo(lo) >= 0).orElse(left.lower)
    }
    val upper = left.upper match {
      case None => right.upper
      case Some(up) => right.upper.filter(_.compareTo(up) <= 0).orElse(left.upper)
    }
    (lower, upper) match {
      case (Some(lo), Some(up)) if up.compareTo(lo) < 0 => None
      case _ => Some(Bounds(lower, upper, inclusive(lower, upper, left, right)))
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

  // noinspection LanguageFeature
  private implicit def toComparable[T](t: T): Comparable[T] = t.asInstanceOf[Comparable[T]]

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
    merged.sortBy(_.bounds)
  }

  /**
    * Determines if the two bounds overlap each other
    *
    * @param left first bounds
    * @param right second bounds
    * @tparam T type class
    * @return true if bounds overlap (or abut)
    */
  private def overlaps[T](left: Bounds[T], right: Bounds[T]): Boolean = {
    left.bounds match {
      case (None, None) => true
      case (None, Some(lUp)) => right.lower.forall(rLo => lUp.compareTo(rLo) >= 0)
      case (Some(lLo), None) => right.upper.forall(rUp => lLo.compareTo(rUp) <= 0)
      case (Some(lLo), Some(lUp)) =>
        right.lower.forall(rLo => lUp.compareTo(rLo) >= 0) && right.upper.forall(rUp => lLo.compareTo(rUp) <= 0)
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
  private def mergeOverlapping[T](left: Bounds[T], right: Bounds[T]): Bounds[T] = {
    val lower = (left.lower, right.lower) match {
      case (Some(loLeft), Some(loRight)) =>
        if (loLeft.compareTo(loRight) > 0) Some(loRight) else Some(loLeft)
      case _ => None
    }
    val upper = (left.upper, right.upper) match {
      case (Some(upLeft), Some(upRight)) =>
        if (upLeft.compareTo(upRight) < 0) Some(upRight) else Some(upLeft)
      case _ => None
    }
    Bounds(lower, upper, inclusive(lower, upper, left, right))
  }

  /**
    * Determines if an endpoint should be inclusive or exclusive
    *
    * @param lower lower endpoint of new bound
    * @param upper upper endpoint of new bound
    * @param left first bound that endpoints came from
    * @param right second bounds that endpoints came from
    * @tparam T type parameter
    * @return inclusivity of resulting bounds
    */
  private def inclusive[T](lower: Option[T], upper: Option[T], left: Bounds[T], right: Bounds[T]): Boolean = {
    if (left.inclusive == right.inclusive) {
      left.inclusive
    } else if (lower.isEmpty) {
      if (upper == left.upper) left.inclusive else right.inclusive
    } else if (upper.isEmpty) {
      if (lower == left.lower) left.inclusive else right.inclusive
    } else if (lower == left.lower && upper == left.upper) {
      left.inclusive
    } else if (lower == right.lower && upper == right.upper) {
      right.inclusive
    } else {
      true // in this case one endpoint is inclusive and the other isn't... just make it inclusive
    }
  }
}