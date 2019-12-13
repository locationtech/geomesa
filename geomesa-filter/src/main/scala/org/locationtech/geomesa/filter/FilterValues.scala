/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import scala.collection.GenTraversableOnce

/**
  * Holds values extracted from a filter. Values may be empty, in which case nothing was extracted from
  * the filter. May be marked as 'disjoint', which means that mutually exclusive values were extracted
  * from the filter. This may be checked to short-circuit queries that will not result in any hits.
  *
  * @param values values extracted from the filter. If nothing was extracted, will be empty
  * @param precise values exactly match the filter, or may return false positives
  * @param disjoint mutually exclusive values were extracted, e.g. 'a < 1 && a > 2'
  * @tparam T type parameter
  */
case class FilterValues[+T](values: Seq[T], precise: Boolean = true, disjoint: Boolean = false) {
  def map[U](f: T => U): FilterValues[U] = FilterValues(values.map(f), precise, disjoint)
  def flatMap[U](f: T => GenTraversableOnce[U]): FilterValues[U] = FilterValues(values.flatMap(f), precise, disjoint)
  def foreach[U](f: T => U): Unit = values.foreach(f)
  def forall(p: T => Boolean): Boolean = values.forall(p)
  def exists(p: T => Boolean): Boolean = values.exists(p)
  def filter(f: T => Boolean): FilterValues[T] = FilterValues(values.filter(f), precise, disjoint)
  def nonEmpty: Boolean = values.nonEmpty || disjoint
  def isEmpty: Boolean = !nonEmpty
}

object FilterValues {

  def empty[T]: FilterValues[T] = FilterValues[T](Seq.empty)

  def disjoint[T]: FilterValues[T] = FilterValues[T](Seq.empty, disjoint = true)

  def or[T](join: (Seq[T], Seq[T]) => Seq[T])(left: FilterValues[T], right: FilterValues[T]): FilterValues[T] = {
    (left.disjoint, right.disjoint) match {
      case (false, false) => FilterValues(join(left.values, right.values), left.precise && right.precise)
      case (false, true)  => left
      case (true,  false) => right
      case (true,  true)  => FilterValues.disjoint
    }
  }

  def and[T](intersect: (T, T) => Option[T])(left: FilterValues[T], right: FilterValues[T]): FilterValues[T] = {
    if (left.disjoint || right.disjoint) {
      FilterValues.disjoint
    } else {
      val intersections = left.values.flatMap(v => right.values.flatMap(intersect(_, v)))
      if (intersections.isEmpty) {
        FilterValues.disjoint
      } else {
        FilterValues(intersections, left.precise && right.precise)
      }
    }
  }
}
