/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

object TieredOrdering {
  // noinspection LanguageFeature
  implicit def toTieredOrdering[T](orderings: Seq[Ordering[T]]): Ordering[T] = TieredOrdering(orderings)
}

case class TieredOrdering[T](orderings: Seq[Ordering[T]]) extends Ordering[T] {
  override def compare(x: T, y: T): Int = {
    orderings.foreach { o =>
      val i = o.compare(x, y)
      if (i != 0) {
        return i
      }
    }
    0
  }
}
