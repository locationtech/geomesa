/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
