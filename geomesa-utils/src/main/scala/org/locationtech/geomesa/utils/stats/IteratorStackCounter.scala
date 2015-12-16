/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

/**
 * The IteratorStackCounter keeps track of the number of Accumulo iterators
 * that are instantiated as a result of a query
 *
 * @param count number of iterators
 */
class IteratorStackCounter(var count: Long = 1) extends Stat {
  override def observe(sf: SimpleFeature): Unit = { }

  override def add(other: Stat): Stat = {
    other match {
      case o: IteratorStackCounter => count += o.count
    }
    this
  }

  override def toJson(): String = s"""{ "count": $count }"""

  override def isEmpty(): Boolean = count == 1L

  override def clear(): Unit = count = 1L

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[IteratorStackCounter] && {
      val isc = obj.asInstanceOf[IteratorStackCounter]
      count == isc.count
    }
  }
}
