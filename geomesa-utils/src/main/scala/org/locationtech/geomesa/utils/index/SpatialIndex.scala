/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope

/**
 * Trait for indexing and querying spatial data
 */
trait SpatialIndex[T] {

  def insert(envelope: Envelope, item: T): Unit

  def remove(envelope: Envelope, item: T): Boolean

  def query(envelope: Envelope): Iterator[T]

  def query(envelope: Envelope, filter: (T) => Boolean): Iterator[T] = query(envelope).filter(filter)
}

object SpatialIndex {
  def getCenter(envelope: Envelope): (Double, Double) = {
    val x = (envelope.getMinX + envelope.getMaxX) / 2.0
    val y = (envelope.getMinY + envelope.getMaxY) / 2.0
    (x, y)
  }
}