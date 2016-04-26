/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.sfcurve.IndexRange

trait SpaceFillingCurve[T] {
  def lat: NormalizedDimension
  def lon: NormalizedDimension
  def index(x: Double, y: Double): T
  def invert(i: T): (Double, Double)
  def ranges(x: (Double, Double), y: (Double, Double), precision: Int = 64): Seq[IndexRange]
}

trait SpaceTimeFillingCurve[T] {
  def lat: NormalizedDimension
  def lon: NormalizedDimension
  def time: NormalizedDimension
  def index(x: Double, y: Double, t: Long): T
  def invert(i: T): (Double, Double, Long)
  def ranges(x: (Double, Double), y: (Double, Double), t: (Long, Long), precision: Int = 64): Seq[IndexRange]
}