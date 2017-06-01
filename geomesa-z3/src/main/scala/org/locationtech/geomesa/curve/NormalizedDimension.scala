/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

trait NormalizedDimension {
  def min: Double
  def max: Double
  def precision: Long

  def normalize(x: Double): Int = math.ceil((x - min) / (max - min) * precision).toInt

  def denormalize(x: Int): Double = if (x == 0) { min } else { (x - 0.5d) * (max - min) / precision + min }
}

case class NormalizedLat(precision: Long) extends NormalizedDimension {
  override val min = -90.0
  override val max = 90.0
}

case class NormalizedLon(precision: Long) extends NormalizedDimension {
  override val min = -180.0
  override val max = 180.0
}

case class NormalizedTime(precision: Long, max: Double) extends NormalizedDimension {
  override val min = 0.0
}
