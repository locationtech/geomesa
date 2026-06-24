/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.fs.storage.converter.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.zorder.sfcurve.IndexRange

case class Z2Scheme(bits: Int, geom: String, geomIndex: Int) extends SpatialScheme(bits, geom) {

  private val xyBits = bits / 2
  private val z2 = new Z2SFC(xyBits)
  private val xRadius = (360d / math.pow(2, xyBits)) / 2
  private val yRadius = (180d / math.pow(2, xyBits)) / 2

  override def pattern: String = s"$bits-bit-z2"

  override protected def digits(bits: Int): Int = math.ceil(bits * math.log10(2)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = z2.ranges(xy)
}

object Z2Scheme {

  val Name = "z2"

  object Z2PartitionSchemeFactory extends SpatialPartitionSchemeFactory(Name) {
    override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme =
      Z2Scheme(bits, geom, geomIndex)
  }
}
