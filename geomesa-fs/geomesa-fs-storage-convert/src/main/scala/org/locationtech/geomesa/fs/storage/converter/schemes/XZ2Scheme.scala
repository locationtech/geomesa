/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.fs.storage.converter.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.zorder.sfcurve.IndexRange

case class XZ2Scheme(bits: Int, geom: String, geomIndex: Int) extends SpatialScheme(bits, geom) {

  private val xz2 = XZ2SFC((bits / 2).asInstanceOf[Short])

  override def pattern: String = s"$bits-bit-xz2"

  // the max XZ2 value is (4^((bits / 2) + 1) - 1) / 3
  // this calculates the number of digits in that value
  override protected def digits(bits: Int): Int = math.ceil(((bits / 2) + 1) * math.log10(4) - math.log10(3)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = xz2.ranges(xy)
}

object XZ2Scheme {

  val Name = "xz2"

  object XZ2PartitionSchemeFactory extends SpatialPartitionSchemeFactory(Name) {
    override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme =
      XZ2Scheme(bits, geom, geomIndex)
  }
}
