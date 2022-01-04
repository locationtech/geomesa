/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.jts.geom.Geometry
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

case class XZ2Scheme(bits: Int, geom: String, geomIndex: Int) extends SpatialScheme(bits, geom) {

  private val xz2 = XZ2SFC((bits / 2).asInstanceOf[Short])

  override def pattern: String = s"$bits-bit-xz2"

  override def getPartitionName(feature: SimpleFeature): String = {
    val geometry = feature.getAttribute(geom).asInstanceOf[Geometry]
    val envelope = geometry.getEnvelopeInternal
    xz2.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY).formatted(format)
  }

  // TODO https://geomesa.atlassian.net/browse/GEOMESA-2967
  override def getCoveringFilter(partition: String): Filter =
    throw new NotImplementedError("https://geomesa.atlassian.net/browse/GEOMESA-2967")

  // the max XZ2 value is (4^((bits / 2) + 1) - 1) / 3
  // this calculates the number of digits in that value
  override protected def digits(bits: Int): Int = math.ceil(((bits / 2) + 1) * math.log10(4) - math.log10(3)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = xz2.ranges(xy)
}

object XZ2Scheme {

  val Name = "xz2"

  class XZ2PartitionSchemeFactory extends SpatialPartitionSchemeFactory(Name) {
    override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme =
      XZ2Scheme(bits, geom, geomIndex)
  }
}
