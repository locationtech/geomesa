/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.jts.geom.Point
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeature

class Z2Scheme(bits: Int, geom: String, leaf: Boolean) extends SpatialScheme(bits, geom, leaf) {

  private val z2 = new Z2SFC(bits / 2)

  override def getName: String = Z2Scheme.Name

  override def getPartition(feature: SimpleFeature): String = {
    val pt = feature.getAttribute(geom).asInstanceOf[Point]
    z2.index(pt.getX, pt.getY).z.formatted(format)
  }

  override protected def digits(bits: Int): Int = math.ceil(bits * math.log10(2)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = z2.ranges(xy)
}

object Z2Scheme {

  val Name = "z2"

  class Z2PartitionSchemeFactory extends SpatialPartitionSchemeFactory {
    override val Name: String = Z2Scheme.Name
    override def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme =
      new Z2Scheme(bits, geom, leaf)
  }
}
