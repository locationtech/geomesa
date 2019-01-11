/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.legacy.z3.Z3IndexV2
import org.locationtech.geomesa.accumulo.iterators.Z2DensityIterator.TableSharingKey
import org.locationtech.geomesa.curve.LegacyZ3SFC
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResult
import org.locationtech.sfcurve.zorder.Z3
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Density iterator that weights hits based on z3 schema
 */
class Z3DensityIterator extends KryoLazyDensityIterator {

  private val zBytes = Array.fill[Byte](8)(0)

  override protected def initResult(sft: SimpleFeatureType,
                                    transform: Option[SimpleFeatureType],
                                    options: Map[String, String]): DensityResult = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val result = super.initResult(sft, transform, options)
    if (sft.nonPoints) {
      // normalize the weight based on how many representations of the geometry are in our index
      // this is stored in the column qualifier
      val normalizeWeight: (Double) => Double = (weight) => {
        val hexCount = topKey.getColumnQualifier.toString
        val hexSeparator = hexCount.indexOf(",")
        if (hexSeparator == -1) {
          weight
        } else {
          weight / Integer.parseInt(hexCount.substring(0, hexSeparator), 16)
        }
      }
      val baseWeight = getWeight
      getWeight = (sf) => normalizeWeight(baseWeight(sf))

      val sfc = LegacyZ3SFC(sft.getZ3Interval)
      // 1 for split plus optional 1 for table sharing
      val zPrefix = if (options(TableSharingKey).toBoolean) { 2 } else { 1 }
      writeGeom = (_, weight, result) => {
        val row = topKey.getRowData
        val zOffset = row.offset() + 3 // two for week and 1 for split
        var k = 0
        while (k < Z3IndexV2.GEOM_Z_NUM_BYTES) {
          zBytes(k) = row.byteAt(zOffset + k)
          k += 1
        }
        val (x, y, _) = sfc.invert(Z3(Longs.fromByteArray(zBytes)))
        val i = gridSnap.i(x)
        val j = gridSnap.j(y)
        if (i != -1 && j != -1) {
          result(i, j) += weight
        }
      }
    }

    result
  }
}

object Z3DensityIterator {

    /**
     * Creates an iterator config for the z3 density iterator
     */
    def configure(sft: SimpleFeatureType,
                  index: AccumuloFeatureIndex,
                  filter: Option[Filter],
                  hints: Hints,
                  priority: Int = KryoLazyDensityIterator.DEFAULT_PRIORITY): IteratorSetting = {
      val is = KryoLazyDensityIterator.configure(sft, index, filter, hints, deduplicate = false, priority)
      is.setIteratorClass(classOf[Z3DensityIterator].getName)
      is
    }
}