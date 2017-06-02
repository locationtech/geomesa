/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Map => jMap}

import com.google.common.primitives.Longs
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.legacy.z2.Z2IndexV1
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils.DensityResult
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.sfcurve.zorder.Z2
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Density iterator that weights hits based on z2 schema
 */
class Z2DensityIterator extends KryoLazyDensityIterator {

  // TODO GEOMESA-1164 shares a lot of code with Z3DensityIter

  import Z2DensityIterator.TableSharingKey

  var normalizeWeight: (Double) => Double = null
  val zBytes = Array.fill[Byte](8)(0)
  var zPrefix: Int = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)
    if (sft.isPoints) {
      normalizeWeight = (weight) => weight
    } else {
      // normalize the weight based on how many representations of the geometry are in our index
      // this is stored in the column qualifier
      normalizeWeight = (weight) => {
        val hexCount = topKey.getColumnQualifier.toString
        val hexSeparator = hexCount.indexOf(",")
        if (hexSeparator == -1) {
          weight
        } else {
          weight / Integer.parseInt(hexCount.substring(0, hexSeparator), 16)
        }
      }
    }

    // 1 for split plus optional 1 for table sharing
    zPrefix = if (jOptions.get(TableSharingKey).toBoolean) 2 else 1
  }

  /**
   * We write the geometry at the center of the zbox that this row represents
   */
  override def writeNonPoint(geom: Geometry, weight: Double, result: DensityResult): Unit = geom match {
    case p: Point => writePointToResult(p, weight, result)
    case _ =>
      val row = topKey.getRowData
      val zOffset = row.offset() + zPrefix
      var i = 0
      while (i < Z2IndexV1.GEOM_Z_NUM_BYTES) {
        zBytes(i) = row.byteAt(zOffset + i)
        i += 1
      }
      val (x, y) = Z2SFC.invert(Z2(Longs.fromByteArray(zBytes)))
      val nWeight = normalizeWeight(weight)
      writePointToResult(x, y, nWeight, result)
  }
}

object Z2DensityIterator {

  val TableSharingKey = "ts"

  /**
   * Creates an iterator config for the z2 density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndex,
                filter: Option[Filter],
                hints: Hints,
                priority: Int = KryoLazyDensityIterator.DEFAULT_PRIORITY): IteratorSetting = {
    val is = KryoLazyDensityIterator.configure(sft, index, filter, hints, deduplicate = false, priority)
    is.setIteratorClass(classOf[Z2DensityIterator].getName)
    is.addOption(TableSharingKey, sft.isTableSharing.toString)
    is
  }
}