/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Map => jMap}

import com.google.common.primitives.Longs
import com.vividsolutions.jts.geom.{Point, Geometry}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.iterators.KryoLazyDensityIterator.DensityResult
import org.locationtech.geomesa.curve.{Z3, Z3SFC}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable

/**
 * Density iterator that weights hits based on z3 schema
 */
class Z3DensityIterator extends KryoLazyDensityIterator {

  var normalizeWeight: (Double) => Double = null
  val zBytes = Array.fill[Byte](8)(0)

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
        if (hexCount.isEmpty) {
          weight
        } else {
          weight / Integer.parseInt(hexCount, 16)
        }
      }
    }
  }

  /**
   * We write the geometry at the center of the zbox that this row represents
   */
  override def writeNonPoint(geom: Geometry, weight: Double, result: DensityResult): Unit = geom match {
    case p: Point => writePointToResult(p, weight, result)
    case _ =>
      val row = topKey.getRowData
      val zOffset = row.offset() + 2
      var i = 0
      while (i < Z3Table.GEOM_Z_NUM_BYTES) {
        zBytes(i) = row.byteAt(zOffset + i)
        i += 1
      }
      val (x, y, _) = Z3SFC.invert(Z3(Longs.fromByteArray(zBytes)))
      val nWeight = normalizeWeight(weight)
      writePointToResult(x, y, nWeight, result)
  }
}

object Z3DensityIterator {

    /**
     * Creates an iterator config for the z3 density iterator
     */
    def configure(sft: SimpleFeatureType,
                  filter: Option[Filter],
                  hints: Hints,
                  priority: Int = KryoLazyDensityIterator.DEFAULT_PRIORITY): IteratorSetting = {
      val is = KryoLazyDensityIterator.configure(sft, filter, hints, priority)
      is.setIteratorClass(classOf[Z3DensityIterator].getName)
      is
    }
}