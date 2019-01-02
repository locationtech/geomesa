/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResult
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class HBaseDensityAggregator extends DensityScan with HBaseAggregator[DensityResult]

object HBaseDensityAggregator {

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "")
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    sf.getUserData.put(DensityScan.DensityValueKey, bytes)
    sf
  }

  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    DensityScan.configure(sft, index, filter, hints) +
        (GeoMesaCoprocessor.AggregatorClass -> classOf[HBaseDensityAggregator].getName)
  }
}
