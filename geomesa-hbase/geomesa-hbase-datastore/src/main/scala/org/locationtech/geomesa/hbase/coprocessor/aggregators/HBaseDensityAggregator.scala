/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.iterators.DensityScan.{DensityResult, DensityResultsToFeatures}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class HBaseDensityAggregator extends DensityScan with HBaseAggregator[DensityResult]

object HBaseDensityAggregator {

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

  class HBaseDensityResultsToFeatures extends DensityResultsToFeatures[Array[Byte]] {
    override protected def bytes(result: Array[Byte]): Array[Byte] = result
  }
}
