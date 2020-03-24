/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.aggregators

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResultsToFeatures
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object HBaseDensityAggregator {

  import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter.AggregatorPackage

  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    DensityScan.configure(sft, index, filter, hints) +
        (GeoMesaCoprocessor.AggregatorClass -> s"$AggregatorPackage.HBaseDensityAggregator")
  }

  class HBaseDensityResultsToFeatures extends DensityResultsToFeatures[Array[Byte]] {
    override protected def bytes(result: Array[Byte]): Array[Byte] = {
      val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
      sf.getUserData.put(DensityScan.DensityValueKey, result)
      println("Adding records to result")
      DensityScan.decodeResult(env, 500, 500)(sf).foreach { println }
      result
    }
  }

  val env = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
}
