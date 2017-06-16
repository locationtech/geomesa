/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.hbase.HBaseFeatureIndexType
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.iterators.{BinAggregatingScan, ByteBufferResult}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class HBaseBinAggregator extends BinAggregatingScan with HBaseAggregator[ByteBufferResult]

object HBaseBinAggregator {
  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    sf.setAttribute(BIN_ATTRIBUTE_INDEX, bytes)
    sf
  }

  def configure(sft: SimpleFeatureType,
                index: HBaseFeatureIndexType,
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    BinAggregatingScan.configure(sft,
      index,
      filter,
      hints.getBinTrackIdField,
      hints.getBinGeomField.getOrElse(sft.getGeomField),
      hints.getBinDtgField,
      hints.getBinLabelField,
      hints.getBinBatchSize,
      hints.isBinSorting,
      hints) ++
      Map(GeoMesaCoprocessor.AggregatorClass -> classOf[HBaseBinAggregator].getName)
  }
}