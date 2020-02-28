/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import java.nio.charset.StandardCharsets

import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseVersionAggregator.VersionAggregator
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.locationtech.geomesa.index.iterators.AggregatingScan.AggregateCallback
import org.locationtech.geomesa.utils.conf.GeoMesaProperties
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class HBaseVersionAggregator extends HBaseAggregator[VersionAggregator] {

  private var scanned = false

  override def setScanner(scanner: RegionScanner): Unit = {}

  override def init(options: Map[String, String]): Unit = {}

  override def aggregate[A <: AggregateCallback](callback: A): A = {
    if (!scanned) {
      scanned = true
      callback.batch(GeoMesaProperties.ProjectVersion.getBytes(StandardCharsets.UTF_8))
    }
    callback
  }

  override protected def initResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      batchSize: Int,
      options: Map[String, String]): VersionAggregator = throw new NotImplementedError()

  override protected def defaultBatchSize: Int = throw new NotImplementedError()

  override protected def aggregateResult(sf: SimpleFeature, result: VersionAggregator): Int =
    throw new NotImplementedError()

  override protected def encodeResult(result: VersionAggregator): Array[Byte] = throw new NotImplementedError()

  override protected def closeResult(result: VersionAggregator): Unit = {}
}

object HBaseVersionAggregator {

  def configure(sft: SimpleFeatureType, index: GeoMesaFeatureIndex[_, _]): Map[String, String] = {
    AggregatingScan.configure(sft, index, None, None, None, 1) +
        (GeoMesaCoprocessor.AggregatorClass -> classOf[HBaseVersionAggregator].getName)
  }

  class VersionAggregator {
    def isEmpty: Boolean = false
    def clear(): Unit = {}
  }
}
