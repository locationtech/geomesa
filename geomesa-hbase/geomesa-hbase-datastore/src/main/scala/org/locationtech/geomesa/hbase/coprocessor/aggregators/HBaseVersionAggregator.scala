/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.conf.GeoMesaProperties
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class HBaseVersionAggregator extends HBaseAggregator[VersionAggregator] {

  private var scanned = false

  override def setScanner(scanner: RegionScanner): Unit = {}

  // we always return true exactly once so that we trigger aggregation, but then don't actually read any rows
  override def hasNextData: Boolean = if (scanned) { false } else { scanned = true; true }

  override protected def initResult(sft: SimpleFeatureType,
                                    transform: Option[SimpleFeatureType],
                                    options: Map[String, String]): VersionAggregator = {
    scanned = false
    new VersionAggregator
  }

  override protected def aggregateResult(sf: SimpleFeature, result: VersionAggregator): Unit = {}

  override protected def notFull(result: VersionAggregator): Boolean = true

  override protected def encodeResult(result: VersionAggregator): Array[Byte] =
    GeoMesaProperties.ProjectVersion.getBytes(StandardCharsets.UTF_8)
}

object HBaseVersionAggregator {

  def configure(sft: SimpleFeatureType, index: GeoMesaFeatureIndex[_, _]): Map[String, String] = {
    AggregatingScan.configure(sft, index, None, None, None) +
        (GeoMesaCoprocessor.AggregatorClass -> classOf[HBaseVersionAggregator].getName)
  }

  class VersionAggregator {
    def isEmpty: Boolean = false
    def clear(): Unit = {}
  }
}
