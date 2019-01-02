/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import org.apache.accumulo.core.data.{Mutation, Range}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AccumuloIndexAdapter}
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.index.index.legacy.Z2LegacyIndex
import org.locationtech.geomesa.index.index.z2.Z2IndexValues
import org.opengis.feature.simple.SimpleFeatureType

// legacy z curve - no delete checks for old col qualifiers
case object Z2IndexV3 extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with Z2LegacyIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val Z2IterPriority = 23

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def updateScanConfig(sft: SimpleFeatureType,
                                          config: ScanConfig,
                                          indexValues: Option[Z2IndexValues]): ScanConfig = {
    indexValues match {
      case None => config
      case Some(values) =>
        val zIter = Z2Iterator.configure(values, sft.isTableSharing, Z2IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}
