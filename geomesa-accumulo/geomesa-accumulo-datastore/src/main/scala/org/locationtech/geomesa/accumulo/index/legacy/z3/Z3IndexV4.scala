/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z3

import org.apache.accumulo.core.data.{Mutation, Range}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AccumuloIndexAdapter}
import org.locationtech.geomesa.accumulo.iterators.Z3Iterator
import org.locationtech.geomesa.index.index.legacy.Z3LegacyIndex
import org.locationtech.geomesa.index.index.z3.Z3IndexValues
import org.opengis.feature.simple.SimpleFeatureType

// legacy z curve - normal table sharing
case object Z3IndexV4 extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with Z3LegacyIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val Z3IterPriority = 23

  override val version: Int = 4

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def updateScanConfig(sft: SimpleFeatureType,
                                          config: ScanConfig,
                                          indexValues: Option[Z3IndexValues]): ScanConfig = {
    indexValues match {
      case None => config
      case Some(values) =>
        val zIter = Z3Iterator.configure(values, hasSplits = true, sft.isTableSharing, Z3IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}