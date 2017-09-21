/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import org.apache.accumulo.core.data.{Mutation, Range}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AccumuloIndexAdapter}
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.curve.LegacyZ2SFC
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.index.legacy.Z2LegacyIndex
import org.locationtech.geomesa.index.index.z2.Z2IndexValues
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// legacy z curve - no delete checks for old col qualifiers
case object Z2IndexV3 extends AccumuloFeatureIndex with AccumuloIndexAdapter[Z2IndexValues]
    with Z2LegacyIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range] {

  val Z2IterPriority = 23

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: AccumuloDataStore,
                                    filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                    indexValues: Option[Z2IndexValues],
                                    ecql: Option[Filter],
                                    hints: Hints,
                                    dedupe: Boolean): ScanConfig = {
    val config = super.scanConfig(sft, ds, filter, indexValues, ecql, hints, dedupe)
    indexValues match {
      case None => config
      case Some(Z2IndexValues(sfc, _, xy)) =>
        val zIter = Z2Iterator.configure(sft, sfc, xy, Z2IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}
