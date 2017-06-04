/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Mutation, Range}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.index.{Z2Index, Z2ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// current version - no delete checks for old col qualifiers
case object Z2Index extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with Z2Index[AccumuloDataStore, AccumuloFeature, Mutation, Range] {

  val Z2IterPriority = 23

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: AccumuloDataStore,
                                    filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                    hints: Hints,
                                    ecql: Option[Filter],
                                    dedupe: Boolean): ScanConfig = {
    val config = super.scanConfig(sft, ds, filter, hints, ecql, dedupe)
    org.locationtech.geomesa.index.index.Z2Index.currentProcessingValues match {
      case None => config
      case Some(Z2ProcessingValues(_, xy)) =>
        val zIter = Z2Iterator.configure(sft, xy, Z2Index.Z2IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}