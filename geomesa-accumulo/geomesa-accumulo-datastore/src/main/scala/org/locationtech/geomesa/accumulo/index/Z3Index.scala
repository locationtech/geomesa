/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.accumulo.iterators.Z3Iterator
import org.locationtech.geomesa.index.index.{Z3Index, Z3ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// current version - normal table sharing
case object Z3Index extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with Z3Index[AccumuloDataStore, AccumuloFeature, Mutation, Range] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val Z3IterPriority = 23

  override val version: Int = 4

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def scanConfig(sft: SimpleFeatureType,
                                    hints: Hints,
                                    ecql: Option[Filter],
                                    dedupe: Boolean): ScanConfig = {
    val config = super.scanConfig(sft, hints, ecql, dedupe)
    org.locationtech.geomesa.index.index.Z3Index.currentProcessingValues match {
      case None => config
      case Some(Z3ProcessingValues(sfc, _, xy, _, times)) =>
        // we know we're only going to scan appropriate periods, so leave out whole ones
        val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
        val filteredTimes = times.filter(_._2 != wholePeriod)
        val sharing = sft.isTableSharing
        val zIter = Z3Iterator.configure(sfc, xy, filteredTimes, isPoints = true, hasSplits = true, sharing, Z3IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}