/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators
import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.arrow.ArrowEncodedSft
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.iterators.{ArrowFileAggregate, ArrowFileScan}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Aggregates and returns arrow 'files'. Each value will be a full arrow file with metadata and batches.
  * This allows us to build up the dictionary values as we encounter the features, instead of
  * having to look them up ahead of time.
  */
class ArrowFileIterator extends BaseAggregatingIterator[ArrowFileAggregate] with ArrowFileScan

object ArrowFileIterator {

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                dictionaries: Seq[String],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = ArrowBatchIterator.DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "arrow-file-iter", classOf[ArrowFileIterator])
    BaseAggregatingIterator.configure(is, deduplicate, None)
    ArrowFileScan.configure(sft, index, filter, dictionaries, hints).foreach { case (k, v) => is.addOption(k, v) }
    is
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", ArrowEncodedSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(0, e.getValue.get())
      sf
    }
  }
}
