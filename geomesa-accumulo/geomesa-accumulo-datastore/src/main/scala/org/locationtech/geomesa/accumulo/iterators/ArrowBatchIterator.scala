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
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.iterators.{ArrowBatchAggregate, ArrowBatchScan}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Aggregates and returns arrow 'record batches'. An arrow file consists of metadata, n-batches, and a footer.
  * The metadata and footer are added by the reduce features step.
  */
class ArrowBatchIterator extends BaseAggregatingIterator[ArrowBatchAggregate] with ArrowBatchScan

object ArrowBatchIterator {

  val DefaultPriority = 25

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                dictionaries: Map[String, ArrowDictionary],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "arrow-batch-iter", classOf[ArrowBatchIterator])
    BaseAggregatingIterator.configure(is, deduplicate, None)
    ArrowBatchScan.configure(sft, index, filter, dictionaries, hints).foreach { case (k, v) => is.addOption(k, v) }
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
