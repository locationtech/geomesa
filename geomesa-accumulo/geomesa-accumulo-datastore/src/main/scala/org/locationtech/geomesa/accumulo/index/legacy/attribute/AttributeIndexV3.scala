/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.attribute

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

// id not serialized in value
case object AttributeIndexV3 extends AccumuloFeatureIndex with AttributeWritableIndex with AttributeQueryableIndex {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  override val name: String = "attr"

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.exists(_.isIndexed)
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft)
    (wf: AccumuloFeature) => {
      getRows(wf).map { case (descriptor, row) =>
        val mutation = new Mutation(row)
        val values = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => wf.fullValues
          case IndexCoverage.JOIN => wf.indexValues
        }
        values.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft)
    (wf: AccumuloFeature) => {
      getRows(wf).map { case (descriptor, row) =>
        val mutation = new Mutation(row)
        val values = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => wf.fullValues
          case IndexCoverage.JOIN => wf.indexValues
        }
        values.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
        mutation
      }
    }
  }
}
