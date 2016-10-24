/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.attribute

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

// current version - id in row keys
object AttributeIndex extends AccumuloFeatureIndex with AttributeWritableIndex with AttributeQueryableIndex {

  override val name: String = "attr"

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.exists(_.isIndexed)
  }

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val getRows = getRowKeys(sft)
    (wf: WritableFeature) => {
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

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val getRows = getRowKeys(sft)
    (wf: WritableFeature) => {
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

// added feature ID and dates to row key
object AttributeIndexV2 extends AccumuloFeatureIndex with AttributeWritableIndex with AttributeQueryableIndex {

  override val name: String = "attr"

  override val version: Int = 2

  override val serializedWithId: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.exists(_.isIndexed)
  }

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val getRows = getRowKeys(sft)
    (wf: WritableFeature) => {
      getRows(wf).map { case (descriptor, row) =>
        val mutation = new Mutation(row)
        val value = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => wf.fullValuesWithId.head
          case IndexCoverage.JOIN => wf.indexValuesWithId.head
        }
        mutation.put(EMPTY_TEXT, EMPTY_TEXT, value.vis, value.value)
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val getRows = getRowKeys(sft)
    (wf: WritableFeature) => {
      getRows(wf).map { case (descriptor, row) =>
        val mutation = new Mutation(row)
        val value = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => wf.fullValuesWithId.head
          case IndexCoverage.JOIN => wf.indexValuesWithId.head
        }
        mutation.putDelete(EMPTY_TEXT, EMPTY_TEXT, value.vis)
        mutation
      }
    }
  }
}