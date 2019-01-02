/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.attribute

import org.apache.accumulo.core.data.{Mutation, Range}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloAttributeIndex
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.index.legacy.AttributeZIndex
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.locationtech.geomesa.utils.stats.IndexCoverage.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

case object AttributeIndexV5 extends AccumuloAttributeIndex
    with AttributeZIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConversions._

  override val version: Int = 5

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft, lenient = false)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    (wf) => getRows(wf).map { case (i, r) => createInsert(r, wf, coverages(i)) }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft, lenient = true)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    (wf) => getRows(wf).map { case (i, r) => createDelete(r, wf, coverages(i)) }
  }

  protected def createInsert(row: Array[Byte], feature: AccumuloFeature, coverage: IndexCoverage): Mutation = {
    val mutation = new Mutation(row)
    val values = coverage match {
      case IndexCoverage.FULL => feature.fullValues
      case IndexCoverage.JOIN => feature.indexValues
    }
    values.foreach(v => mutation.put(v.cf, v.cq, v.vis, v.value))
    mutation
  }

  protected def createDelete(row: Array[Byte], feature: AccumuloFeature, coverage: IndexCoverage): Mutation = {
    val mutation = new Mutation(row)
    val values = coverage match {
      case IndexCoverage.FULL => feature.fullValues
      case IndexCoverage.JOIN => feature.indexValues
    }
    values.foreach(v => mutation.putDelete(v.cf, v.cq, v.vis))
    mutation
  }
}
